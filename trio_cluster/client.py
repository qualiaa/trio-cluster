from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from logging import getLogger
from typing import Any, Callable, Optional
from uuid import UUID, uuid4

import cloudpickle as cpkl
import trio

from . import utils
from ._client_handle import ClientHandle
from ._connected_client import ConnectedClient, ClientMessageSender
from ._duplex_connection import _DuplexConnection, ConnectionManager
from ._exc import InternalError, Shutdown, UserError
from ._message import client_messages, messages, to_client_message, Message, Status


_LOG = getLogger(__name__)


class Worker(ABC):
    """Implement this interface with your worker logic.

    Note that the methods of this interface are called in the main thread.
    To prevent starvation of client networking tasks, you must ensure a Trio
    checkpoint is called within a reasonable timeframe (ideally <1s). One
    way to achieve this is:

    >>> await trio.sleep(SECONDS)

    Another way is to open a nursery and spawn subtasks (which also must
    ultimately hit checkpoints):

    >>> async with trio.open_nursery() as nursery:
    >>>     self._nursery = nursery  # Make nursery available to
    >>>                              # handle_client_message
    >>>     nursery.start_soon(my_task, *args)

    Yet another way is to call

    >>> await trio.lowlevel.checkpoint()

    which immediately switches and puts the task at the back of the schedule.

    Your code may be "cancelled" at any checkpoint - for information on what
    this means and how to protect operations from cancellation, see
    https://trio.readthedocs.io/en/stable/reference-core.html#cancellation-and-timeouts
    """
    @abstractmethod
    async def run(
            self,
            peers: Callable[[], list[ConnectedClient]],
            server_send: ClientMessageSender
    ) -> None:
        """Worker main task. If this returns, the client will shut down.

        If you have no main logic and only wish to respond to messages in
        handler methods, you must still ensure this method does not return, and
        also not block the main thread. One way to achieve this is:

        >>> await trio.sleep_forever()

        Alternatively, a simple loop may suffice for many purposes:

        >>> while True:
        >>>     ...  # One step of worker logic
        >>>     await trio.sleep(0.1)

        peers is a callable which returns a list of *currently active*
        peers. Calling peers() may yield different results on either side
        of a Trio checkpoint -- at the moment this method is invoked, peers()
        should return the empty list. You should call it repeatedly in order to
        get an up-to-date list.

        This method runs in its own task. In order to allow other methods to
        parent their tasks to this one, you should open a nursery here and
        store it as an attribute:

        >>> async with trio.open_nursery() as nursery:
        >>>     self._nursery = nursery  # Make nursery available to
        >>>                              # handle_client_message
        >>>     ... # Do something to keep nursery alive (e.g. trio.sleep_forever())

        Then other methods can spawn sub-tasks under this nursery:

        >>> self._nursery.start_soon(task, *args)
        """

    async def handle_peer_message(
            self, peer: ClientHandle, tag: str, data: Any) -> Optional[bool]:
        """Handle a message from a peer.

        You can return False from this function to signal failure to the
        caller.

        peer describes the peer information, but note that you cannot reply to
        the message from this method.

        tag and data are determined by the matching call to
        peer.send(tag, data) which produced the message. A useful pattern is:

        >>> match tag:
        >>>     case "command_a": ...  # Process command A
        >>>     case "command_b": ...  # Process command B
        >>>     ...

        Note: This function is called within peer's dedicated task. Due to
        this, you should aim to return as quickly as possible to avoid a
        backlog of messages from this peer. If a message instigates a long
        computation or ongoing process, you should parent it to your main task
        - see the Worker.run docstring (note also the caveats in the class
        docstring).
        """

    async def handle_server_message(self, tag: str, data: Any) -> None:
        """Handle a message from the manager.

        tag and data are determined by the matching call to client.send(tag,
        data) which produced the message. A useful pattern is:

        >>> match tag:
        >>>     case "command_a": ...  # Process command A
        >>>     case "command_b": ...  # Process command B
        >>>     ...

        Note: This function is called within the server's dedicated task. Due
        to this, you should aim to return as quickly as possible to avoid a
        backlog of messages from the server. If a message instigates a long
        computation or ongoing process, you should parent it to your main task
        - see the Worker.run docstring (note also the caveats in the class
        docstring).
        """


class Client:
    def __init__(self, port: int, worker: Worker):
        self._handle = ClientHandle(
            uid=uuid4(),
            hostname="localhost",
            port=port)

        self._worker = worker

        self._server_lock = trio.Lock()
        self._server_stream: Optional[trio.SocketStream] = None

        self._peer_connections = ConnectionManager(self._handle)
        self._peers: dict[UUID, _Peer] = {}
        self._nursery: trio.Nursery = None

    async def run(self,
                  server_hostname: str,
                  server_port: int,
                  registration_key: str) -> None:
        _LOG.info("Starting client %s", self._handle.uid)
        async with trio.open_nursery() as nursery:
            self._nursery = nursery
            await nursery.start(
                trio.serve_tcp, self._handle_inbound_connection, self._handle.port)
            _LOG.info("Listening for peers on port %s", self._handle.port)

            await self._connect_to_server(
                server_hostname, server_port, registration_key)

            nursery.start_soon(self._run_worker)
            # NOTE: Must start polling server after starting worker
            nursery.start_soon(self._receive_server_messages)

        # TODO: Clean up internal state
        _LOG.debug("Client closing normally")

    async def _run_worker(self) -> None:
        def shutdown():
            raise Shutdown("Server connection lost")
        try:
            await self._worker.run(
                peers=lambda: [peer.as_connected_client()
                               for peer in self._peers.values()],
                server_send=ClientMessageSender(
                    stream=self._server_stream,
                    lock=self._server_lock,
                    await_response=False,
                    stream_failure_callback=shutdown))
        except Exception as e:
            _LOG.exception("User exception in run")
            raise UserError from e
        # Worker has closed gracefully
        self._nursery.cancel_scope.cancel()

    async def _receive_server_messages(self) -> None:
        _LOG.info("Polling server messages")
        if self._server_stream is None:
            raise InternalError("Trying to receive without server stream")

        async with self._server_stream:
            async for msgtype, payload in messages(self._server_stream,
                                                   ignore_errors=True):
                _LOG.debug("%s received from server", msgtype.name)
                match msgtype:
                    case Message.Shutdown:
                        self._nursery.cancel_scope.cancel()
                        _LOG.info("Server connection closed gracefully")
                        return
                    case Message.NewPeer:
                        peer = ClientHandle.from_dict(payload)
                        _LOG.info("New Peer: %s", peer.uid)
                        self._nursery.start_soon(
                            self._peer_connections.initiate_noexcept, peer)
                    case Message.RemovePeer:
                        uid = UUID(bytes=payload["uid"])
                        _LOG.info("Removing peer: %s", uid)
                        try:
                            self._peers[uid].cancel_scope.cancel()
                        except KeyError:
                            _LOG.info("No such connection: %s", uid)
                    case Message.ClientMessage:
                        tag, data = to_client_message(payload)
                        try:
                            await self._worker.handle_server_message(tag, data)
                        except Exception as e:
                            _LOG.exception("User exception in handle_server_message")
                            raise UserError from e
                    case _:
                        _LOG.warning("Unhandled message: %s; Payload: %s",
                                     msgtype.name, payload)

    @utils.noexcept(log=_LOG)
    async def _handle_inbound_connection(
            self, recv_stream: trio.SocketStream) -> None:
        _LOG.info("Received connection")
        try:
            message, payload = await Message.recv(recv_stream)
            peer = ClientHandle.from_dict(
                {"hostname": utils.get_hostname(recv_stream)} | payload)
            if peer.uid == self._handle.uid:
                raise ValueError("UID collision")
        except Exception:
            async with recv_stream:
                await Status.Failure.send(recv_stream)
            raise

        _LOG.debug("Peer received")
        match message:
            case Message.ConnectPing:
                conn = await self._peer_connections.establish_from_ping(
                    peer, recv_stream)

            case Message.ConnectPong:
                conn = await self._peer_connections.establish_from_pong(
                    peer, recv_stream)

            case _:
                raise ValueError("Unhandled message:", message, payload)

        peer = _Peer(peer, conn)
        self._peers[peer.uid] = peer
        try:
            await peer.poll(self._worker)
        finally:
            del self._peers[peer.uid]
            await self._peer_connections.aclose(peer.uid)

    async def _connect_to_server(self,
                                 server_hostname: str,
                                 server_port: int,
                                 registration_key: str
                                 ) -> None:
        _LOG.info("Connecting to server")
        server_stream = await utils.open_tcp_stream_retry(server_hostname, server_port)
        _LOG.info("Connected")

        try:
            await Message.ConnectPing.send(
                server_stream,
                key=registration_key,
                port=self._handle.port,
                uid=self._handle.uid.bytes
            )

            # TODO: No need to use cpkl here any more
            registration_response = cpkl.loads(await server_stream.receive_some())

            if registration_response["status"] == Status.BadKey:
                raise RuntimeError("Incorrect registration key")
            if registration_response["status"] != Status.Success:
                raise RuntimeError("Server signalled unexplained registration failure")

            self._handle.hostname = registration_response["hostname"]
        except BaseException:
            await server_stream.aclose()
            raise
        _LOG.debug("Registered!")
        self._server_stream = server_stream


@dataclass(slots=True, frozen=True)
class _Peer:
    handle: ClientHandle
    connection: _DuplexConnection
    cancel_scope: trio.CancelScope = field(default_factory=trio.CancelScope)

    @property
    def uid(self):
        return self.handle.uid

    def __str__(self):
        return str(self.handle)

    def __repr__(self):
        return f"<_Peer({self.handle!r})>"

    @property
    def lock(self):
        return self.connection.lock

    def as_connected_client(self) -> ConnectedClient:
        return ConnectedClient(
            handle=self.handle,
            local=utils.host_is_local(self.connection.send),
            send=ClientMessageSender(
                self.connection.send,
                self.connection.lock,
                stream_failure_callback=self.cancel_scope.cancel))

    async def poll(self, worker: Worker) -> None:
        with self.cancel_scope:
            async for tag, data in client_messages(self.connection.recv):
                _LOG.debug("Received ClientMessage with tag %s", tag)

                try:
                    result = await worker.handle_peer_message(
                        self.handle, tag, data)
                except Exception as e:
                    try:
                        await Status.Failure.send(self.connection.recv)
                    except Exception:
                        pass
                    _LOG.exception("User exception in handle_peer_message")
                    raise UserError from e
                except BaseException:
                    try:
                        await Status.Failure.send(self.connection.recv)
                    except Exception:
                        pass
                    raise

                if result or result is None:
                    await Status.Success.send(self.connection.recv)
                else:
                    await Status.Failure.send(self.connection.recv)
