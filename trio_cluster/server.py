from abc import ABC, abstractmethod
from dataclasses import dataclass
from logging import getLogger
from typing import Any, Callable
from uuid import UUID

import cloudpickle as cpkl
import trio

from . import utils
from ._client_handle import ClientHandle
from .message import Message, Status

_LOG = getLogger(__name__)


class Manager(ABC):
    """Implement this interface with your server logic.

    Note that the methods of this interface are called in in the main thread.
    To prevent starvation of server networking tasks, you must ensure a Trio
    checkpoint is called within a reasonable timeframe (ideally <1s). One
    way to achieve this is:

    >>> await trio.sleep(SECONDS)

    Another way is to open a nursery and spawn subtasks (which also must
    ultimately hit checkpoints):

    >>> async with trio.open_nursery() as nursery:
    >>>     nursery.start_soon(my_task, *args)

    Yet another way is to call

    >>> await trio.lowlevel.checkpoint()

    which immediately switches and puts the task at the back of the schedule.

    Your code may be "cancelled" at any checkpoint - for information on what
    this means and how to protect operations from cancellation, see
    https://trio.readthedocs.io/en/stable/reference-core.html#cancellation-and-timeouts
    """
    @abstractmethod
    async def run(self, clients: Callable[[], list[ConnectedClient]]) -> None:
        """Manager main task. If this returns, the server will shut down.

        If you have no main logic and only wish to respond to messages in
        handler methods, you must still ensure this method does not return, and
        also not block the main thread. One way to achieve this is:

        >>> await trio.sleep_forever()

        Alternatively, a simple loop may suffice for many purposes:

        >>> while True:
        >>>     ...  # One step of worker logic
        >>>     await trio.sleep(0.1)

        clients is a callable which returns a list of *currently active*
        clients. Calling clients() may yield different results on either side
        of a Trio checkpoint -- at the moment this method is invoked, clients()
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

    async def handle_client_message(
            self, client: ConnectedClient, tag: str, data: Any) -> None:
        """Handle a message from a client.

        This is guaranteed not to be called before new_client for the specific
        client

        tag and data are determined by the matching call from the Worker to
        server_send(tag, data) which produced the message. A useful pattern is:

        >>> match tag:
        >>>     case "command_a": ...  # Process command A
        >>>     case "command_b": ...  # Process command B
        >>>     ...

        Note: This function is called within client's dedicated task. Due to
        this, you should aim to return as quickly as possible to avoid a
        backlog of messages from this client. If a message instigates a long
        computation or ongoing process, you should parent it to your main task
        - see the Manager.run docstring (note also the caveats in the class
        docstring).
        """

    async def new_client(self, client: ConnectedClient) -> None:
        """Handle new client.

        You may use this method to do book-keeping and also to send clients an
        initial message upon connection. It is guaranteed to be called before
        the client sends a message or appears in the result of clients().

        Note: This function is called within client's dedicated task. Due to
        this, you should aim to return as quickly as possible to avoid a
        backlog of messages from this client. If a message instigates a long
        computation or ongoing process, you should parent it to your main task
        - see the Manager.run docstring (note also the caveats in the class
        docstring).
        """


@dataclass(slots=True, frozen=True)
class _Client:
    handle: ClientHandle
    stream: trio.SocketStream
    lock: trio.Lock

    def __str__(self):
        return str(self.handle)

    def __repr__(self):
        return f"<Client({self.handle!r})>"

    def as_connected_client(self) -> ConnectedClient:
        return ConnectedClient(
            handle=self.handle,
            send=ClientMessageSender(
                self.stream, self.lock, await_response=False))


class Server:
    def __init__(self, registration_key: str, port: int, manager: Manager):
        self._registration_key = registration_key
        self._port = port

        self._manager = manager

        self._clients = dict()

    async def listen(self) -> None:
        async with trio.open_nursery() as nursery:
            nursery.start_soon(self._manager.run, self._get_clients)
            listeners = await trio.open_tcp_listeners(self._port)
            for listener in listeners:
                utils.set_keepalive(listener.socket)
            _LOG.info("Listening for clients")
            await trio.serve_listeners(self._client_connection, listeners)
        _LOG.info("Server closing gracefully")

    def _get_clients(self) -> list[ConnectedClient]:
        return [c.as_connected_client() for c in self._clients.values()]

    @utils.noexcept(log=_LOG)
    async def _client_connection(self, client_stream: trio.SocketStream) -> None:
        _LOG.info("Received connection")
        async with client_stream:
            handle = await self._register_client(client_stream)
            client = _Client(
                handle=handle,
                stream=client_stream,
                lock=trio.Lock())

            async with trio.open_nursery() as nursery:
                # FIXME: Possible contention on these streams
                for peer in self._clients.values():
                    nursery.start_soon(_send_peer, peer, client)

            self._clients[client.handle.uid] = client
            try:
                async with client_stream:
                    await self._manage_client(client)
            finally:
                # FIXME: If parent nursery has been cancelled, what happens?
                del self._clients[client.handle.uid]
                with trio.move_on_after(1) as cleanup_scope:
                    # TODO: Think about shielding across whole codebase
                    cleanup_scope.shield = True
                    async with trio.open_nursery() as nursery:
                        # TODO: Make noexcept work when passed a function directly
                        nursery.start_soon(utils.noexcept()(Message.Shutdown.send),
                                           client._stream)
                        # FIXME: Possible contention on these streams
                        for peer in self._clients.values():
                            nursery.start_soon(utils.noexcept()(_remove_peer),
                                               peer, client)

    async def _manage_client(self, client: _Client) -> None:
        _LOG.info("Connected to new client")
        try:
            _LOG.debug("Calling _manager.new_client")
            await self._manager.new_client(client.as_connected_client())
        except Exception as e:
            _LOG.exception("Exception in user code")
            raise UserError from e

        async for msg in client.stream:
            try:
                messagetype, payload = Message.from_bytes(msg)
                assert messagetype == Message.ClientMessage, messagetype
                tag = payload["tag"]
                data = payload["data"]
            except Exception as e:
                _LOG.exception("Unexpected message from client")

            try:
                await self._manager.handle_client_message(
                    client.as_connected_client(), tag, data)
            except Exception as e:
                _LOG.exception("User-code exception in handle_client_message")

    async def _register_client(
            self, client_stream: trio.SocketStream) -> ClientHandle:
        registration_msg = await Message.ConnectPing.expect(client_stream)

        if registration_msg["key"] != self._registration_key:
            await client_stream.send_all(cpkl.dumps({"status": Status.BadKey}))
            raise ValueError("Incorrect registration key")

        else:
            client = ClientHandle(
                hostname=utils.get_hostname(client_stream),
                uid=UUID(bytes=registration_msg["uid"]),
                port=registration_msg["port"])

            await client_stream.send_all(cpkl.dumps({
                "status": Status.Success,
                "hostname": client.hostname
            }))
            _LOG.debug("Registered!")
            return client


async def _send_peer(peer: _Client, client: _Client) -> None:
    _LOG.debug("Sending NewPeer %s to %s", client, peer)
    async with peer.lock:
        await Message.NewPeer.send(peer.stream, **client.handle.to_dict())


async def _remove_peer(peer: _Client, client: _Client) -> None:
    _LOG.debug("Sending RemovePeer %s to %s", client, peer)
    async with peer.lock:
        await Message.RemovePeer.send(peer.stream, uid=client.handle.uid.bytes)
