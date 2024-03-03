from abc import ABC, abstractmethod
from collections.abc import Awaitable
from dataclasses import dataclass
from logging import getLogger
from typing import Any, Callable, NoReturn, Optional, Self
from uuid import UUID, uuid4

import cloudpickle as cpkl
import trio

from . import utils
from ._client_handle import ClientHandle
from ._duplex_connection import (
    _DuplexConnection,
    ConnectionManager)
from ._exc import *
from .message import Message, Status


_LOG = getLogger(__name__)


class ClientMessageSender:
    def __init__(self,
                 stream: trio.SocketStream,
                 lock: trio.Lock,
                 await_response=True):
        self._stream = stream
        self._lock = lock
        self._await_response = await_response

    async def __call__(self, tag: str, data: Any, pickle=False) -> bool:
        # TODO: If stream has broken, need to remove peer/server
        if pickle:
            data = cpkl.dumps(data)
        try:
            async with self._lock:
                await Message.ClientMessage.send(self._stream, tag=tag, data=data)
        except Exception as e:
            _LOG.exception("Error occurred while sending data")
            return False
        if self._await_response:
            try:
                await Status.Success.expect(self._stream)
            except Exception:
                _LOG.exception("Peer did not signal success")
                return False
        return True


@dataclass(frozen=True, slots=True)
class Peer:
    handle: ClientHandle
    send: ClientMessageSender


def _peer_from_duplex_connection(conn: _DuplexConnection):
        return Peer(handle=conn.destination, send=ClientMessageSender(
            conn.send, conn.lock))


class Worker(ABC):
    @abstractmethod
    async def run(
            self,
            peers: Callable[[], list[Peer]],
            server_send: ClientMessageSender
    ):
        ...

    async def handle_peer_message(self, peer_handle: ClientHandle, tag: str, data: Any):
        ...

    async def handle_server_message(self, tag: str, data: Any):
        ...


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

    async def run(self,
                  server_hostname: str,
                  server_port: int,
                  registration_key: str) -> None:
        _LOG.info("Starting client %s", self._handle.uid)
        async with trio.open_nursery() as nursery:
            await nursery.start(
                trio.serve_tcp, self._handle_inbound_connection, self._handle.port)
            _LOG.debug("Listening for peers")

            await self._connect_to_server(
                server_hostname, server_port, registration_key)

            nursery.start_soon(self._run_worker)
            # NOTE: Must start polling server after starting worker
            nursery.start_soon(self._receive_server_messages, nursery)

        # TODO: Clean up internal state
        _LOG.info("Client closing normally")

    async def _run_worker(self):
        try:
            await self._worker.run(
                peers=lambda: [_peer_from_duplex_connection(conn) for conn in self._peer_connections],
                server_send=ClientMessageSender(
                    stream=self._server_stream,
                    lock=self._server_lock,
                    await_response=False
            ))
        except Exception as e:
            _LOG.exception("Worker error")
            raise UserError from e

    async def _receive_server_messages(self, nursery: trio.Nursery) -> NoReturn:
        _LOG.info("Polling server messages")
        async with self._server_stream:
            while True:
                await self._receive_server_message(nursery)

    @utils.noexcept(log=_LOG)
    async def _receive_server_message(self, nursery: trio.Nursery) -> None:
        try:
            # NOTE: Cannot simply poll socket at OS level, as this blocks us
            #       (our worker) from sending data on the same stream.
            #       Hence use of fail_after.
            with trio.fail_after(0.01):
                async with self._server_lock:
                    messagetype, payload = await Message.recv(self._server_stream)
        except trio.TooSlowError:
            return

        _LOG.debug("%s received from server", messagetype.name)
        # TODO: Validate full payload by message type in one step before
        #       dispatch
        match messagetype:
            case Message.Shutdown:
                nursery.cancel_scope.cancel()
                _LOG.info("Server connection closed gracefully")
                return
            case Message.NewPeer:
                peer = ClientHandle.from_dict(payload)
                nursery.start_soon(self._peer_connections.initiate_noexcept, peer)
            case Message.RemovePeer:
                await self._peer_connections.aclose(UUID(bytes=payload["uid"]))
            case Message.ClientMessage:
                tag, data = payload["tag"], payload["data"]
                try:
                    await self._worker.handle_server_message(tag, data)
                except Exception as e:
                    raise UserError from e
            case _:
                _LOG.warning("Unhandled message: %s; Payload: %s",
                             messagetype.name, payload)

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
                await self._peer_connections.establish_from_ping(peer, recv_stream)

            case Message.ConnectPong:
                await self._peer_connections.establish_from_pong(peer, recv_stream)

            case _:
                raise ValueError("Unhandled message:", message, payload)

        try:
            await self._poll_peer(peer)
        finally:
            await self._peer_connections.aclose(peer.uid)

    async def _poll_peer(self, peer) -> NoReturn:
        recv_stream = self._peer_connections[peer.uid].recv
        async for msg in recv_stream:
            try:
                try:
                    messagetype, payload = Message.from_bytes(msg)
                    assert messagetype == Message.ClientMessage, messagetype
                    tag = payload["tag"]
                    data = payload["data"]
                except Exception:
                    _LOG.warning("Unexpected message from peer")
                    raise

                try:
                    result = await self._worker.handle_peer_message(
                        peer, tag, data)
                except Exception as e:
                    _LOG.exception("User exception in handle_peer_message")
                    raise UserError from e

                if result is None:
                    result = True

                # FIXME: May double-send if raises
                if result:
                    await Status.Success.send(recv_stream)
                else:
                    await Status.Failure.send(recv_stream)
            except BaseException:
                # FIXME: May reraise if stream gone
                await Status.Failure.send(recv_stream)
                raise

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

            registration_response = cpkl.loads(await server_stream.receive_some())

            if registration_response["status"] == Status.BadKey:
                raise RuntimeError("Incorrect registration key")
            if registration_response["status"] != Status.Success:
                # TODO: Retry until successful or the client is shut down
                raise RuntimeError("Server signalled registration failure")
            self._handle.hostname = registration_response["hostname"]
        except BaseException:
            await server_stream.aclose()
            raise
        _LOG.debug("Registered!")
        self._server_stream = server_stream
