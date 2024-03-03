from abc import ABC, abstractmethod
from collections.abc import Awaitable
from dataclasses import dataclass
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


@dataclass(frozen=True, slots=True)
class Peer:
    handle: ClientHandle
    _send: trio.SocketStream

    async def send(self, tag: str, data: Any, pickle=False) -> bool:
        # TODO: If _send socket has broken, need to remove this peer from the
        # peer list
        if pickle:
            data = cpkl.dumps(data)
        await Message.ClientMessage.send(self._send, tag=tag, data=data)
        try:
            await Status.Success.expect(self._send)
        except Exception:
            print("Peer did not signal success")
            return False
        return True


def _peer_from_duplex_connection(conn: _DuplexConnection):
        return Peer(handle=conn.destination, _send=conn.send)


class Worker(ABC):
    @abstractmethod
    async def run(
            self,
            peers: Callable[[], list[Peer]],
            server_send: Callable[[bytes | object], Awaitable[None]]
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
        print(f"Starting client {self._handle.uid}")
        async with trio.open_nursery() as nursery:
            await nursery.start(
                trio.serve_tcp, self._handle_inbound_connection, self._handle.port)
            print("Listening")

            try:
                await self._connect_to_server(
                    server_hostname, server_port, registration_key)
            except Exception as e:
                print("Failed to connect to server:", type(e), *e.args)
                nursery.cancel_scope.cancel()
                raise
            print("Connected to server")

            async with self._server_stream:
                await self._run(nursery.cancel_scope)
        # TODO: Clean up internal state
        print("Client done")

    async def _run(
            self,
            cancel_scope: trio.CancelScope) -> None:
        async with trio.open_nursery() as nursery:
            nursery.start_soon(self._run_worker)
            # NOTE: Must start polling server after starting worker
            nursery.start_soon(self._receive_server_messages,
                               nursery, cancel_scope)

    async def _run_worker(self):
        try:
            await self._worker.run(self._get_peers, self._send_to_server)
        except Exception as e:
            print("Worker run error", type(e), *e.args)
            raise UserError from e

    def _get_peers(self) -> list[Peer]:
        return [_peer_from_duplex_connection(conn) for conn in self._peer_connections]

    async def _send_to_server(self, tag: str, data: Any, pickle=False):
        if pickle:
            data = cpkl.dumps(data)
        try:
            async with self._server_lock:
                await Message.ClientMessage.send(
                    self._server_stream, tag=tag, data=data)
        except Exception as e:
            print("Error occurred while sending data", type(e), *e.args)
            return False
        return True

    async def _receive_server_messages(
            self,
            nursery: trio.Nursery,
            cancel_scope: trio.CancelScope) -> NoReturn:
        print("Waiting for messages")
        while True:
            await self._receive_server_message(nursery, cancel_scope)

    @utils.noexcept("Server message")
    async def _receive_server_message(
            self,
            nursery: trio.Nursery,
            cancel_scope: trio.CancelScope) -> None:
        try:
            with trio.fail_after(0.01):
                async with self._server_lock:
                    messagetype, payload = await Message.recv(self._server_stream)
        except trio.TooSlowError:
            return
        except Exception as e:
            print("Did not receive valid message:", type(e), *e.args)
            return

        print(messagetype.name, "received")
        # TODO: Validate full payload by message type in one step before
        #       dispatch
        match messagetype:
            case Message.Shutdown:
                cancel_scope.cancel()
                print("Server connection closing")
                return
            case Message.NewPeer:
                peer = ClientHandle.from_dict(payload)
                nursery.start_soon(self._peer_connections.initiate, peer)
            case Message.RemovePeer:
                await self._peer_connections.aclose(UUID(bytes=payload["uid"]))
            case Message.ClientMessage:
                tag, data = payload["tag"], payload["data"]
                try:
                    await self._worker.handle_server_message(tag, data)
                except Exception as e:
                    # TODO: Print tb
                    raise UserError from e
            case _:
                print("Unhandled message:", messagetype.name,
                      "Payload:", payload)

    @utils.noexcept("Inbound connection")
    async def _handle_inbound_connection(
            self, recv_stream: trio.SocketStream) -> None:
        print("Received connection")
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

        print("Peer received")
        match message:
            case Message.ConnectPing:
                print("Them -> Me")
                await self._peer_connections.establish_from_ping(peer, recv_stream)

            case Message.ConnectPong:
                print("Them -> Me")
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
                    print("Unexpected message from peer")
                    raise

                try:
                    result = await self._worker.handle_peer_message(
                        peer, tag, data)
                except Exception as e:
                    # TODO: print bt
                    print("User-code exception in handle_peer_message:", type(e), *e.args)
                    raise UserError from e

                if result is None:
                    result = True

                if result:
                    await Status.Success.send(recv_stream)
                else:
                    # FIXME: May double-send if raises
                    await Status.Failure.send(recv_stream)
            except:
                # FIXME: May reraise if stream gone
                await Status.Failure.send(recv_stream)
                raise

    async def _connect_to_server(self,
                                 server_hostname: str,
                                 server_port: int,
                                 registration_key: str
                                 ) -> None:
        server_stream = await trio.open_tcp_stream(server_hostname, server_port)

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
        print("Registered!")
        self._server_stream = server_stream
