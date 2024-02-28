import dataclasses
from abc import ABC, abstractmethod
from collections.abc import Awaitable
from dataclasses import dataclass
from typing import Any, Callable, Optional
from uuid import UUID, uuid4

import cloudpickle as cpkl
import msgpack
import trio

from . import utils
from .message import Message, Status


class UserError(Exception):
    """Error occurred in user code."""


class SequenceError(Exception):
    """Something has happened out of sequence"""


class InternalError(Exception):
    """Something unexpected has happened."""


@dataclass(slots=True, frozen=True)
class ClientHandle:
    uid: UUID
    hostname: str
    port: int

    def __hash__(self):
        return hash(self.uid)

    def __repr__(self):
        return f"{type(self).__name__}({self.uid!r}, {self.hostname}, {self.port})"

    def __str__(self):
        return f"[{self.hostname}]:{self.port} ({self.uid.hex[:6]})"

    @classmethod
    def from_dict(cls, d: dict[str, Any]):
        return cls(**d | {"uid": UUID(bytes=d["uid"])})

    def to_dict(self):
        return dataclasses.asdict(self) | {"uid": self.uid.bytes}


@dataclass(frozen=True, slots=True)
class Peer:
    handle: ClientHandle
    _send: trio.SocketStream

    async def send(self, tag: str, data: Any, pickle=False) -> bool:
        # TODO: If _send socket has broken, need to remove this peer from the
        # peer list
        if pickle:
            data = cpkl.dumps(data)
        await Message.PeerMessage.send(self._send, tag=tag, data=data)
        try:
            await Status.Success.expect(self._send)
        except Exception:
            print("Peer did not signal success")
            return False
        return True


class Worker(ABC):
    @abstractmethod
    async def run(
            self,
            work_detail,
            peers: Callable[[], list[Peer]],
            server_send: Callable[[bytes | object], Awaitable[None]]
    ):
        ...

    async def handle_peer_message(self, peer_handle: ClientHandle, tag: str, data: Any):
        ...

    async def handle_server_message(self, tag: str, data: Any):
        ...


@dataclass(slots=True)
class _PeerConnection:
    peer: ClientHandle
    lock: trio.Lock
    send: Optional[trio.abc.Stream] = None
    recv: Optional[trio.abc.Stream] = None

    def __str__(self):
        return str(self.peer)

    def __repr__(self):
        return f"<{type(self).__name__}({self.peer!r}, send={self.send!r}, recv={self.recv!r})>"

    def as_peer(self) -> Peer:
        return Peer(handle=self.peer, _send=self.send)


class Client:
    def __init__(self, port: int, worker):
        self._uid = uuid4()
        self._hostname = "localhost"
        self._port = port

        self._worker = worker

        self._server_stream = None

        self._incomplete_connections = {}
        self._peers = {}

    async def run(self,
                  server_hostname: str,
                  server_port: int,
                  registration_key: str) -> None:
        async with trio.open_nursery() as nursery:
            await nursery.start(
                trio.serve_tcp, self._handle_inbound_connection, self._port)
            print("Listening")

            try:
                work_detail, server_stream = await self._register(
                    server_hostname, server_port, registration_key)
            except Exception as e:
                print("Failed to connect to server:", type(e), *e.args)
                nursery.cancel_scope.cancel()
                return
            print("Connected to server")

            async with server_stream:
                await self._run(server_stream, work_detail, nursery.cancel_scope)
        # TODO: Clean up internal state
        print("Client done")

    async def _run(
            self,
            server_stream: trio.SocketStream,
            work_detail: Any,
            cancel_scope: trio.CancelScope) -> None:
        async with trio.open_nursery() as nursery:
            nursery.start_soon(self._worker.run,
                               work_detail,
                               self._get_peers,
                               self._send_to_server)
            # NOTE: Must start polling server after starting worker
            nursery.start_soon(self._receive_server_messages,
                               server_stream, nursery, cancel_scope)

    def _get_peers(self):
        return [conn.as_peer() for conn in self._peers.values()]

    async def _send_to_server(self, tag: str, data: Any, pickle=False):
        if pickle:
            data = cpkl.dumps(data)
        try:
            await Message.PeerMessage.send(self._server_stream, tag=tag, data=data)
        except Exception as e:
            print("Error occurred while sending data", type(e), *e.args)
            return False
        return True

    async def _receive_server_messages(
            self,
            server_stream: trio.SocketStream,
            nursery: trio.Nursery,
            cancel_scope: trio.CancelScope) -> None:
        print("Waiting for messages")
        while True:
            try:
                message, payload = await Message.recv(server_stream)
            except Exception as e:
                print("Did not receive valid message:", type(e), *e.args)
                continue

            print(message.name, "received")
            # TODO: Validate full payload by message type in one step before
            #       dispatch
            match message:
                case Message.Shutdown:
                    cancel_scope.cancel()
                    print("Server connection closing")
                    return
                case Message.NewPeer:
                    nursery.start_soon(self._peer_connect_ping,
                                       ClientHandle.from_dict(payload))
                case Message.RemovePeer:
                    await self._remove_peer(UUID(bytes=payload["uid"]))
                case Message.PeerMessage:
                    tag, data = payload["tag"], payload["data"]
                    try:
                        self._worker.handle_server_message(tag, data)
                    except Exception as e:
                        raise UserError from e
                case _:
                    print("Unhandled message:", message.name,
                          "Payload:", payload)

    @utils.noexcept("Inbound connection")
    async def _handle_inbound_connection(
            self, recv_stream: trio.SocketStream) -> None:
        print("Received connection")
        try:
            message, payload = await Message.recv(recv_stream)
            peer = ClientHandle.from_dict(
                {"hostname": utils.get_hostname(recv_stream)} | payload)
            if peer.uid == self._uid:
                raise ValueError("UID collision")
        except Exception:
            async with recv_stream:
                await Status.Failure.send(recv_stream)
            raise

        try:
            print("Peer received")
            match message:
                case Message.ConnectPing:
                    print("Them -> Me")
                    if peer.uid in self._peers | self._incomplete_connections:
                        await Status.Failure.send(recv_stream)
                        raise SequenceError("Peer already connected!")

                    conn = self._incomplete_connections[peer.uid] = (
                        _PeerConnection(peer=peer, lock=trio.Lock()))
                    conn.recv = recv_stream
                    await Status.Success.send(recv_stream)
                    await self._peer_connect_pong(conn)

                case Message.ConnectPong:
                    print("Them -> Me")
                    if peer.uid in self._peers:
                        raise SequenceError("Pong peer already connected")
                    if peer.uid not in self._incomplete_connections:
                        raise SequenceError("Pong from unknown peer")
                    if not self._incomplete_connections[peer.uid].send:
                        raise SequenceError("Pong peer has no connection")
                    if self._incomplete_connections[peer.uid].recv:
                        raise SequenceError("Pong peer already has recv connection")

                    self._incomplete_connections[peer.uid].recv = recv_stream
                    await Status.Success.send(recv_stream)
                    self._complete_connection(peer.uid)

                case _:
                    raise ValueError("Unhandled message:", message, payload)

            async for msg in recv_stream:
                try:
                    try:
                        msg = msgpack.unpackb(msg)
                        messagetype = Message.from_bytes(msg["messagetype"])
                        assert messagetype == Message.PeerMessage, messagetype
                        tag = msg["payload"]["tag"]
                        data = msg["payload"]["data"]
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
                        await Status.Failure.send(recv_stream)
                except:
                    # FIXME: May reraise if stream gone
                    await Status.Failure.send(recv_stream)
                    raise
        finally:
            await self._remove_peer(peer.uid)

    @utils.noexcept("Initiating peer connection")
    async def _peer_connect_ping(self, peer: ClientHandle) -> None:
        """If A -> B and then B -> A, this function is A -> B"""
        print("Starting ping connection")
        if peer.uid == self._uid:
            raise InternalError("Received myself as peer!")
        if peer.uid in self._incomplete_connections:
            raise SequenceError("Already have peer!")

        conn = self._incomplete_connections[peer.uid] = _PeerConnection(peer=peer, lock=trio.Lock())
        await self._open_send_stream(conn, Message.ConnectPing)

    async def _peer_connect_pong(self, conn: _PeerConnection) -> None:
        """If A -> B and then B -> A, this function is B -> A"""
        print("Starting pong connection")
        await self._open_send_stream(conn, Message.ConnectPong)
        self._complete_connection(conn.peer.uid)

    def _complete_connection(self, uid: UUID) -> None:
        print("Peer connection complete", uid)
        self._peers[uid] = self._incomplete_connections.pop(uid)

    async def _open_send_stream(
            self, conn: _PeerConnection, message: Message) -> None:
        async with conn.lock:
            print("Me -> Them")
            send_stream = await trio.open_tcp_stream(
                conn.peer.hostname,
                conn.peer.port)
            conn.send = send_stream
        await message.send(
            send_stream,
            port=self._port,
            uid=self._uid.bytes
        )
        try:
            await Status.Success.expect(send_stream)
        except Exception:
            await self._remove_peer(conn.peer.uid)
            raise

    async def _remove_peer(self, uid: UUID) -> None:
        if not isinstance(uid, UUID):
            raise TypeError("Peer must be passed as UUID")
        print("Removing peer", uid)
        if uid in self._peers:
            conn = self._peers.pop(uid)
        elif uid in self._incomplete_connections:
            conn = self._incomplete_connections.pop(uid)
        else:
            print("No such peer:", uid)
            return

        async with conn.lock:
            if conn.send:
                await conn.send.aclose()
            if conn.recv:
                await conn.recv.aclose()
        print("Removed peer:", conn.peer)

    async def _register(self,
                        server_hostname: str,
                        server_port: int,
                        registration_key: str
                        ) -> tuple[Any, trio.SocketStream]:
        server_stream = await trio.open_tcp_stream(server_hostname, server_port)

        try:
            await Message.ConnectPing.send(
                server_stream,
                key=registration_key,
                port=self._port,
                uid=self._uid.bytes
            )

            # TODO: Get rid of work function
            registration_response = cpkl.loads(await server_stream.receive_some())

            if registration_response["status"] == Status.BadKey:
                raise RuntimeError("Incorrect registration key")
            if registration_response["status"] != Status.Success:
                # TODO: Retry until successful or the client is shut down
                raise RuntimeError("Server signalled registration failure")
            self._hostname = registration_response["hostname"]
        except BaseException:
            await server_stream.aclose()
            raise
        return registration_response["work_fn"], server_stream
