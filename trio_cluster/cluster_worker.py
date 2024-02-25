import dataclasses
from dataclasses import dataclass
from functools import partial
from typing import Any, Callable, NoReturn, Optional, TypeAlias
from uuid import UUID, uuid4

import cloudpickle as cpkl
import msgpack
import trio
import trio_parallel

from . import utils
from .message import Message, Status


class SequenceError(Exception):
    """Something has happened out of sequence"""


class InternalError(Exception):
    """Something unexpected has happened."""


WorkFunction: TypeAlias = Callable[[], Any]


@dataclass
class _Client:
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


@dataclass
class _PeerConnection:
    peer: _Client
    lock: trio.Lock
    send: Optional[trio.abc.Stream] = None
    recv: Optional[trio.abc.Stream] = None

    def __str__(self):
        return str(self.peer)

    def __repr__(self):
        return f"<{type(self).__name__}({self.peer!r}, send={self.send!r}, recv={self.recv!r})>"


class ClusterWorker:
    def __init__(self, port: int):
        self._uid = uuid4()
        self._hostname = "localhost"
        self._port = port

        self._incomplete_connections = {}
        self._peers = {}

    async def run(self, *args, **kargs):
        async with trio.open_nursery() as nursery:
            await nursery.start(
                trio.serve_tcp, self._handle_inbound_connection, self._port)
            print("Listening")

            nursery.start_soon(
                partial(self._run, *args, **kargs, cancel_scope=nursery.cancel_scope))
        print("Client done")

    async def _run(
            self,
            server_hostname: str,
            server_port: int,
            registration_key: str,
            cancel_scope: trio.CancelScope):
        work_fn, server_stream = await self.register(
            server_hostname, server_port, registration_key)

        async with server_stream, trio.open_nursery() as nursery:
            nursery.start_soon(
                utils.every, 5, lambda: print(f"I'm alive: {self._peers}\n"))
            nursery.start_soon(self.do_work, work_fn)
            nursery.start_soon(utils.aevery, 1, self._ping_peers)
            await self._receive_server_messages(
                server_stream, nursery, cancel_scope=cancel_scope)

    async def do_work(self, fn: WorkFunction) -> NoReturn:
        while True:
            print("Starting work")
            await trio_parallel.run_sync(fn, cancellable=True)
            print("Finished work")

    async def _ping_peers(self) -> None:
        for conn in self._peers.values():
            try:
                try:
                    conn.lock.acquire_nowait()
                except trio.WouldBlock:
                    pass
                else:
                    await conn.send.send_all(msgpack.packb({"msg": "Hello"}))
                    try:
                        await Status.recv(conn.send)
                    except Exception as e:
                        print(f"Invalid response from {conn.peer}:", type(e), *e.args)
                finally:
                    conn.lock.release()
            except Exception as e:
                print("Error messaging peer:", type(e), *e.args)

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
            match message:
                case Message.Shutdown:
                    cancel_scope.cancel()
                    print("Server connection closing")
                    return
                case Message.NewPeer:
                    nursery.start_soon(self._peer_connect_ping,
                                       _Client.from_dict(payload))
                case Message.RemovePeer:
                    await self._remove_peer(UUID(bytes=payload["uid"]))
                case _:
                    print("Unhandled message:", message.name,
                          "Payload:", payload)

    @utils.noexcept("Inbound connection")
    async def _handle_inbound_connection(
            self, recv_stream: trio.SocketStream) -> None:
        print("Received connection")
        try:
            message, payload = await Message.recv(recv_stream)
            peer = _Client.from_dict(
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

                    conn = self._incomplete_connections[peer.uid] = _PeerConnection(peer=peer, lock=trio.Lock())
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
                print("Polling stream")
                msg = msgpack.unpackb(msg)
                await Status.Success.send(recv_stream)
                print(f"[{peer}]", msg)
        except Exception:
            await self._remove_peer(peer.uid)
            raise

    @utils.noexcept("Initiating peer connection")
    async def _peer_connect_ping(self, peer: _Client) -> None:
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

    async def register(self,
                       server_hostname: str,
                       server_port: int,
                       registration_key: str
                       ) -> tuple[WorkFunction, trio.SocketStream]:
        server_stream = await trio.open_tcp_stream(server_hostname, server_port)

        await Message.ConnectPing.send(
            server_stream,
            key=registration_key,
            port=self._port,
            uid=self._uid.bytes
        )

        registration_response = cpkl.loads(await server_stream.receive_some())

        if registration_response["status"] != Status.Success:
            # TODO: Retry until successful or the worker is shut down
            raise RuntimeError("Server signalled registration failure")
        self._hostname = registration_response["hostname"]
        return registration_response["work_fn"], server_stream
