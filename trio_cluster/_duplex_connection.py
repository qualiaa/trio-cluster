from collections.abc import Iterator
from dataclasses import dataclass
from logging import getLogger
from typing import Self
from uuid import UUID

import trio

from ._exc import SequenceError
from ._message import MessageGenerator, Message, Status, messages
from ._listen_address import ListenAddress
from . import utils


_LOG = getLogger(__name__)


@dataclass(slots=True, frozen=True)
class _DuplexConnection:
    destination: ListenAddress
    lock: trio.Lock
    send: trio.abc.Stream
    recv: trio.abc.Stream
    send_response: MessageGenerator

    def __str__(self):
        return str(self.destination)

    def __repr__(self):
        return f"<{type(self).__name__}({self.destination!r}, send={self.send!r}, recv={self.recv!r})>"

    async def aclose(self):
        async with self.lock:
            await self.send_response.aclose()
            await self.send.aclose()
            await self.recv.aclose()


class _IncompleteConnection:
    # TODO: Move a lot of this implementation into ConnectionManager
    # TODO: Possibly simplify(?) with trio.Events for Ping/Pong/Complete?
    def __init__(self,
                 *,
                 _me: ListenAddress,
                 _destination: ListenAddress,
                 _in_progress: dict[UUID, Self],
                 _finished: dict[UUID, _DuplexConnection],
                 _recv=None):
        if _me.uid == _destination.uid:
            raise ValueError("Cannot connect to myself!")
        if _destination.uid in _in_progress | _finished:
            raise SequenceError("Already have connection!")
        # NOTE: Must add self to in_progress before hitting an await in case we
        #       receive a duplicate destination in another task, or an aclose.
        #       This means all ctors must call __init__ before
        #       calling an await!
        _in_progress[_destination.uid] = self

        self._me = _me
        self._destination = _destination
        self._in_progress = _in_progress
        self._finished = _finished
        self._recv = _recv

        self._lock = trio.Lock()
        self._send: trio.SocketStream = None
        self._send_response: MessageGenerator = None

    @classmethod
    async def initiate(cls,
                       me: ListenAddress,
                       destination: ListenAddress,
                       in_progress: dict[UUID, Self],
                       finished: dict[UUID, _DuplexConnection]) -> Self:
        """Send A -> B"""
        self = cls(_me=me, _destination=destination, _in_progress=in_progress, _finished=finished)
        await self._open_send_stream(Message.ConnectPing)
        return self

    @classmethod
    async def establish_from_ping(cls,
                                  me: ListenAddress,
                                  destination: ListenAddress,
                                  in_progress: dict[UUID, Self],
                                  finished: dict[UUID, _DuplexConnection],
                                  recv_stream: trio.SocketStream) -> Self:
        """Receive A -> B and respond with B -> A"""
        try:
            self = cls(_me=me,
                       _destination=destination,
                       _in_progress=in_progress,
                       _finished=finished,
                       _recv=recv_stream)
        except Exception:
            await Status.Failure.send(recv_stream)
            raise
        await Status.Success.send(recv_stream)

        await self._open_send_stream(Message.ConnectPong)
        return self

    async def establish_from_pong(self,
                                  destination: ListenAddress,
                                  recv_stream: trio.SocketStream) -> None:
        """Receive B -> A"""
        if destination.uid in self._finished:
            raise SequenceError("Pong destination already connected")
        if not self._send:
            raise SequenceError("Pong destination has no connection")
        if self._recv:
            raise SequenceError("Pong destination already has recv connection")

        self._recv = recv_stream

        await Status.Success.send(recv_stream)

    @property
    def destination(self) -> ListenAddress:
        return self._destination

    async def aclose(self):
        async with self._lock:
            for s in self._send, self._recv, self._send_response:
                if s:
                    await s.aclose()

    async def _open_send_stream(self, message: Message) -> None:
        # NOTE: We need a lock here to ensure an interleaved self.aclose waits
        #       for self._send to be set
        async with self._lock:
            _LOG.debug("Me -> Them")
            send_stream = await trio.open_tcp_stream(self._destination.addr,
                                                     self._destination.listen_port)
            self._send = send_stream
            self._send_response = messages(send_stream)
        await message.send(send_stream,
                           hostname=self._me.hostname,
                           listen_port=self._me.listen_port,
                           uid=self._me.uid.bytes)
        try:
            # TODO: This can obviously be improved
            msgtype, payload = await anext(self._send_response)
            msgtype.expect(Message.Status)
            Status(payload["status"]).expect(Status.Success)
        except Exception:
            # FIXME: What if we receive an aclose between message.send and
            #        Status.Success?
            self._in_progress.pop(self._destination.uid)
            await self.aclose()
            raise

    def complete(self):
        if self._send is None:
            raise SequenceError("Cannot complete connection: no send stream")
        if self._recv is None:
            raise SequenceError("Cannot complete connection: no recv stream")
        return _DuplexConnection(
            destination=self._destination, lock=self._lock, send=self._send, recv=self._recv, send_response=self._send_response)


class ConnectionManager:
    def __init__(self, handle: ListenAddress):
        self._handle = handle

        self._connections = {}
        self._in_progress = {}

    async def establish_from_ping(
            self,
            destination: ListenAddress,
            recv_stream: trio.SocketStream
    ) -> _DuplexConnection:
        _LOG.debug("Them -> Me")
        conn = await _IncompleteConnection.establish_from_ping(
            self._handle,
            destination,
            self._in_progress,
            self._connections,
            recv_stream
        )
        return self._complete(conn.destination.uid)

    async def establish_from_pong(
            self,
            destination: ListenAddress,
            recv_stream: trio.SocketStream
    ) -> _DuplexConnection:
        _LOG.debug("Them -> Me")
        try:
            try:
                conn = self._in_progress[destination.uid]
            except KeyError:
                raise SequenceError("Pong from unknown destination")
            await conn.establish_from_pong(destination, recv_stream)
        except Exception:
            await Status.Failure.send(recv_stream)
            raise
        return self._complete(conn.destination.uid)

    async def initiate(self, destination: ListenAddress) -> _IncompleteConnection:
        _LOG.debug("Starting ping connection")
        return await _IncompleteConnection.initiate(
            self._handle, destination, self._in_progress, self._connections)

    @utils.noexcept(log=_LOG)
    async def initiate_noexcept(self, *args, **kargs) -> _IncompleteConnection:
        return await self.initiate(*args, **kargs)

    async def aclose(self, uid: UUID) -> None:
        _LOG.info("Removing connection %s", uid)
        if uid in self._connections:
            conn = self._connections.pop(uid)
        elif uid in self._in_progress:
            conn = self._in_progress.pop(uid)
        else:
            _LOG.info("No such connection: %s", uid)
            return

        await conn.aclose()
        _LOG.info("Removed connection: %s", uid)

    def _complete(self, uid: UUID) -> _DuplexConnection:
        conn = self._connections[uid] = self._in_progress.pop(uid).complete()
        return conn

    def __iter__(self) -> Iterator[_DuplexConnection]:
        return iter(list(self._connections.values()))

    def __getitem__(self, uid: UUID) -> _DuplexConnection:
        return self._connections[uid]

    @property
    def connections(self) -> dict[UUID, _DuplexConnection]:
        return self._connections.copy()

    @property
    def incomplete(self) -> dict[UUID, _IncompleteConnection]:
        return self._in_progress.copy()

    @property
    def all(self) -> dict[UUID, _DuplexConnection | _IncompleteConnection]:
        return self._connections | self._in_progress
