import math
from base64 import b64encode, b64decode
from enum import IntEnum
from itertools import count
from typing import Any, Self

import cloudpickle as cpkl
import msgpack
import trio

from ._exc import MessageParseError, UnexpectedMessageError

_unique = count()


class MessageBase(IntEnum):
    # TODO: Replace this with a string-backed enum
    @classmethod
    def byte_size(cls) -> int:
        return 1 + int(math.log2(max(cls))//8)

    def __bytes__(self) -> bytes:
        return self.to_bytes()

    def to_bytes(self) -> bytes:
        return int.to_bytes(self, self.byte_size(), "little")

    def to_b64(self) -> bytes:
        return b64encode(bytes(self))

    @classmethod
    def from_bytes(cls, v: bytes) -> Self:
        if len(v) != cls.byte_size():
            raise MessageParseError(
                f"{cls} is of size {cls.byte_size()} bytes, received {len(v)}")
        return cls(int.from_bytes(v, "little"))

    @classmethod
    def from_b64(cls, v) -> Self:
        return cls.from_bytes(b64decode(v))


class Message(MessageBase):
    Shutdown = next(_unique)
    NewPeer = next(_unique)
    RemovePeer = next(_unique)
    ConnectPing = next(_unique)
    ConnectPong = next(_unique)
    ClientMessage = next(_unique)

    async def send(self, stream: trio.SocketStream, **kargs) -> None:
        message = {"messagetype": bytes(self)}
        if kargs:
            message["payload"] = kargs
        await stream.send_all(msgpack.packb(message))

    @classmethod
    def from_bytes(cls, v: bytes) -> tuple[Self, Any]:
        msg = msgpack.unpackb(v)
        try:
            messagetype = super().from_bytes(msg["messagetype"])
        except KeyError:
            raise MessageParseError("No messagetype in message") from None
        except (ValueError, TypeError):
            raise MessageParseError(
                f"Invalid messagetype in message: {msg['messagetype']}"
            ) from None
        return messagetype, msg.get("payload")

    @classmethod
    async def recv(cls, stream: trio.SocketStream) -> tuple[Self, Any]:
        msg = await stream.receive_some()
        return cls.from_bytes(msg)

    async def expect(self, stream: trio.SocketStream) -> Any:
        messagetype, payload = await self.recv(stream)
        if messagetype != self:
            raise UnexpectedMessageError(
                f"Received {messagetype.name}, expected {self.name}")
        return payload


class Status(MessageBase):
    Success = next(_unique)
    BadKey = next(_unique)
    Failure = next(_unique)

    async def send(self, stream: trio.SocketStream) -> None:
        await stream.send_all(bytes(self))

    @classmethod
    async def recv(cls, stream: trio.SocketStream) -> Self:
        return cls.from_bytes(await stream.receive_some())

    async def expect(self, stream: trio.SocketStream) -> None:
        status = await self.recv(stream)
        if status != self:
            raise UnexpectedMessageError(f"Received {status.name}, expected {self.name}")
