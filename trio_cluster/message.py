import math
from typing import Any, Self
from base64 import b64encode, b64decode
from enum import IntEnum
from itertools import count

import msgpack
import trio


_unique = count()


class MessageBase(IntEnum):
    # TODO: Replace this with a string-backed enum
    @classmethod
    def byte_size(cls) -> int:
        return math.floor(math.log2(max(cls))) + 1

    def __bytes__(self) -> bytes:
        return self.to_bytes()

    def to_bytes(self) -> bytes:
        return int.to_bytes(self, self.byte_size(), "little")

    def to_b64(self) -> bytes:
        return b64encode(bytes(self))

    @classmethod
    def from_bytes(cls, v: bytes) -> Self:
        if len(v) != cls.byte_size():
            raise ValueError(f"{cls} is of size {cls.byte_size()} bytes, received {len(v)}")
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
    PeerMessage = next(_unique)

    async def send(self, stream: trio.SocketStream, **kargs) -> None:
        message = {"messagetype": bytes(self)}
        if kargs:
            message["payload"] = kargs
        await stream.send_all(msgpack.packb(message))


    @classmethod
    async def recv(cls, stream: trio.SocketStream) -> tuple[Self, Any]:
        msg = await stream.receive_some()
        msg = msgpack.unpackb(msg)
        try:
            messagetype = cls.from_bytes(msg["messagetype"])
        except KeyError:
            raise ValueError("No messagetype in message") from None
        except (ValueError, TypeError):
            raise ValueError("Invalid messagetype in message:", msg["messagetype"]) from None
        return messagetype, msg.get("payload")

    async def expect(self, stream: trio.SocketStream) -> Any:
        messagetype, payload = await self.recv(stream)
        if messagetype != self:
            raise ValueError(f"Received {messagetype.name}, expected {self.name}")
        return payload


class Status(MessageBase):
    Success = next(_unique)
    BadKey = next(_unique)
    Failure = next(_unique)

    async def send(self, stream: trio.SocketStream) -> None:
        print(f"Sending {self.name}")
        await stream.send_all(bytes(self))

    @classmethod
    async def recv(cls, stream: trio.SocketStream) -> Self:
        return cls.from_bytes(await stream.receive_some())

    async def expect(self, stream: trio.SocketStream) -> None:
        status = await self.recv(stream)
        if status != self:
            raise ValueError(f"Received {status.name}, expected {self.name}")
