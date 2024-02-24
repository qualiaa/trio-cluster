import math
import msgpack
from base64 import b64encode, b64decode
from enum import IntEnum
from itertools import count


_unique = count()


class MessageBase(IntEnum):
    # TODO: Replace this with a string-backed enum
    @classmethod
    def byte_size(cls):
        return math.floor(math.log2(max(cls))) + 1

    def __bytes__(self):
        return self.to_bytes()

    def to_bytes(self):
        return int.to_bytes(self, self.byte_size(), "little")

    def to_b64(self):
        return b64encode(bytes(self))

    @classmethod
    def from_bytes(cls, v):
        if len(v) != cls.byte_size():
            raise ValueError(f"{cls} is of size {cls.byte_size()} bytes, received {len(v)}")
        return cls(int.from_bytes(v, "little"))

    @classmethod
    def from_b64(cls, v):
        return cls.from_bytes(b64decode(v))


class Message(MessageBase):
    Shutdown = next(_unique)
    NewPeer = next(_unique)
    RemovePeer = next(_unique)
    ConnectPing = next(_unique)
    ConnectPong = next(_unique)

    async def send(self, stream, **kargs):
        message = {"command": bytes(self)}
        if kargs:
            message["payload"] = kargs
        await stream.send_all(msgpack.packb(message))

    @classmethod
    async def recv(cls, stream):
        msg = await stream.receive_some()
        msg = msgpack.unpackb(msg)
        try:
            command = cls.from_bytes(msg["command"])
        except KeyError:
            raise ValueError("No command in message") from None
        except (ValueError, TypeError):
            raise ValueError("Invalid command in message:", msg["command"]) from None
        return command, msg.get("payload")

    async def expect(self, stream):
        command, payload = await self.recv(stream)
        if command != self:
            raise ValueError(f"Received {command.name}, expected {self.name}")
        return payload


class Status(MessageBase):
    Success = next(_unique)
    BadKey = next(_unique)
    Failure = next(_unique)

    async def send(self, stream):
        print(f"Sending {self.name}")
        await stream.send_all(bytes(self))

    @classmethod
    async def recv(cls, stream):
        return cls.from_bytes(await stream.receive_some())

    async def expect(self, stream):
        status = await self.recv(stream)
        if status != self:
            raise ValueError(f"Received {status.name}, expected {self.name}")
