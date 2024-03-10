import math
from collections.abc import Mapping
from contextlib import aclosing
from enum import IntEnum
from itertools import count
from logging import getLogger
from typing import Any, Self

import cloudpickle as cpkl
import msgpack
import trio

from ._exc import MessageParseError, UnexpectedMessageError

_LOG = getLogger(__name__)

# TODO: Make this configurable
_MAX_BACKLOG_BYTES = 200*1024*1024
_unique = count()

# FIXME: The message "protocol" here is basically insane - I just kept adding
#        things as I needed them. Could definitely unify all of these
#        requirements much more cleanly and with less data on the wire.


class MessageBase(IntEnum):
    @classmethod
    def byte_size(cls) -> int:
        return 1 + int(math.log2(max(cls))//8)

    def __bytes__(self) -> bytes:
        return self.to_bytes()

    def to_bytes(self) -> bytes:
        return int.to_bytes(self, self.byte_size(), "little")

    @classmethod
    def from_bytes(cls, v: bytes) -> Self:
        if len(v) != cls.byte_size():
            raise MessageParseError(
                f"{cls} is of size {cls.byte_size()} bytes, received {len(v)}")
        return cls(int.from_bytes(v, "little"))


class Message(MessageBase):
    Status = next(_unique)
    Shutdown = next(_unique)
    Registration = next(_unique)
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
    def from_bytes_with_payload(cls, v: bytes) -> tuple[Self, Any]:
        try:
            msg = msgpack.unpackb(v)
        except msgpack.ExtraData as e:
            raise MessageParseError(f"Extra data received: `{e.args[1]}'")
        try:
            messagetype = cls.from_bytes(msg["messagetype"])
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
        return cls.from_bytes_with_payload(msg)

    def expect(self, messagetype: Self) -> None:
        if messagetype != self:
            raise UnexpectedMessageError(
                f"Received {self.name}, expected {messagetype.name}")

    async def expect_from(self, stream: trio.SocketStream) -> Any:
        messagetype, payload = await self.recv(stream)
        messagetype.expect(self)
        return payload


class Status(IntEnum):
    Success = 1
    BadKey = 2
    Failure = 3

    async def send(self, stream: trio.SocketStream) -> None:
        await Message.Status.send(stream, status=self)

    @classmethod
    async def recv(cls, stream: trio.SocketStream) -> Self:
        return Status(await Message.Status.expect_from(stream)["status"])

    def expect(self, status: Self) -> None:
        if status != self:
            raise UnexpectedMessageError(f"Expected {status.name}, received {self.name}")

    async def expect_from(self, stream: trio.SocketStream) -> None:
        status = await self.recv(stream)
        status.expect(self)


def to_client_message(payload):
    tag, data = payload["tag"], payload["data"]
    if isinstance(data, Mapping) and data.get("_pickled"):
        data = cpkl.loads(data["_data"])
    return tag, data


async def messages(stream, ignore_errors=False):
    unpacker = msgpack.Unpacker(max_buffer_size=_MAX_BACKLOG_BYTES)
    async for data in stream:
        if data == b"":
            # EOF
            return

        try:
            unpacker.feed(data)
        except msgpack.BufferFull:
            if not ignore_errors:
                raise
            _LOG.warning("Up to %.4g MiB could not be parsed and were discarded",
                         _MAX_BACKLOG_BYTES/1024/1024)
            unpacker = msgpack.Unpacker()

        for msg in unpacker:
            try:
                yield Message.from_bytes(msg["messagetype"]), msg.get("payload")
            except (KeyError, TypeError, ValueError, MessageParseError):
                if not ignore_errors:
                    raise
                _LOG.warning("Invalid/incomplete message ignored: %s", msg)
            await trio.lowlevel.checkpoint()


async def client_messages(stream, ignore_errors=False):
    async with aclosing(messages(stream, ignore_errors=ignore_errors)) as msgs:
        async for msgtype, payload in msgs:
            try:
                msgtype.expect(Message.ClientMessage)
                yield to_client_message(payload)
            except Exception as e:
                if not ignore_errors:
                    raise
                try:
                    raise e
                except TypeError:
                    _LOG.warning("Payload has wrong type: %s", type(payload))
                except UnexpectedMessageError:
                    _LOG.warning("Unexpected messagetype from client: %s", msgtype.name)
                except KeyError:
                    _LOG.warning("tag or data missing: %s", str(payload.keys()))
