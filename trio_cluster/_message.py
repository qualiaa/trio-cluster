import math
from collections.abc import Mapping
from enum import IntEnum
from itertools import count
from logging import getLogger
from typing import Any, Self

import cloudpickle as cpkl
import msgpack
import trio

from ._exc import MessageParseError, UnexpectedMessageError

_LOG = getLogger(__name__)

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

    @classmethod
    def from_bytes(cls, v: bytes) -> Self:
        if len(v) != cls.byte_size():
            raise MessageParseError(
                f"{cls} is of size {cls.byte_size()} bytes, received {len(v)}")
        return cls(int.from_bytes(v, "little"))


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
    def from_bytes_with_payload(cls, v: bytes) -> tuple[Self, Any]:
        msg = msgpack.unpackb(v)
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


class Status(MessageBase):
    Success = next(_unique)
    BadKey = next(_unique)
    Failure = next(_unique)

    async def send(self, stream: trio.SocketStream) -> None:
        await stream.send_all(bytes(self))

    @classmethod
    async def recv(cls, stream: trio.SocketStream) -> Self:
        return cls.from_bytes(await stream.receive_some())

    def expect(self, status: Self) -> None:
        if status != self:
            raise UnexpectedMessageError(f"Received {self.name}, expected {status.name}")

    async def expect_from(self, stream: trio.SocketStream) -> None:
        status = await self.recv(stream)
        status.expect(self)


async def messages(stream, ignore_errors=False):
    backlog = bytearray()

    async for data in stream:
        if data == b"":
            # EOF
            return

        messages = []
        backlog.extend(data)
        while backlog:
            try:
                messages.append(msgpack.unpackb(backlog))
                backlog.clear()
            except msgpack.ExtraData as e:
                msg, backlog = e.args
                messages.append(msg)
            except ValueError:
                if len(backlog) > 100*1024*1024:
                    if not ignore_errors:
                        raise
                    _LOG.warning("Invalid/incomplete data ignored: %s", msg)
                    backlog.clear()
                break

        for msg in messages:
            try:
                yield Message.from_bytes(msg["messagetype"]), msg.get("payload")
            except (KeyError, TypeError, ValueError, MessageParseError):
                if not ignore_errors:
                    raise
                _LOG.warning("Invalid/incomplete message ignored: %s", msg)
                continue


def to_client_message(payload):
    tag, data = payload["tag"], payload["data"]
    if isinstance(data, Mapping) and data.get("_pickled"):
        data = cpkl.loads(data["_data"])
    return tag, data


async def client_messages(stream, ignore_errors=False):
    async for msgtype, payload in messages(stream, ignore_errors=ignore_errors):
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
                _LOG.warning("tag or data missing: %s", str(data.keys()))
