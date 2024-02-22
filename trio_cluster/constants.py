import math
from enum import IntEnum
from base64 import b64encode, b64decode
from itertools import count

_unique_port = count(5757)
DEFAULT_SERVER_PORT = next(_unique_port)
DEFAULT_PEER_PORT = next(_unique_port)

DEFAULT_HEARTBEAT_PERIOD = 3
DEFAULT_HEARTBEAT_TIMEOUT = 30


class MessageType(IntEnum):
    # TODO: Replace this with a string-backed enum
    @classmethod
    @property
    def byte_size(cls):
        return math.floor(math.log2(max(cls))) + 1

    def to_bytes(self):
        return int.to_bytes(self, self.byte_size, "little")

    def to_b64(self):
        return b64encode(self.to_bytes())

    @classmethod
    def from_bytes(cls, v):
        if len(v) != cls.byte_size:
            raise ValueError(f"{cls} is of size {cls.byte_size} bytes, received {len(v)}")
        return cls(int.from_bytes(v, "little"))

    @classmethod
    def from_b64(cls, v):
        return cls.from_bytes(b64decode(v))


_unique = count()


class Command(MessageType):
    Shutdown = next(_unique)
    NewPeer = next(_unique)
    RemovePeer = next(_unique)


class Status(MessageType):
    Success = next(_unique)
    BadKey = next(_unique)
    Failed = next(_unique)
