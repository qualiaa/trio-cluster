import dataclasses
import platform
import socket
from dataclasses import dataclass
from typing import Any, Self
from uuid import UUID, uuid4

import trio


@dataclass(slots=True, frozen=True)
class ListenAddress:
    uid: UUID
    hostname: str
    addr: str
    listen_port: int
    is_local: bool

    def __hash__(self):
        return hash(self.uid)

    def __eq__(self, o):
        if not isinstance(o, ListenAddress):
            return NotImplemented
        return self.uid == o.uid and self.uid is not None

    def __repr__(self):
        return f"{type(self).__name__}({self.uid!r}, {self.hostname}, {self.addr}, {self.listen_port}, is_local={self.is_local})"

    def __str__(self):
        return f"[{'*' if self.is_local else self.hostname}:{self.listen_port}] ({self.uid.hex[:6]})"

    @classmethod
    def local(cls, listen_port, uid: UUID = None):
        return cls(uid=uid or uuid4(),
                   hostname=platform.node(),
                   addr="localhost",
                   listen_port=listen_port,
                   is_local=True)

    @classmethod
    def server(cls, host_or_addr, port):
        hostname, _, [addr, *_] = socket.gethostbyaddr(host_or_addr)
        return cls(None, hostname, addr, port, addr_is_local(addr))

    @classmethod
    def from_server_blocking(cls, handle_from_server: Self, server: Self):
        """

        This may block!
        """
        if server.is_local:
            # Server is local to us, so we can use same handle
            return handle_from_server
        if handle_from_server.is_local:
            # Server is not local, but handle is local to server - use server's addr
            hostname = server.hostname
            addr = server.addr
            is_local = False
        else:
            # Handle is not local to server and so addr should work for us too
            # We need to check if the addr is local for us.
            hostname, _, [addr, *_] = socket.gethostbyaddr(handle_from_server.addr)
            is_local = addr_is_local(addr)
        return cls(uid=handle_from_server.uid,
                   hostname=hostname,
                   addr=addr,
                   listen_port=handle_from_server.listen_port,
                   is_local=is_local)

    @classmethod
    def from_inbound_connection(cls, stream: trio.SocketStream, uid, hostname, listen_port):
        if isinstance(uid, bytes):
            uid = UUID(bytes=uid)
        return cls(uid=uid,
                   hostname=hostname,
                   addr=get_addr(stream),
                   listen_port=listen_port,
                   is_local=socket_is_local(stream))

    @classmethod
    def from_dict(cls, d: dict[str, Any]):
        uid = d["uid"]
        if isinstance(uid, bytes):
            d = d.copy()
            d["uid"] = UUID(bytes=d["uid"])
        return cls(**d)

    def to_dict(self):
        return dataclasses.asdict(self) | {"uid": self.uid.bytes}


def get_addr(stream: trio.SocketStream) -> str:
    return stream.socket.getpeername()[0]


def addr_is_local(addr: str) -> bool:
    # This may not be entirely foolproof
    fqdn = socket.getfqdn(addr)
    return fqdn == "localhost" or fqdn == socket.getfqdn()


def socket_is_local(stream: trio.SocketStream) -> bool:
    return addr_is_local(get_addr(stream))
