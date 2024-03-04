import dataclasses
from dataclasses import dataclass
from typing import Any
from uuid import UUID


@dataclass(slots=True)
class ClientHandle:
    uid: UUID
    hostname: str
    port: int

    def __hash__(self):
        return hash(self.uid)

    def __eq__(self, o):
        if not isinstance(o, ClientHandle):
            return NotImplemented
        return self.uid == o.uid

    def __repr__(self):
        return f"{type(self).__name__}({self.uid!r}, {self.hostname}, {self.port})"

    def __str__(self):
        return f"[{self.hostname}]:{self.port} ({self.uid.hex[:6]})"

    @classmethod
    def from_dict(cls, d: dict[str, Any]):
        uid = d["uid"]
        if isinstance(uid, bytes):
            d = d.copy()
            d["uid"] = UUID(bytes=d["uid"])
        return cls(**d)

    def to_dict(self):
        return dataclasses.asdict(self) | {"uid": self.uid.bytes}
