import dataclasses
from dataclasses import dataclass
from typing import Any, Optional
from uuid import UUID, uuid4


@dataclass(slots=True)
class ClientHandle:
    uid: Optional[UUID]
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
        return cls(**d | {"uid": UUID(bytes=d["uid"])})

    def to_dict(self):
        return dataclasses.asdict(self) | {"uid": self.uid.bytes}
