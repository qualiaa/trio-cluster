import dataclasses
from dataclasses import dataclass
from uuid import UUID, uuid4

from . import utils
import cloudpickle as cpkl
import msgpack
import trio
import trio_parallel

from .constants import Command, Status


@dataclass
class _Client:
    uid: UUID
    hostname: str
    port: int

    def __hash__(self):
        return hash(self.uid)

    def __repr__(self):
        return f"{type(self).__name__}({self.uid!r}, {self.hostname}, {self.port})"

    def __str__(self):
        return f"[{self.hostname}]:{self.port} ({self.uid.hex[:6]})"

    @classmethod
    def from_dict(cls, d):
        return cls(**d | {"uid": UUID(bytes=d["uid"])})

    def to_dict(self):
        return dataclasses.asdict(self) | {"uid": self.uid.bytes}


class ClusterWorker:
    def __init__(self, port):
        self._uid = uuid4()
        self._hostname = "localhost"
        self._port = port
        self._peers = {}

    async def run(self, server_hostname, server_port, registration_key):
        work_fn, server_stream = await self.register(server_hostname, server_port, registration_key)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(utils.every, 2, lambda: print(f"I'm alive: {self._peers}\n"))
            nursery.start_soon(self.do_work, work_fn)
            await self._process_commands(server_stream, cancel_scope=nursery.cancel_scope)
        print("Client done")

    async def do_work(self, fn):
        while True:
            print("Starting work")
            await trio_parallel.run_sync(fn, cancellable=True)
            print("Finished work")

    async def _process_commands(self, server_stream, cancel_scope):
        print ("Waiting for commands")
        async with server_stream:
            async for msg in server_stream:
                try:
                    msg = msgpack.loads(msg)
                    payload = msg.get("payload")
                except Exception:
                    print("Invalid message received")
                    continue
                try:
                    command = Command.from_b64(msg["command"])
                except ValueError:
                    print(f"Invalid command received:", msg["command"])
                    continue

                print(command.name, "received")
                match command:
                    case Command.Shutdown:
                        await server_stream.send_all(Status.Success.to_bytes())
                        break
                    case Command.NewPeer:
                        peer = _Worker.from_dict(payload)
                        if peer.uid == self._uid:
                            print("This is me")
                        elif peer.uid in self._peers:
                            print("Already have peer!")
                        else:
                            self._peers[peer.uid] = peer
                    case Command.RemovePeer:
                        uid = UUID(bytes=payload["uid"])
                        try:
                            del self._peers[uid]
                        except KeyError:
                            print("No such peer")
                    case _:
                        print("Unhandled command:", command.name, payload)

        cancel_scope.cancel()
        print("Command listener closing")

    async def register(self, host, server_port, registration_key):
        server_stream = await trio.open_tcp_stream(host, server_port)

        await _send_message(server_stream, {
            "key": registration_key,
            "port": self._port,
            "uid": self._uid.bytes
        })

        registration_response = cpkl.loads(await server_stream.receive_some())

        if registration_response["status"] != Status.Success:
            # TODO: Retry until successful or the worker is shut down
            raise RuntimeError()
        self._hostname = registration_response["hostname"]
        return registration_response["work_fn"], server_stream


async def _send_message(stream, msg):
    msg = msgpack.packb(msg)
    await stream.send_all(msg)
