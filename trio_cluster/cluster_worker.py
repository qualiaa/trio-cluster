import json
from argparse import ArgumentParser
from copy import deepcopy
from dataclasses import dataclass
from functools import partial

import cloudpickle as cpkl
import trio
import trio_parallel
import trio_util

from . import constants as C
from . import utils
from .constants import Command, Status


@dataclass
class _WorkerHandle:
    hostname: str
    command_port: int
    data_port: int

    def __hash__(self):
        return hash((self.hostname, self.command_port, self.data_port))

    @classmethod
    def from_dict(cls, d):
        return cls(**d)

    def to_dict(self):
        return deepcopy(self.__dict__)


class ClusterWorker:
    def __init__(self, data_port, command_port):
        # NOTE: The hostname will be overwritten by the host on registration
        self._handle = _WorkerHandle("localhost", command_port, data_port)

    async def run(self, server_hostname, registration_port, registration_key):
        work_fn = await self.register(server_hostname, registration_port, registration_key)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(utils.tcp_accept_one,
                               partial(self._process_commands, cancel_scope=nursery.cancel_scope),
                               self._handle.command_port)
            nursery.start_soon(self.do_work, work_fn)
        print("Client done")

    async def do_work(self, fn):
        print("Starting work")
        await trio_parallel.run_sync(fn, cancellable=True)
        print("Finished work")
        return "Result"

    async def _process_commands(self, command_stream, cancel_scope):
        print ("Waiting for commands")
        async with command_stream:
            async for msg in command_stream:
                try:
                    msg = json.loads(msg)
                    command = Command.from_b64(msg["command"])
                except json.JSONDecodeError:
                    print(f"Invalid JSON received:", msg)
                    continue
                except ValueError:
                    print(f"Invalid command received:", msg["command"])
                    continue

                if command == Command.Heartbeat:
                    print("Heartbeat received")
                    peers = set(_WorkerHandle.from_dict(w) for w in msg["data"])
                    self._peers = peers - {self._handle}
                    await command_stream.send_all(Status.Success.to_bytes())
                    print(self._peers)

                elif command == Command.Shutdown:
                    print("Shutdown received")
                    await command_stream.send_all(Status.Success.to_bytes())
                    break

        cancel_scope.cancel()
        print("Command listener closing")

    async def register(self, host, registration_port, registration_key):
        tcp_stream = await utils.open_tcp_stream_retry(host, registration_port)

        async with tcp_stream:
            await tcp_stream.send_all(json.dumps({
                "key": registration_key,
                "handle": self._handle.to_dict(),
            }).encode())

            registration_response = cpkl.loads(await tcp_stream.receive_some())

            if registration_response["status"] == Status.Success:
                self._handle.hostname = registration_response["worker_hostname"]
                return registration_response["work_fn"]
            # TODO: Retry until successful or the worker is shut down
            raise RuntimeError()


def main(args):
    worker = ClusterWorker(args.data_port, args.command_port)
    trio.run(worker.run,
             args.server_hostname,
             args.registration_port,
             args.registration_key)



if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("registration_key")
    parser.add_argument("--server-hostname", default="localhost")
    parser.add_argument("--registration-port", type=int, default=C.DEFAULT_REGISTRATION_PORT)
    parser.add_argument("--data-port", type=int, default=C.DEFAULT_DATA_PORT)
    parser.add_argument("--command-port", type=int, default=C.DEFAULT_COMMAND_PORT)
    main(parser.parse_args())
