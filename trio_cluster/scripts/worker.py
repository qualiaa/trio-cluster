import logging
from argparse import ArgumentParser
from typing import Any

import cloudpickle as cpkl
import trio
import trio_parallel

from . import _constants as C
from .. import utils
from ..client import Client, Worker as WorkerBase


class Worker(WorkerBase):
    def __init__(self):
        self._work_fn = None

    async def run(self, peers, server_send):
        print("Running worker!")
        async with trio.open_nursery() as nursery:
            nursery.start_soon(utils.aevery, 1, self._ping_peers, peers, server_send)
            nursery.start_soon(self._do_work, server_send)

    async def _ping_peers(self, peers, server_send):
        for peer in peers():
            try:
                await peer.send("ping", "Hello")
            except Exception as e:
                print("Error messaging peer:", type(e), *e.args)
        await server_send("ping", "Hello")

    async def _do_work(self, server_send):
        while True:
            if self._work_fn is not None:
                print("Doing work")
                result = await trio_parallel.run_sync(self._work_fn, cancellable=True)
                await server_send("result", result)
            else:
                await trio.sleep(2)
            if self._work_fn is not None:
                print("Done!")

    async def handle_peer_message(self, peer_handle, tag: str, data: Any):
        match tag:
            case "ping": print(f"[{peer_handle}] {data}")
            case tag: print(f"[{peer_handle}] unknown tag {tag}")

    async def handle_server_message(self, tag: str, data: Any):
        match tag:
            case "work_fn": self._work_fn = cpkl.loads(data)
            case tag: print(f"[server] unknown tag {tag}")


def run(args):
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    worker = Client(args.port, Worker())
    try:
        trio.run(worker.run,
                 args.server_hostname,
                 args.server_port,
                 args.registration_key)
    except KeyboardInterrupt:
        pass


def main():
    parser = ArgumentParser()
    parser.add_argument("registration_key")
    parser.add_argument("-p", "--port", type=int, default=C.DEFAULT_CLIENT_PORT)
    parser.add_argument("-H", "--server-hostname", default="localhost")
    parser.add_argument("-P", "--server-port", type=int, default=C.DEFAULT_SERVER_PORT)
    parser.add_argument("-v", "--verbose", action="store_true")
    run(parser.parse_args())


if __name__ == "__main__":
    main()
