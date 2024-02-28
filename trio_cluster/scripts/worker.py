from argparse import ArgumentParser
from typing import Any

import trio
import trio_parallel

from .. import utils, constants as C
from ..client import Client, Worker as WorkerBase


class Worker(WorkerBase):
    async def run(self, work_detail, peers, server_send):
        async with trio.open_nursery() as nursery:
            nursery.start_soon(utils.aevery, 1, self._ping_peers, peers)
            nursery.start_soon(self._do_work, work_detail)

    async def _ping_peers(self, peers):
        for peer in peers():
            try:
                await peer.send("ping", "Hello")
            except Exception as e:
                print("Error messaging peer:", type(e), *e.args)

    async def _do_work(self, work_fn):
        while True:
            print("Doing work")
            await trio_parallel.run_sync(work_fn, cancellable=True)
            print("Done!")

    async def handle_peer_message(self, peer_handle, tag: str, data: Any):
        match tag:
            case "ping": print(f"[{peer_handle}] {data}")
            case tag: print(f"[{peer_handle}] unknown tag {tag}")

    async def handle_server_message(self, tag: str, data: Any):
        pass


def run(args):
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
    run(parser.parse_args())


if __name__ == "__main__":
    main()
