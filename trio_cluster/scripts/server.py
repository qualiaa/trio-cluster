import logging
import time
from argparse import ArgumentParser
from functools import cached_property
from random import randbytes

import trio

from . import _constants as C
from .. import utils
from ..server import Server, Manager as BaseManager


class A():
    def __call__(self):
        time.sleep(1)
        return 5


class Manager(BaseManager):
    async def run(self, clients):
        def print_status():
            print("I'm alive!")
            for i, client in enumerate(clients()):
                print(f"\t{i}: {client.handle}")
            print()
        async with trio.open_nursery() as nursery:
            nursery.start_soon(utils.every, 2, print_status)

    # Lock around this
    async def handle_client_message(self, client, tag, data):
        print(f"{client} {tag}: {data}")

    # Lock around this
    async def new_client(self, client):
        await client.send("work_fn", A(), pickle=True)
        await client.send("big chungus", self.big_chungus)
        print("Sent big chungus!")

    @cached_property
    def big_chungus(self):
        return randbytes(100*1024*1024)


def run(args):
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    server = Server(args.registration_key, args.port, Manager())
    try:
        trio.run(server.listen)
    except KeyboardInterrupt:
        pass


def main():
    parser = ArgumentParser()
    parser.add_argument("registration_key")
    parser.add_argument("-p", "--port", type=int, default=C.DEFAULT_SERVER_PORT)
    parser.add_argument("-v", "--verbose", action="store_true")

    run(parser.parse_args())


if __name__ == "__main__":
    main()
