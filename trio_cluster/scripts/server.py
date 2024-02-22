from argparse import ArgumentParser

import trio

from .. import constants as C
from ..cluster_manager import ClusterManager


def run(args):
    server = ClusterManager(args.registration_key, args.port)
    trio.run(server.listen)


def main():
    parser = ArgumentParser()
    parser.add_argument("registration_key")
    parser.add_argument("-p", "--port", type=int, default=C.DEFAULT_SERVER_PORT)
    run(parser.parse_args())


if __name__ == "__main__":
    main()
