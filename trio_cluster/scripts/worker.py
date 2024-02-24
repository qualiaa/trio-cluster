from argparse import ArgumentParser

import trio

from .. import constants as C
from ..cluster_worker import ClusterWorker


def run(args):
    worker = ClusterWorker(args.port)
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
    parser.add_argument("-p", "--port", type=int, default=C.DEFAULT_PEER_PORT)
    parser.add_argument("-H", "--server-hostname", default="localhost")
    parser.add_argument("-P", "--server-port", type=int, default=C.DEFAULT_SERVER_PORT)
    run(parser.parse_args())


if __name__ == "__main__":
    main()
