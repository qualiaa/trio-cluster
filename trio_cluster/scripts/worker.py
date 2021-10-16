from argparse import ArgumentParser

import trio

from .. import constants as C
from ..cluster_worker import ClusterWorker


def run(args):
    worker = ClusterWorker(args.data_port, args.command_port)
    trio.run(worker.run,
             args.server_hostname,
             args.registration_port,
             args.registration_key)


def main():
    parser = ArgumentParser()
    parser.add_argument("registration_key")
    parser.add_argument("--server-hostname", default="localhost")
    parser.add_argument("-r", "--registration-port", type=int, default=C.DEFAULT_REGISTRATION_PORT)
    parser.add_argument("-c", "--command-port", type=int, default=C.DEFAULT_COMMAND_PORT)
    parser.add_argument("-d", "--data-port", type=int, default=C.DEFAULT_DATA_PORT)
    run(parser.parse_args())


if __name__ == "__main__":
    main()
