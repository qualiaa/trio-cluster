from argparse import ArgumentParser

import trio

from .. import constants as C
from ..cluster_manager import ClusterManager


def run(args):
    server = ClusterManager(args.registration_key,
                            args.registration_port,
                            args.heartbeat_period,
                            args.heartbeat_timeout)
    trio.run(server.listen)


def main():
    parser = ArgumentParser()
    parser.add_argument("registration_key")
    parser.add_argument("--registration-port", type=int, default=C.DEFAULT_REGISTRATION_PORT)
    parser.add_argument("--heartbeat-period", type=int, default=C.DEFAULT_HEARTBEAT_PERIOD)
    parser.add_argument("--heartbeat-timeout", type=int, default=C.DEFAULT_HEARTBEAT_TIMEOUT)

    run(parser.parse_args())


if __name__ == "__main__":
    main()
