from argparse import ArgumentParser

import trio

from .. import constants as C
from ..server import Server


def run(args):
    server = Server(args.registration_key, args.port)
    try:
        trio.run(server.listen)
    except KeyboardInterrupt:
        pass


def main():
    parser = ArgumentParser()
    parser.add_argument("registration_key")
    parser.add_argument("-p", "--port", type=int, default=C.DEFAULT_SERVER_PORT)
    run(parser.parse_args())


if __name__ == "__main__":
    main()
