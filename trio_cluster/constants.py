from itertools import count

_unique_port = count(5757)
DEFAULT_SERVER_PORT = next(_unique_port)
DEFAULT_PEER_PORT = next(_unique_port)

DEFAULT_HEARTBEAT_PERIOD = 3
DEFAULT_HEARTBEAT_TIMEOUT = 30
