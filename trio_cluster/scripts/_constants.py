from itertools import count

_unique_port = count(5757)
DEFAULT_SERVER_PORT = next(_unique_port)
DEFAULT_CLIENT_PORT = next(_unique_port)
