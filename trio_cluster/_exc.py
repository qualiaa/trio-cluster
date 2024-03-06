import trio


class Shutdown(SystemExit):
    """Intentional shutdown."""


class UserError(Exception):
    """Error occurred in user code."""


class MessageError(Exception):
    """Catch-all for message-related errors"""


class MessageParseError(MessageError):
    """Error occurred parsing message"""


class UnexpectedMessageError(MessageError):
    """Message was not expect()-ed"""


class SequenceError(Exception):
    """Something has happened out of sequence"""


class InternalError(Exception):
    """Something unexpected has happened."""


STREAM_ERRORS = trio.BrokenResourceError, trio.ClosedResourceError
