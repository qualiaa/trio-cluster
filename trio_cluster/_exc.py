import trio


class UserError(Exception):
    """Error occurred in user code."""


class SequenceError(Exception):
    """Something has happened out of sequence"""


class InternalError(Exception):
    """Something unexpected has happened."""


STREAM_ERRORS = trio.BrokenResourceError, trio.ClosedResourceError
