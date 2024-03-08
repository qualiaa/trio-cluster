from logging import getLogger
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, TypeAlias, Union

import cloudpickle as cpkl
import trio

from . import utils
from ._client_handle import ClientHandle
from ._exc import UnexpectedMessageError
from ._message import Message, Status


_LOG = getLogger(__name__)

FailureCallback: TypeAlias = Union[
    Callable[[], Awaitable[None]],
    Callable[[], None]
]


class ClientMessageSender:
    """Callable for sending ClientMessages to a specific stream."""
    def __init__(
            self,
            stream: trio.SocketStream,
            lock: trio.Lock,
            await_response=True,
            stream_failure_callback: FailureCallback | None = None):
        self._stream = stream
        self._lock = lock
        self._await_response = await_response

        self._stream_failure_callback = utils.ascoroutinefunction(
            (lambda: None) if stream_failure_callback is None else
            stream_failure_callback
        )

    async def __call__(self,
                       tag: str,
                       data: Any,
                       pickle=False,
                       ignore_errors=False) -> bool:
        """Send the client-message to the destination.

        If this raises, then the data has (most likely) not been sent.

        If this function returns False, it means that the data has been sent,
        but either no reply was received or the receiver signalled a failure.
        """
        if pickle:
            data = {"_pickled": True, "_data": cpkl.dumps(data)}
        try:
            async with self._lock:
                await Message.ClientMessage.send(
                    self._stream, tag=tag, data=data)
        except trio.BrokenResourceError:
            _LOG.debug("Calling stream failure callback")
            await self._stream_failure_callback()
            if not ignore_errors:
                raise
        except trio.ClosedResourceError:
            if not ignore_errors:
                raise

        # From this point, we return False rather than raising an exception.
        if self._await_response:
            try:
                await Status.Success.expect_from(self._stream)
            except trio.BrokenResourceError:
                _LOG.debug("Calling stream failure callback")
                await self._stream_failure_callback()
                return False
            except (trio.ClosedResourceError, UnexpectedMessageError):
                return False
            except Exception:
                _LOG.exception("Unexpected exception")
                return False
        return True

    def __eq__(self, o):
        raise TypeError("Cannot check ClientMessageSender for equality")


@dataclass(frozen=True, slots=True)
class ConnectedClient:
    handle: ClientHandle
    send: ClientMessageSender

    def __str__(self):
        return str(self.handle)

    def __repr__(self):
        return f"<ConnectedClient({self.handle!r})>"

    def __eq__(self, o):
        raise TypeError("Cannot check ConnectedClient for equality")
