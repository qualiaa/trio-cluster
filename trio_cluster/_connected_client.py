from dataclasses import dataclass
from logging import getLogger
from typing import Any, Awaitable, Callable, Optional, TypeAlias, Union

import cloudpickle as cpkl
import trio

from . import utils
from ._exc import UnexpectedMessageError
from ._listen_address import ListenAddress
from ._message import MessageGenerator, Message, Status


_LOG = getLogger(__name__)

_FailureCallback: TypeAlias = Union[
    Callable[[], Awaitable[None]],
    Callable[[], None]
]


class ClientMessageSender:
    """Callable for sending ClientMessages to a specific stream."""
    def __init__(
            self,
            stream: trio.SocketStream,
            lock: trio.Lock,
            responses: Optional[MessageGenerator] = None,
            stream_failure_callback: _FailureCallback | None = None):
        self._stream = stream
        self._lock = lock
        self._responses = responses

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
        if self._responses:
            try:
                # TODO: This can obviously be improved
                msgtype, payload = await anext(self._responses)
                msgtype.expect(Message.Status)
                Status(payload["status"]).expect(Status.Success)
            except (trio.BrokenResourceError, StopAsyncIteration):
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
    handle: ListenAddress
    send: ClientMessageSender

    @property
    def local(self):
        return self.handle.is_local

    def __str__(self):
        return str(self.handle)

    def __repr__(self):
        return f"<ConnectedClient({self.handle!r})>"

    def __eq__(self, o):
        raise TypeError("Cannot check ConnectedClient for equality")


ActiveClientsFn: TypeAlias = Callable[[], list[ConnectedClient]]
