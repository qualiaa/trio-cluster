from logging import getLogger
from dataclasses import dataclass
from typing import Any

import cloudpickle as cpkl
import trio

from ._client_handle import ClientHandle
from .message import Message, Status


_LOG = getLogger(__name__)


class ClientMessageSender:
    """Callable for sending ClientMessages to a specific stream."""
    def __init__(self,
                 stream: trio.SocketStream,
                 lock: trio.Lock,
                 await_response=True):
        self._stream = stream
        self._lock = lock
        self._await_response = await_response

    async def __call__(self, tag: str, data: Any, pickle=False) -> bool:
        """Send the client-message to the destination.

        If this raises, then the data has (most likely) not been sent.

        If this function returns False, it means that the data has been sent,
        but either no reply was received or the receiver signalled a failure.
        """
        if pickle:
            data = cpkl.dumps(data)
        try:
            async with self._lock:
                await Message.ClientMessage.send(
                    self._stream, tag=tag, data=data)
        except Exception:
            _LOG.exception("Error occurred while sending data")
            return False
        if self._await_response:
            try:
                await Status.Success.expect(self._stream)
            except Exception:
                _LOG.exception("Peer did not signal success")
                return False
        return True


@dataclass(frozen=True, slots=True)
class ConnectedClient:
    handle: ClientHandle
    send: ClientMessageSender

    def __str__(self):
        return str(self.handle)

    def __repr__(self):
        return f"<ConnectedClient({self.handle!r})>"

    def __hash__(self):
        return hash(self.handle)

    def __eq__(self, o):
        if not isinstance(o, ConnectedClient):
            return NotImplemented
        return self.handle == o.handle
