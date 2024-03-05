import socket
from logging import getLogger
from functools import wraps
from inspect import iscoroutinefunction
from typing import NoReturn

import trio

_LOG = getLogger(__name__)


async def race(async_fn, *async_fns):
    async_fns += async_fn,
    winner = None

    async def run(async_fn, cancel_scope):
        nonlocal winner
        winner = await async_fn()
        cancel_scope.cancel()

    async with trio.open_nursery() as nursery:
        for async_fn in async_fns:
            nursery.start_soon(run, async_fn, nursery.cancel_scope)
    return winner


async def every(seconds: float, func, *args, **kargs) -> NoReturn:
    await aevery(seconds, as_coroutine(func), *args, **kargs)


async def aevery(seconds: float, func, *args, **kargs) -> NoReturn:
    while True:
        await func(*args, **kargs)
        await trio.sleep(seconds)


def get_hostname(stream) -> str:
    return stream.socket.getpeername()[0]


def noexcept(*to_throw, log=None, catch_base=False):
    if to_throw:
        # Special handling if first argument is a callable to wrap
        first, *rest = to_throw
        is_exc = isinstance(first, type) and issubclass(first, BaseException)
        if callable(first) and not is_exc:
            return noexcept(*rest, log=log)(first)

    catch = BaseException if catch_base else Exception

    def decorator(f):
        @wraps(f)
        async def wrapped(*args, **kargs):
            try:
                return await f(*args, **kargs)
            except to_throw:
                raise
            except catch as e:
                (log or _LOG).warning(
                    "Ignoring exception in %s: %s %s ",
                    f.__qualname__, type(e), type(e), *e.args)
        return wrapped
    return decorator


def as_coroutine(f):
    """Awaitable wrapper for f."""
    if iscoroutinefunction(f):
        return f
    if not callable(f):
        raise TypeError(f"Expected coroutine or callable, got {type(f)}.")

    @wraps(f)
    async def call(*args, **kargs):
        return f(*args, **kargs)
    return call


async def open_tcp_stream_retry(*args, wait: float = 1, **kargs) -> trio.SocketStream:
    while True:
        try:
            return await trio.open_tcp_stream(*args, **kargs)
        except OSError:
            await trio.sleep(wait)


def set_keepalive(sock: socket.socket) -> None:
    # FIXME: One of these settings becomes irrelevant when USER_TIMEOUT
    #        provided... remember which one
    # Enable TCP keepalive
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    # Keepalive attempts (-1 for the initial keepalive)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 2)

    # Connection idle time before sending first keepalive probe
    # TODO: Increase keepalive times
    # TODO: Stagger keepalive times
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 1)
    # Delay between subsequent keepalive probes. Should be relatively prime to
    # TCP_KEEPIDLE
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 1)
    # User timeout - this ensures that interrupted sends do not override
    #                keepalive
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_USER_TIMEOUT, 1)
