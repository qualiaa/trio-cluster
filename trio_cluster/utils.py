import socket
from functools import wraps
from inspect import iscoroutinefunction

import trio


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


async def tcp_accept_one(handler, port, *, host=None, backlog=None):
    print("Listening for things")
    listeners = await trio.open_tcp_listeners(port, host=host, backlog=backlog)
    print("Starting race")
    tcp_stream = await race(*[l.accept for l in listeners])
    print("Got one")
    for l in listeners:
        await l.aclose()

    print("Starting handler")
    await handler(tcp_stream)


async def every(time, func, *args, **kargs):
    await aevery(time, as_coroutine(func), *args, **kargs)


async def aevery(time, func, *args, **kargs):
    while True:
        await func(*args, **kargs)
        await trio.sleep(time)


def get_hostname(stream):
    return stream.socket.getpeername()[0]


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


def set_keepalive(sock):
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
