import trio
from functools import wraps
from inspect import iscoroutinefunction


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
