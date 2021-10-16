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
    while True:
        func(*args, **kargs)
        await trio.sleep(time)


async def aevery(time, func, *args, **kargs):
    while True:
        await func(*args, **kargs)
        await trio.sleep(time)


async def open_tcp_stream_retry(*args, backoff=0.1, **kargs):
    while True:
        try:
            return await trio.open_tcp_stream(*args, **kargs)
        except OSError:
            await trio.sleep(backoff)
