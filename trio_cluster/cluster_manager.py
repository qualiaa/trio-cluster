import socket
import time
from uuid import UUID

import msgpack
import cloudpickle as cpkl
import trio

from . import utils
from .constants import Command, Status
from .cluster_worker import _Worker


class A():
    def __call__(self):
        time.sleep(5)
        return 5


class ClusterManager:
    def __init__(self, registration_key, port):
        self._registration_key = registration_key
        self._port = port

        self._half_initialised = set()
        self._workers = dict()
        self._finished = trio.Event()

    async def listen(self):
        async with trio.open_nursery() as nursery:
            def print_status():
                print("I'm alive!")
                for i, (worker, _) in enumerate(self._workers.values()):
                    print(f"\t[{i}] {worker}")
                print()
            nursery.start_soon(utils.every, 2, print_status)
            listeners = await trio.open_tcp_listeners(self._port)
            for listener in listeners:
                _keepalive(listener.socket)
            await trio.serve_listeners(self._worker_task, listeners)
        print("Server done")

    async def _worker_task(self, worker_stream):
        async with worker_stream:
            try:
                worker = await self._register_worker(worker_stream)

                async with trio.open_nursery() as nursery:
                    # FIXME: Possible contention on these streams
                    for peer, peer_stream in self._workers.values():
                        nursery.start_soon(_send_peer, peer_stream, worker)
                        # TODO: Remove this part when listening impl is done
                        nursery.start_soon(_send_peer, worker_stream, peer)

                await self._manage_worker(worker, worker_stream)

                async with trio.open_nursery() as nursery:
                    # FIXME: Possible contention on these streams
                    for peer, peer_stream in self._workers.values():
                        nursery.start_soon(_remove_peer, peer_stream, worker)
            except Exception as e:
                print("Worker exception", type(e), e)

    async def _manage_worker(self, worker, worker_stream):
        self._workers[worker.uid] = worker, worker_stream
        try:
            await self._receive_worker_commands(worker, worker_stream)
        finally:
            del self._workers[worker.uid]
            with trio.move_on_after(1) as cleanup_scope:
                cleanup_scope.shield = True
                await _send_command(worker_stream, Command.Shutdown)

    async def _receive_worker_commands(self, worker, worker_stream):
        print("Connected to command stream")
        async for msg in worker_stream:
            print(msgpack.unpackb(msg))

    async def _register_worker(self, worker_stream):
        registration_msg = await _receive_message(worker_stream)

        if registration_msg["key"] != self._registration_key:
            await worker_stream.send_all(cpkl.dumps({"status": Status.BadKey}))

        else:
            worker = _Worker(
                hostname=worker_stream.socket.getpeername()[0],
                uid=UUID(bytes=registration_msg["uid"]),
                port=registration_msg["port"])

            await worker_stream.send_all(cpkl.dumps({
                "status": Status.Success,
                "work_fn": A(),
                "hostname": worker.hostname
            }))
            return worker


async def _send_command(stream, command, **kargs):
    message = {"command": command.to_b64().decode("ascii")}
    if kargs:
        message["payload"] = kargs
    await stream.send_all(msgpack.packb(message))


async def _receive_message(stream):
    return msgpack.unpackb(await stream.receive_some())


async def _send_peer(stream, worker):
    await _send_command(stream, Command.NewPeer, **worker.to_dict())


async def _remove_peer(stream, worker):
    await _send_command(stream, Command.RemovePeer, uid=worker.uid.bytes)


def _keepalive(sock):
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
