import itertools
import json
from argparse import ArgumentParser
from functools import partial

import cloudpickle as cpkl
import trio

from . import constants as C
from . import utils
from .constants import Command, Status

from .work_fn import work_fn
from .worker import _WorkerHandle


class A():
    def __call__():
        return itertools.islice(itertools.count(), 500000000)


class ClusterManager:
    def __init__(self, registration_key, registration_port, heartbeat_period, heartbeat_timeout):
        self._registration_key = registration_key
        self._registration_port = registration_port
        self._heartbeat_period = heartbeat_period
        self._heartbeat_timeout = heartbeat_timeout

        self._workers = set()
        self._finished = trio.Event()

    async def listen(self):
        # Channel for transferring registered _WorkerHandles
        reg_send, reg_recv = trio.open_memory_channel(0)

        async with reg_send, reg_recv, trio.open_nursery() as nursery:
            nursery.start_soon(utils.every, 5, lambda: print(f"I'm alive: {self._workers}\n"))
            nursery.start_soon(self._listen_for_new_workers, reg_send)

            async for worker_handle in reg_recv:
                nursery.start_soon(partial(self._manage_worker, worker_handle))

        print("Server done")

    async def _manage_worker(self, worker: _WorkerHandle):
        self._workers.add(worker)
        # FIXME: Failures handling workers should not crash server
        # TODO: Individual failures are perhaps worth restarting
        try:
            async with trio.open_nursery() as nursery:
                nursery.start_soon(self._send_worker_commands, worker)
        finally:
            self._workers.remove(worker)

    async def _send_worker_commands(self, worker: _WorkerHandle):
        with trio.fail_after(1):
            command_stream = await utils.open_tcp_stream_retry(
                worker.hostname, worker.command_port)
        async with command_stream:
            print("Connected to command stream")
            try:
                await utils.aevery(self._heartbeat_period,
                                   self._send_worker_heartbeat,
                                   command_stream)
            finally:
                with trio.move_on_after(1) as cleanup_scope:
                    cleanup_scope.shield = True
                    await command_stream.send_all(Command.Shutdown.to_bytes())

    async def _listen_for_new_workers(self, result_channel):
        handler = partial(self._register_worker, result_channel=result_channel)
        await trio.serve_tcp(handler, self._registration_port)

    async def _register_worker(self, worker_stream, result_channel):
        # FIXME: Error registering worker should not crash whole server
        async with worker_stream:
            # Receive registration details
            registration_msg = json.loads(await worker_stream.receive_some())

            if registration_msg["key"] != self._registration_key:
                await worker_stream.send_all(cpkl.dumps({"status": Status.BadKey}))

            else:
                worker_handle = _WorkerHandle.from_dict(registration_msg["handle"])
                worker_handle.hostname = worker_stream.socket.getpeername()[0]

                await worker_stream.send_all(cpkl.dumps({
                    "status": Status.Success,
                    "work_fn": A(),
                    "worker_hostname": worker_handle.hostname
                }))

                # Return worker handle to listener in order to spawn new worker
                # task
                await result_channel.send(worker_handle)

    async def _send_worker_heartbeat(self, command_stream):
        await command_stream.send_all(json.dumps({
            "command": Command.Heartbeat.to_b64().decode("ascii"),
            "data": [handle.to_dict() for handle in self._workers]
        }).encode())
        print("Sent heartbeat")

        with trio.fail_after(self._heartbeat_timeout):
            msg = await command_stream.receive_some()
            assert Status.from_bytes(msg) == Status.Success
            print("Received heartbeat")


def main(args):
    server = ClusterManager(args.registration_key,
                            args.registration_port,
                            args.heartbeat_period,
                            args.heartbeat_timeout)
    trio.run(server.listen)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("registration_key")
    parser.add_argument("--registration-port", type=int, default=C.DEFAULT_REGISTRATION_PORT)
    parser.add_argument("--heartbeat-period", type=int, default=C.DEFAULT_HEARTBEAT_PERIOD)
    parser.add_argument("--heartbeat-timeout", type=int, default=C.DEFAULT_HEARTBEAT_TIMEOUT)

    main(parser.parse_args())
