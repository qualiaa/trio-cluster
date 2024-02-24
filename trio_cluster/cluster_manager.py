import time
from uuid import UUID

import msgpack
import cloudpickle as cpkl
import trio

from . import utils
from .constants import Command, Status
from .cluster_worker import _Client


class A():
    def __call__(self):
        time.sleep(5)
        return 5


class ClusterManager:
    def __init__(self, registration_key, port):
        self._registration_key = registration_key
        self._port = port

        self._half_initialised = set()
        self._clients = dict()
        self._finished = trio.Event()

    async def listen(self):
        async with trio.open_nursery() as nursery:
            def print_status():
                print("I'm alive!")
                for i, (client, _) in enumerate(self._clients.values()):
                    print(f"\t{i}: {client}")
                print()
            nursery.start_soon(utils.every, 2, print_status)
            listeners = await trio.open_tcp_listeners(self._port)
            for listener in listeners:
                utils.set_keepalive(listener.socket)
            await trio.serve_listeners(self._client_task, listeners)
        print("Server done")

    @utils.noexcept("Client connection")
    async def _client_task(self, client_stream):
        async with client_stream:
            client = await self._register_client(client_stream)

            async with trio.open_nursery() as nursery:
                # FIXME: Possible contention on these streams
                for peer, peer_stream in self._clients.values():
                    nursery.start_soon(_send_peer, peer_stream, client)

            await self._manage_client(client, client_stream)

            async with trio.open_nursery() as nursery:
                # FIXME: Possible contention on these streams
                for peer, peer_stream in self._clients.values():
                    nursery.start_soon(_remove_peer, peer_stream, client)

    async def _manage_client(self, client, client_stream):
        self._clients[client.uid] = client, client_stream
        try:
            await self._receive_client_commands(client, client_stream)
        finally:
            del self._clients[client.uid]
            with trio.move_on_after(1) as cleanup_scope:
                cleanup_scope.shield = True
                await _send_command(client_stream, Command.Shutdown)

    async def _receive_client_commands(self, client, client_stream):
        print("Connected to command stream")
        async for msg in client_stream:
            print(msgpack.unpackb(msg))

    async def _register_client(self, client_stream):
        registration_msg = await _receive_message(client_stream)

        if registration_msg["key"] != self._registration_key:
            await client_stream.send_all(cpkl.dumps({"status": Status.BadKey}))

        else:
            client = _Client(
                hostname=utils.get_hostname(client_stream),
                uid=UUID(bytes=registration_msg["uid"]),
                port=registration_msg["port"])

            await client_stream.send_all(cpkl.dumps({
                "status": Status.Success,
                "work_fn": A(),
                "hostname": client.hostname
            }))
            return client


async def _send_command(stream, command, **kargs):
    message = {"command": command.to_b64().decode("ascii")}
    if kargs:
        message["payload"] = kargs
    await stream.send_all(msgpack.packb(message))


async def _receive_message(stream):
    return msgpack.unpackb(await stream.receive_some())


async def _send_peer(peer_stream, client):
    await _send_command(peer_stream, Command.NewPeer, **client.to_dict())


async def _remove_peer(peer_stream, client):
    await _send_command(peer_stream, Command.RemovePeer, uid=client.uid.bytes)
