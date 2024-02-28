import time
from uuid import UUID

import msgpack
import cloudpickle as cpkl
import trio

from . import utils
from .message import Message, Status
from .client import ClientHandle


class A():
    def __call__(self):
        time.sleep(5)
        return 5


class Server:
    def __init__(self, registration_key: str, port: int):
        self._registration_key = registration_key
        self._port = port

        self._half_initialised = set()
        self._clients = dict()
        self._finished = trio.Event()

    async def listen(self) -> None:
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
    async def _client_task(self, client_stream: trio.SocketStream) -> None:
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

    async def _manage_client(
            self, client: ClientHandle, client_stream: trio.SocketStream) -> None:
        self._clients[client.uid] = client, client_stream
        try:
            await self._receive_client_messages(client, client_stream)
        finally:
            del self._clients[client.uid]
            with trio.move_on_after(1) as cleanup_scope:
                cleanup_scope.shield = True
                await Message.Shutdown.send(client_stream)

    async def _receive_client_messages(
            self, client: ClientHandle, client_stream: trio.SocketStream) -> None:
        print("Connected to message stream")
        async for msg in client_stream:
            print(msgpack.unpackb(msg))

    async def _register_client(
            self, client_stream: trio.SocketStream) -> ClientHandle:
        registration_msg = await Message.ConnectPing.expect(client_stream)

        if registration_msg["key"] != self._registration_key:
            await client_stream.send_all(cpkl.dumps({"status": Status.BadKey}))
            raise ValueError("Incorrect registration key")

        else:
            client = ClientHandle(
                hostname=utils.get_hostname(client_stream),
                uid=UUID(bytes=registration_msg["uid"]),
                port=registration_msg["port"])

            await client_stream.send_all(cpkl.dumps({
                "status": Status.Success,
                "work_fn": A(),
                "hostname": client.hostname
            }))
            return client


async def _send_peer(peer_stream: trio.SocketStream, client: ClientHandle) -> None:
    await Message.NewPeer.send(peer_stream, **client.to_dict())


async def _remove_peer(peer_stream: trio.SocketStream, client: ClientHandle) -> None:
    await Message.RemovePeer.send(peer_stream, uid=client.uid.bytes)
