from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable
from uuid import UUID

import cloudpickle as cpkl
import trio

from . import utils
from .message import Message, Status
from .client import ClientHandle


@dataclass(frozen=True, slots=True)
# TODO: unique name
class Client:
    handle: ClientHandle
    _stream: trio.SocketStream
    _lock: trio.Lock

    def __str__(self):
        return str(self.handle)

    def __repr__(self):
        return f"<Client({self.handle!r})>"

    async def send(self, tag: str, data: Any, pickle=False) -> None:
        if pickle:
            data = cpkl.dumps(data)
        await Message.ClientMessage.send(self._stream, tag=tag, data=data)


class Manager(ABC):
    @abstractmethod
    async def run(self, clients: Callable[[], list[Client]]):
        ...

    async def handle_client_message(self, client: Client, tag: str, data: Any):
        pass

    async def new_client(self, client):
        pass


class Server:
    def __init__(self, registration_key: str, port: int, manager: Manager):
        self._registration_key = registration_key
        self._port = port

        self._manager = manager

        self._clients = dict()

    async def listen(self) -> None:
        async with trio.open_nursery() as nursery:
            nursery.start_soon(self._manager.run, self._get_clients)
            listeners = await trio.open_tcp_listeners(self._port)
            for listener in listeners:
                utils.set_keepalive(listener.socket)
            await trio.serve_listeners(self._client_task, listeners)
        print("Server done")

    def _get_clients(self) -> list[Client]:
        return list(self._clients.values())

    @utils.noexcept("Client connection")
    async def _client_task(self, client_stream: trio.SocketStream) -> None:
        async with client_stream:
            handle = await self._register_client(client_stream)
            client = Client(
                handle=handle,
                _stream=client_stream,
                _lock=trio.Lock())

            async with trio.open_nursery() as nursery:
                # FIXME: Possible contention on these streams
                for peer in self._clients.values():
                    nursery.start_soon(_send_peer, peer, client)

            async with client_stream:
                await self._manage_client(client)

            async with trio.open_nursery() as nursery:
                # FIXME: Possible contention on these streams
                for peer in self._clients.values():
                    nursery.start_soon(_remove_peer, peer, client)

    async def _manage_client(self, client: Client) -> None:
        self._clients[client.handle.uid] = client
        try:
            await self._manager.new_client(client)
            await self._receive_client_messages(client)
        finally:
            del self._clients[client.handle.uid]
            with trio.move_on_after(1) as cleanup_scope:
                # TODO: Think about shielding across whole codebase
                cleanup_scope.shield = True
                await Message.Shutdown.send(client._stream)

    async def _receive_client_messages(self, client: Client) -> None:
        print("Connected to message stream")
        async for msg in client._stream:
            try:
                messagetype, payload = Message.from_bytes(msg)
                assert messagetype == Message.ClientMessage, messagetype
                tag = payload["tag"]
                data = payload["data"]
            except Exception as e:
                print("Unexpected message from client", type(e), *e.args)

            try:
                await self._manager.handle_client_message(client, tag, data)
            except Exception as e:
                # TODO: print bt
                print("User-code exception in handle_client_message:", type(e), *e.args)

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
                "hostname": client.hostname
            }))
            print("Registered!")
            return client


async def _send_peer(peer: Client, client: Client) -> None:
    await Message.NewPeer.send(peer._stream, **client.handle.to_dict())


async def _remove_peer(peer: Client, client: Client) -> None:
    await Message.RemovePeer.send(peer._stream, uid=client.handle.uid.bytes)
