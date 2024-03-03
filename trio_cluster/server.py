from abc import ABC, abstractmethod
from dataclasses import dataclass
from logging import getLogger
from typing import Any, Callable
from uuid import UUID

import cloudpickle as cpkl
import trio

from . import utils
from ._client_handle import ClientHandle
from .message import Message, Status

_LOG = getLogger(__name__)


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
            _LOG.info("Listening for clients")
            await trio.serve_listeners(self._client_connection, listeners)
        _LOG.info("Server closing gracefully")

    def _get_clients(self) -> list[Client]:
        return list(self._clients.values())

    @utils.noexcept(log=_LOG)
    async def _client_connection(self, client_stream: trio.SocketStream) -> None:
        _LOG.info("Received connection")
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

            self._clients[client.handle.uid] = client
            try:
                async with client_stream:
                    await self._manage_client(client)
            finally:
                # FIXME: If parent nursery has been cancelled, what happens?
                del self._clients[client.handle.uid]
                with trio.move_on_after(1) as cleanup_scope:
                    # TODO: Think about shielding across whole codebase
                    cleanup_scope.shield = True
                    async with trio.open_nursery() as nursery:
                        # TODO: Make noexcept work when passed a function directly
                        nursery.start_soon(utils.noexcept()(Message.Shutdown.send),
                                           client._stream)
                        # FIXME: Possible contention on these streams
                        for peer in self._clients.values():
                            nursery.start_soon(utils.noexcept()(_remove_peer),
                                               peer, client)

    async def _manage_client(self, client: Client) -> None:
        _LOG.info("Connected to new client")
        try:
            _LOG.debug("Calling _manager.new_client")
            await self._manager.new_client(client)
        except Exception as e:
            _LOG.exception("Exception in user code")
            raise UserError from e

        async for msg in client._stream:
            try:
                messagetype, payload = Message.from_bytes(msg)
                assert messagetype == Message.ClientMessage, messagetype
                tag = payload["tag"]
                data = payload["data"]
            except Exception as e:
                _LOG.exception("Unexpected message from client")

            try:
                await self._manager.handle_client_message(client, tag, data)
            except Exception as e:
                _LOG.exception("User-code exception in handle_client_message")

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
            _LOG.debug("Registered!")
            return client


async def _send_peer(peer: Client, client: Client) -> None:
    await Message.NewPeer.send(peer._stream, **client.handle.to_dict())


async def _remove_peer(peer: Client, client: Client) -> None:
    await Message.RemovePeer.send(peer._stream, uid=client.handle.uid.bytes)
