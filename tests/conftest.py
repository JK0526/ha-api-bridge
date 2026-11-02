"""Shared mock Remote HA WebSocket server for integration tests."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

ENTITY_ID = "climate.office"
TOKEN = "test-token-never-log"


def climate_state() -> dict[str, Any]:
    """Return a representative Remote HA climate state."""
    return {
        "entity_id": ENTITY_ID,
        "state": "cool",
        "attributes": {
            "friendly_name": "Office AC",
            "current_temperature": 26.0,
            "temperature": 25.0,
            "target_temp_high": 27.0,
            "target_temp_low": 23.0,
            "hvac_modes": ["off", "cool", "heat", "fan_only"],
            "fan_mode": "auto",
            "fan_modes": ["auto", "low", "high"],
            "swing_mode": "off",
            "swing_modes": ["off", "vertical"],
            "preset_mode": "none",
            "preset_modes": ["none", "eco"],
            "supported_features": 1 | 2 | 8 | 16 | 32 | 128 | 256,
            "min_temp": 16.0,
            "max_temp": 30.0,
            "target_temp_step": 0.5,
            "temperature_unit": "°C",
        },
        "last_changed": "2026-10-29T00:00:00+00:00",
        "last_updated": "2026-10-29T00:00:01+00:00",
        "context": {"id": "test-context", "parent_id": None, "user_id": None},
    }


def compressed_state(state: dict[str, Any]) -> dict[str, Any]:
    """Encode the entity state as subscribe_entities does."""
    context = state["context"]
    if context.get("parent_id") is None and context.get("user_id") is None:
        compressed_context: str | dict[str, Any] = context["id"]
    else:
        compressed_context = context
    return {
        "s": state["state"],
        "a": state["attributes"],
        "c": compressed_context,
        "lc": datetime.fromisoformat(state["last_changed"]).timestamp(),
        "lu": datetime.fromisoformat(state["last_updated"]).timestamp(),
        "future_metadata": {"ignored": True},
    }


class MockRemoteHA:
    """State and helpers for the test WebSocket endpoint."""

    def __init__(self, server: TestServer) -> None:
        self.server = server
        self.states = [climate_state()]
        self.requests: list[dict[str, Any]] = []
        self.auth_messages: list[dict[str, Any]] = []
        self.sockets: list[web.WebSocketResponse] = []
        self.subscription_ids: list[int] = []
        self.unsubscriptions: list[int] = []
        self.subscriptions_by_socket: dict[int, int] = {}
        self.ignore_service_response = False
        self.fail_service_response = False
        self.ignore_subscribe_response = False
        self.ignore_unsubscribe_response = False
        self.ignore_get_states = False
        self.unit_system_temperature: str | None = "°C"
        self.invalid_get_states = False
        self.delay_connect = False
        self.connect_started = asyncio.Event()
        self.release_connect = asyncio.Event()
        self.delay_auth_response = False
        self.reject_auth = False
        self.close_after_subscribe_count: int | None = None

    @property
    def url(self) -> str:
        """Return the HTTP base URL."""
        return str(self.server.make_url("/")).rstrip("/")

    @property
    def current_socket(self) -> web.WebSocketResponse:
        """Return the latest active socket."""
        return next(socket for socket in reversed(self.sockets) if not socket.closed)

    @property
    def active_sockets(self) -> list[web.WebSocketResponse]:
        """Return open test server sockets."""
        return [socket for socket in self.sockets if not socket.closed]

    @property
    def active_subscription_count(self) -> int:
        """Return subscriptions attached to currently open server sockets."""
        active_socket_ids = {id(socket) for socket in self.active_sockets}
        return sum(
            socket_id in active_socket_ids for socket_id in self.subscriptions_by_socket
        )

    async def send(self, message: dict[str, Any]) -> None:
        """Send a server event to the latest active socket."""
        await self.current_socket.send_json(message)


@pytest.fixture
async def remote_ha(
    aiohttp_server: Any, socket_enabled: None
) -> AsyncIterator[MockRemoteHA]:
    """Run a local mock WebSocket server implementing just the HA protocol."""
    remote: MockRemoteHA | None = None

    async def websocket_handler(request: web.Request) -> web.WebSocketResponse:
        nonlocal remote
        assert remote is not None
        if remote.delay_connect:
            remote.connect_started.set()
            await remote.release_connect.wait()
            return web.Response(status=503)
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        remote.sockets.append(ws)
        await ws.send_json({"type": "auth_required", "ha_version": "2026.10.0"})
        auth = await ws.receive_json()
        remote.auth_messages.append(auth)
        if remote.reject_auth or auth.get("access_token") != TOKEN:
            await ws.send_json({"type": "auth_invalid"})
            await ws.close()
            return ws
        if remote.delay_auth_response:
            async for _ in ws:
                pass
            return ws
        await ws.send_json({"type": "auth_ok", "ha_version": "2026.10.0"})

        async for message in ws:
            if message.type != web.WSMsgType.TEXT:
                continue
            payload = message.json()
            remote.requests.append(payload)
            command = payload["type"]
            if command == "get_config":
                await ws.send_json(
                    {
                        "id": payload["id"],
                        "type": "result",
                        "success": True,
                        "result": {
                            "unit_system": {
                                "temperature": remote.unit_system_temperature
                            }
                        },
                    }
                )
            elif command == "get_states":
                if remote.ignore_get_states:
                    continue
                await ws.send_json(
                    {
                        "id": payload["id"],
                        "type": "result",
                        "success": True,
                        "result": None if remote.invalid_get_states else remote.states,
                    }
                )
            elif command == "subscribe_entities":
                remote.subscription_ids.append(payload["id"])
                remote.subscriptions_by_socket[id(ws)] = payload["id"]
                if remote.ignore_subscribe_response:
                    continue
                await ws.send_json(
                    {
                        "id": payload["id"],
                        "type": "result",
                        "success": True,
                        "result": None,
                    }
                )
                if (
                    remote.close_after_subscribe_count is not None
                    and len(remote.subscription_ids)
                    >= remote.close_after_subscribe_count
                ):
                    await ws.close()
                    return ws
                await ws.send_json(
                    {
                        "id": payload["id"],
                        "type": "event",
                        "event": {
                            "a": {
                                state["entity_id"]: compressed_state(state)
                                for state in remote.states
                                if state["entity_id"] in payload["entity_ids"]
                            }
                        },
                    }
                )
            elif command == "unsubscribe_events":
                remote.unsubscriptions.append(payload["subscription"])
                if (
                    remote.subscriptions_by_socket.get(id(ws))
                    == payload["subscription"]
                ):
                    remote.subscriptions_by_socket.pop(id(ws), None)
                if remote.ignore_unsubscribe_response:
                    continue
                await ws.send_json(
                    {
                        "id": payload["id"],
                        "type": "result",
                        "success": True,
                        "result": None,
                    }
                )
            elif command == "call_service":
                if remote.ignore_service_response:
                    continue
                response = {
                    "id": payload["id"],
                    "type": "result",
                    "success": not remote.fail_service_response,
                    "result": None,
                }
                if remote.fail_service_response:
                    response["error"] = {"message": "Rejected by mock Remote HA"}
                await ws.send_json(response)
        return ws

    app = web.Application()
    app.router.add_get("/api/websocket", websocket_handler)
    server = await aiohttp_server(app)
    remote = MockRemoteHA(server)
    yield remote
    for socket in remote.sockets:
        if not socket.closed:
            await socket.close()
