"""Tests for the single-connection WebSocket client."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from custom_components.ha_api_bridge import client as client_module
from custom_components.ha_api_bridge.client import (
    HAWebSocketClient,
    RemoteAuthError,
    RemoteRequestTimeout,
    websocket_url,
)
from custom_components.ha_api_bridge.climate import RemoteClimateEntity
from tests.conftest import ENTITY_ID, TOKEN, MockRemoteHA

pytestmark = pytest.mark.enable_socket


def test_websocket_url_conversion() -> None:
    assert websocket_url("http://ha.local:8123/") == "ws://ha.local:8123/api/websocket"
    assert (
        websocket_url("https://ha.example/home/")
        == "wss://ha.example/home/api/websocket"
    )
    assert (
        websocket_url("http://ha.local/api/websocket") == "ws://ha.local/api/websocket"
    )
    with pytest.raises(ValueError):
        websocket_url("ftp://ha.local")


async def test_auth_subscribe_delta_service_and_shutdown(
    hass: Any, remote_ha: MockRemoteHA
) -> None:
    client = HAWebSocketClient(hass, remote_ha.url, TOKEN, [ENTITY_ID])
    await client.async_start()
    try:
        async with asyncio.timeout(1):
            while not client.synced:
                await asyncio.sleep(0.01)
        assert client.ha_version == "2026.10.0"
        assert client.states[ENTITY_ID]["state"] == "cool"
        assert remote_ha.auth_messages == [{"type": "auth", "access_token": TOKEN}]
        assert len(remote_ha.sockets) == 1
        assert len(remote_ha.subscription_ids) == 1
        subscribe = next(
            item for item in remote_ha.requests if item["type"] == "subscribe_entities"
        )
        assert subscribe["entity_ids"] == [ENTITY_ID]

        before = client.states[ENTITY_ID]["attributes"]["temperature"]
        await client.async_call_service(
            ENTITY_ID, "set_temperature", {"temperature": 24}
        )
        call = next(
            item for item in remote_ha.requests if item["type"] == "call_service"
        )
        assert call["domain"] == "climate"
        assert call["service"] == "set_temperature"
        assert call["service_data"] == {"temperature": 24}
        assert client.states[ENTITY_ID]["attributes"]["temperature"] == before

        await remote_ha.send(
            {
                "id": remote_ha.subscription_ids[0],
                "type": "event",
                "event": {
                    "c": {
                        ENTITY_ID: {
                            "a": {
                                "+": {"temperature": 24, "fan_mode": "low"},
                                "-": ["preset_mode"],
                            }
                        }
                    }
                },
            }
        )
        async with asyncio.timeout(1):
            while client.states[ENTITY_ID]["attributes"].get("temperature") != 24:
                await asyncio.sleep(0.01)
        assert client.states[ENTITY_ID]["attributes"]["temperature"] == 24
        assert client.states[ENTITY_ID]["attributes"]["fan_mode"] == "low"
        assert "preset_mode" not in client.states[ENTITY_ID]["attributes"]
    finally:
        await client.async_close()

    assert remote_ha.unsubscriptions == remote_ha.subscription_ids
    assert not client.connected


async def test_invalid_auth(hass: Any, remote_ha: MockRemoteHA) -> None:
    remote_ha.reject_auth = True
    client = HAWebSocketClient(hass, remote_ha.url, TOKEN, [ENTITY_ID])
    with pytest.raises(RemoteAuthError):
        await client.async_connect()
    await client.async_close()
    assert remote_ha.auth_messages[0]["access_token"] == TOKEN


async def test_request_timeout_does_not_disconnect(
    hass: Any, remote_ha: MockRemoteHA, monkeypatch: pytest.MonkeyPatch
) -> None:
    remote_ha.ignore_service_response = True
    monkeypatch.setattr(client_module, "REQUEST_TIMEOUT", 0.02)
    client = HAWebSocketClient(hass, remote_ha.url, TOKEN, [ENTITY_ID])
    await client.async_start()
    try:
        with pytest.raises(RemoteRequestTimeout):
            await client.async_call_service(ENTITY_ID, "turn_off")
        assert client.connected
    finally:
        await client.async_close()


async def test_reconnect_resubscribes_after_disconnect(
    hass: Any, remote_ha: MockRemoteHA, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(client_module, "RECONNECT_DELAYS", (0.01,))
    client = HAWebSocketClient(hass, remote_ha.url, TOKEN, [ENTITY_ID])
    await client.async_start()
    entity = RemoteClimateEntity(client, "a-stable-entry-uuid", ENTITY_ID)
    try:
        async with asyncio.timeout(1):
            while not entity.available:
                await asyncio.sleep(0.01)
        await remote_ha.current_socket.close()
        async with asyncio.timeout(2):
            while entity.available:
                await asyncio.sleep(0.01)
        assert not client.connected
        async with asyncio.timeout(2):
            while len(remote_ha.subscription_ids) < 2 or not entity.available:
                await asyncio.sleep(0.01)
        assert len(remote_ha.sockets) == 2
        assert len(remote_ha.subscription_ids) == 2
        assert client.states[ENTITY_ID]["state"] == "cool"
    finally:
        entity._remove_listener()
        await client.async_close()
