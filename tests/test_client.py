"""Tests for the single-connection WebSocket client."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from custom_components.ha_api_bridge import client as client_module
from custom_components.ha_api_bridge.client import (
    HAWebSocketClient,
    RemoteAuthError,
    RemoteCommandError,
    RemoteConnectionError,
    RemoteRequestTimeout,
    normalized_remote_url,
    websocket_url,
)
from custom_components.ha_api_bridge.climate import RemoteClimateEntity
from tests.conftest import ENTITY_ID, TOKEN, MockRemoteHA

pytestmark = pytest.mark.enable_socket


def test_websocket_url_conversion() -> None:
    assert websocket_url("http://ha.local:8123/") == "ws://ha.local:8123/api/websocket"
    assert (
        websocket_url("192.168.1.118:8123") == "ws://192.168.1.118:8123/api/websocket"
    )
    assert (
        websocket_url("https://ha.example/home/")
        == "wss://ha.example/home/api/websocket"
    )
    with pytest.raises(ValueError):
        websocket_url("http://ha.local/api/websocket")
    with pytest.raises(ValueError):
        websocket_url("ftp://ha.local")


def test_remote_url_normalization() -> None:
    assert normalized_remote_url("192.168.1.118:8123") == "http://192.168.1.118:8123"
    assert normalized_remote_url("HomeAssistant.Local:8123") == (
        "http://homeassistant.local:8123"
    )
    assert (
        normalized_remote_url("HTTP://HOMEASSISTANT.LOCAL:8123/")
        == "http://homeassistant.local:8123"
    )
    with pytest.raises(ValueError):
        normalized_remote_url("http://homeassistant.local:8123/api/websocket/")
    assert normalized_remote_url("HTTP://HA.LOCAL:80/") == "http://ha.local"
    assert normalized_remote_url("HTTPS://HA.LOCAL:443") == "https://ha.local"
    assert normalized_remote_url("https://ha.local:8443/") == "https://ha.local:8443"


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
        assert client.states[ENTITY_ID]["context"] == "test-context"
        assert client.states[ENTITY_ID]["last_changed"]
        assert client.states[ENTITY_ID]["last_updated"]

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
                            "+": {
                                "a": {"temperature": 24, "fan_mode": "low"},
                            },
                            "-": {
                                "a": ["preset_mode"],
                            },
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
        assert client.connected
    finally:
        await client.async_close()


async def test_ha_2026_10_subscribe_entities_payloads_merge_and_notify(
    hass: Any, remote_ha: MockRemoteHA
) -> None:
    """Merge the wire shape emitted by HA 2026.10.3 and notify listeners."""
    client = HAWebSocketClient(hass, remote_ha.url, TOKEN, [ENTITY_ID])
    notifications: list[dict[str, Any]] = []
    remove_listener = client.add_listener(
        lambda: notifications.append(
            {
                "state": client.states.get(ENTITY_ID, {}).get("state"),
                "attributes": dict(
                    client.states.get(ENTITY_ID, {}).get("attributes", {})
                ),
            }
        )
    )
    await client.async_start()
    try:
        async with asyncio.timeout(1):
            while not client.synced:
                await asyncio.sleep(0.01)
        initial_notifications = len(notifications)
        snapshot = client.states[ENTITY_ID]
        assert snapshot["state"] == "cool"
        assert snapshot["attributes"]["temperature"] == 25.0
        assert snapshot["context"] == "test-context"
        assert snapshot["last_changed"]
        assert snapshot["last_updated"]

        subscription_id = remote_ha.subscription_ids[0]
        await remote_ha.send(
            {
                "id": subscription_id,
                "type": "event",
                "event": {
                    "c": {
                        ENTITY_ID: {
                            "+": {"a": {"temperature": 24}},
                        }
                    }
                },
            }
        )
        async with asyncio.timeout(1):
            while client.states[ENTITY_ID]["attributes"]["temperature"] != 24:
                await asyncio.sleep(0.01)
        assert len(notifications) == initial_notifications + 1

        await remote_ha.send(
            {
                "id": subscription_id,
                "type": "event",
                "event": {
                    "c": {
                        ENTITY_ID: {
                            "+": {
                                "s": "heat",
                                "c": {"id": "changed-context"},
                                "lc": 1793232001.0,
                            }
                        }
                    }
                },
            }
        )
        async with asyncio.timeout(1):
            while client.states[ENTITY_ID]["state"] != "heat":
                await asyncio.sleep(0.01)
        assert client.states[ENTITY_ID]["context"] == {
            "id": "changed-context",
        }
        assert notifications[-1]["state"] == "heat"

        await remote_ha.send(
            {
                "id": subscription_id,
                "type": "event",
                "event": {
                    "c": {
                        ENTITY_ID: {
                            "+": {
                                "a": {
                                    "fan_mode": "low",
                                    "swing_mode": "vertical",
                                    "current_temperature": 25.5,
                                }
                            },
                            "-": {"a": ["preset_mode"]},
                        }
                    }
                },
            }
        )
        async with asyncio.timeout(1):
            while client.states[ENTITY_ID]["attributes"].get("fan_mode") != "low":
                await asyncio.sleep(0.01)
        attributes = client.states[ENTITY_ID]["attributes"]
        assert attributes["swing_mode"] == "vertical"
        assert attributes["current_temperature"] == 25.5
        assert "preset_mode" not in attributes
        assert notifications[-1]["attributes"]["current_temperature"] == 25.5
    finally:
        remove_listener()
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
        assert len(remote_ha.active_sockets) == 1
        assert remote_ha.active_subscription_count == 1
        assert client._reader_task is not None and not client._reader_task.done()
        assert client.states[ENTITY_ID]["state"] == "cool"
    finally:
        entity._remove_listener()
        await client.async_close()


async def test_pending_service_fails_on_disconnect(
    hass: Any, remote_ha: MockRemoteHA
) -> None:
    remote_ha.ignore_service_response = True
    client = HAWebSocketClient(hass, remote_ha.url, TOKEN, [ENTITY_ID])
    await client.async_start()
    request = asyncio.create_task(client.async_call_service(ENTITY_ID, "turn_off"))
    try:
        async with asyncio.timeout(1):
            while not any(
                item["type"] == "call_service" for item in remote_ha.requests
            ):
                await asyncio.sleep(0.01)
        await remote_ha.current_socket.close()
        with pytest.raises(RemoteConnectionError):
            await request
        assert client._pending == {}
    finally:
        if not request.done():
            request.cancel()
            await asyncio.gather(request, return_exceptions=True)
        await client.async_close()


async def test_cancelled_service_ignores_late_result(
    hass: Any, remote_ha: MockRemoteHA
) -> None:
    remote_ha.ignore_service_response = True
    client = HAWebSocketClient(hass, remote_ha.url, TOKEN, [ENTITY_ID])
    await client.async_start()
    request = asyncio.create_task(client.async_call_service(ENTITY_ID, "turn_on"))
    try:
        async with asyncio.timeout(1):
            while not any(
                item["type"] == "call_service" for item in remote_ha.requests
            ):
                await asyncio.sleep(0.01)
        payload = next(
            item for item in remote_ha.requests if item["type"] == "call_service"
        )
        request.cancel()
        with pytest.raises(asyncio.CancelledError):
            await request
        assert client._pending == {}
        await remote_ha.send(
            {"id": payload["id"], "type": "result", "success": True, "result": None}
        )
        assert client.connected
        assert client._pending == {}
    finally:
        await client.async_close()


async def test_unload_clears_pending_service_and_socket(
    hass: Any, remote_ha: MockRemoteHA
) -> None:
    remote_ha.ignore_service_response = True
    client = HAWebSocketClient(hass, remote_ha.url, TOKEN, [ENTITY_ID])
    await client.async_start()
    request = asyncio.create_task(client.async_call_service(ENTITY_ID, "turn_off"))
    try:
        async with asyncio.timeout(1):
            while not any(
                item["type"] == "call_service" for item in remote_ha.requests
            ):
                await asyncio.sleep(0.01)
        await asyncio.gather(client.async_close(), client.async_close())
        with pytest.raises(RemoteConnectionError):
            await request
        assert client._pending == {}
        assert client._ws is None
        assert client._reader_task is None
        assert client._reconnect_task is None
        assert remote_ha.active_sockets == []
        await client._disconnect()
        await client._disconnect()
    finally:
        if not request.done():
            request.cancel()
            await asyncio.gather(request, return_exceptions=True)
        await client.async_close()


async def test_unload_cancellation_still_disconnects(
    hass: Any, remote_ha: MockRemoteHA
) -> None:
    remote_ha.ignore_unsubscribe_response = True
    client = HAWebSocketClient(hass, remote_ha.url, TOKEN, [ENTITY_ID])
    await client.async_start()
    close_task = asyncio.create_task(client.async_close())
    try:
        async with asyncio.timeout(1):
            while not remote_ha.unsubscriptions:
                await asyncio.sleep(0.01)
        close_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await close_task
        assert client._pending == {}
        assert client._ws is None
        assert client._reader_task is None
        assert client._reconnect_task is None
        assert remote_ha.active_sockets == []
    finally:
        if not close_task.done():
            close_task.cancel()
            await asyncio.gather(close_task, return_exceptions=True)
        await client.async_close()


async def test_remote_service_failure_is_a_home_assistant_error(
    hass: Any, remote_ha: MockRemoteHA
) -> None:
    remote_ha.fail_service_response = True
    client = HAWebSocketClient(hass, remote_ha.url, TOKEN, [ENTITY_ID])
    await client.async_start()
    try:
        with pytest.raises(RemoteCommandError, match="rejected a command"):
            await client.async_call_service(ENTITY_ID, "turn_off")
        assert client._pending == {}
    finally:
        await client.async_close()


@pytest.mark.parametrize("stage", ["connect", "authentication", "subscribe"])
async def test_setup_cancellation_cleans_transport(
    hass: Any, remote_ha: MockRemoteHA, stage: str
) -> None:
    client = HAWebSocketClient(hass, remote_ha.url, TOKEN, [ENTITY_ID])
    if stage == "connect":
        remote_ha.delay_connect = True
    elif stage == "authentication":
        remote_ha.delay_auth_response = True
    else:
        remote_ha.ignore_subscribe_response = True

    start_task = asyncio.create_task(client.async_start())
    try:
        async with asyncio.timeout(1):
            if stage == "connect":
                await remote_ha.connect_started.wait()
            elif stage == "authentication":
                while not remote_ha.auth_messages:
                    await asyncio.sleep(0.01)
            else:
                while not any(
                    item["type"] == "subscribe_entities" for item in remote_ha.requests
                ):
                    await asyncio.sleep(0.01)
        start_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await start_task
        remote_ha.release_connect.set()
        assert client._pending == {}
        assert client._ws is None
        assert client._reader_task is None
        assert client._reconnect_task is None
        async with asyncio.timeout(1):
            while remote_ha.active_sockets:
                await asyncio.sleep(0.01)
    finally:
        remote_ha.release_connect.set()
        if not start_task.done():
            start_task.cancel()
            await asyncio.gather(start_task, return_exceptions=True)
        await client.async_close()


async def test_unload_during_reconnect_delay_prevents_reconnect(
    hass: Any, remote_ha: MockRemoteHA, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(client_module, "RECONNECT_DELAYS", (0.2,))
    client = HAWebSocketClient(hass, remote_ha.url, TOKEN, [ENTITY_ID])
    await client.async_start()
    try:
        await remote_ha.current_socket.close()
        async with asyncio.timeout(1):
            while client.connected:
                await asyncio.sleep(0.01)
        await asyncio.sleep(0.02)
        await client.async_close()
        await asyncio.sleep(0.25)
        assert len(remote_ha.sockets) == 1
        assert remote_ha.active_sockets == []
        assert client._reader_task is None
        assert client._reconnect_task is None
    finally:
        await client.async_close()


async def test_reconnect_failures_keep_single_reader_socket_and_subscription(
    hass: Any, remote_ha: MockRemoteHA, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(client_module, "RECONNECT_DELAYS", (0.01,))
    remote_ha.close_after_subscribe_count = 2
    client = HAWebSocketClient(hass, remote_ha.url, TOKEN, [ENTITY_ID])
    active_readers = 0
    max_readers = 0
    original_reader = client._reader

    async def track_reader(ws: Any) -> None:
        nonlocal active_readers, max_readers
        active_readers += 1
        max_readers = max(max_readers, active_readers)
        try:
            await original_reader(ws)
        finally:
            active_readers -= 1

    client._reader = track_reader  # type: ignore[method-assign]
    await client.async_start()
    reconnect_task = client._reconnect_task
    max_sockets = len(remote_ha.active_sockets)
    max_subscriptions = remote_ha.active_subscription_count
    try:
        await remote_ha.current_socket.close()
        async with asyncio.timeout(2):
            while len(remote_ha.subscription_ids) < 4:
                max_sockets = max(max_sockets, len(remote_ha.active_sockets))
                max_subscriptions = max(
                    max_subscriptions, remote_ha.active_subscription_count
                )
                await asyncio.sleep(0.005)
        assert client._reconnect_task is reconnect_task
        assert not reconnect_task.done()
        assert max_readers == 1
        assert max_sockets == 1
        assert max_subscriptions == 1
    finally:
        await client.async_close()


async def test_start_is_not_duplicated(hass: Any, remote_ha: MockRemoteHA) -> None:
    client = HAWebSocketClient(hass, remote_ha.url, TOKEN, [ENTITY_ID])
    await client.async_start()
    reader_task = client._reader_task
    reconnect_task = client._reconnect_task
    try:
        with pytest.raises(RemoteConnectionError, match="already started"):
            await client.async_start()
        assert len(remote_ha.sockets) == 1
        assert len(remote_ha.subscription_ids) == 1
        assert client._reader_task is reader_task
        assert client._reconnect_task is reconnect_task
    finally:
        await client.async_close()
