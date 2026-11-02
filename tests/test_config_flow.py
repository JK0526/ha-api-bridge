"""Tests for connection validation and entity selection."""

from __future__ import annotations

import asyncio
from typing import Any
from urllib.parse import urlsplit

import pytest
from homeassistant.config_entries import SOURCE_USER

from custom_components.ha_api_bridge import client as client_module
from custom_components.ha_api_bridge import config_flow as config_flow_module
from custom_components.ha_api_bridge.client import HAWebSocketClient
from custom_components.ha_api_bridge.const import (
    CONF_ENTITY_IDS,
    CONF_INSTANCE_ID,
    CONF_TOKEN,
    CONF_URL,
    DOMAIN,
)
from tests.conftest import ENTITY_ID, TOKEN, MockRemoteHA

pytestmark = pytest.mark.enable_socket


async def test_config_flow_auth_and_create_entry(
    hass: Any,
    remote_ha: MockRemoteHA,
    enable_custom_integrations: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parsed_url = urlsplit(remote_ha.url)
    url_without_scheme = parsed_url.netloc
    result = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": SOURCE_USER},
        data={CONF_URL: url_without_scheme, CONF_TOKEN: TOKEN},
    )
    assert result["type"] == "form"
    assert result["step_id"] == "select_entities"
    assert len(remote_ha.sockets) == 1
    assert remote_ha.sockets[0].closed
    assert [item["type"] for item in remote_ha.requests] == ["get_states"]

    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {CONF_ENTITY_IDS: [ENTITY_ID]}
    )
    assert result["type"] == "create_entry"
    entry = result["result"]
    assert entry.unique_id == remote_ha.url
    assert entry.data[CONF_URL] == remote_ha.url
    assert entry.data[CONF_TOKEN] == TOKEN
    assert entry.data[CONF_ENTITY_IDS] == [ENTITY_ID]
    assert "remote_version" not in entry.data
    assert entry.data[CONF_INSTANCE_ID]
    first_client = entry.runtime_data
    assert set(first_client._listeners) == {ENTITY_ID}
    original_first_close = first_client.async_close

    async def verify_first_entity_removed() -> None:
        assert first_client._listeners == {}
        await original_first_close()

    monkeypatch.setattr(first_client, "async_close", verify_first_entity_removed)
    assert await hass.config_entries.async_unload(entry.entry_id)
    assert not first_client.connected
    assert first_client._reader_task is None
    assert first_client._reconnect_task is None
    assert first_client._listeners == {}
    assert len(remote_ha.sockets) == 2
    assert len(remote_ha.subscription_ids) == 1

    duplicate_url = f"HTTP://{parsed_url.netloc.upper()}/"
    sockets_before_duplicate = len(remote_ha.sockets)
    duplicate = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": SOURCE_USER},
        data={CONF_URL: duplicate_url, CONF_TOKEN: TOKEN},
    )
    assert duplicate["type"] == "abort"
    assert duplicate["reason"] == "already_configured"
    assert len(remote_ha.sockets) == sockets_before_duplicate

    assert await hass.config_entries.async_setup(entry.entry_id)
    second_client = entry.runtime_data
    assert second_client is not first_client
    assert second_client.connected
    assert len(remote_ha.sockets) == 3
    assert len(remote_ha.subscription_ids) == 2
    assert remote_ha.unsubscriptions == remote_ha.subscription_ids[:1]
    assert set(second_client._listeners) == {ENTITY_ID}
    assert len(second_client._listeners[ENTITY_ID]) == 1
    original_second_close = second_client.async_close

    async def verify_second_entity_removed() -> None:
        assert second_client._listeners == {}
        await original_second_close()

    monkeypatch.setattr(second_client, "async_close", verify_second_entity_removed)
    assert await hass.config_entries.async_unload(entry.entry_id)
    assert second_client._listeners == {}
    assert remote_ha.unsubscriptions == remote_ha.subscription_ids


async def test_config_flow_reports_invalid_token(
    hass: Any, remote_ha: MockRemoteHA, enable_custom_integrations: None
) -> None:
    remote_ha.reject_auth = True
    result = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": SOURCE_USER},
        data={CONF_URL: remote_ha.url, CONF_TOKEN: "invalid"},
    )
    assert result["type"] == "form"
    assert result["step_id"] == "user"
    assert result["errors"] == {"base": "invalid_auth"}
    assert remote_ha.sockets[0].closed


async def test_config_flow_reports_invalid_url(
    hass: Any, enable_custom_integrations: None
) -> None:
    result = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": SOURCE_USER},
        data={CONF_URL: "ftp://remote-ha.invalid", CONF_TOKEN: TOKEN},
    )
    assert result["type"] == "form"
    assert result["step_id"] == "user"
    assert result["errors"] == {"base": "cannot_connect"}

    endpoint = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": SOURCE_USER},
        data={CONF_URL: "http://remote-ha.invalid/api/websocket", CONF_TOKEN: TOKEN},
    )
    assert endpoint["type"] == "form"
    assert endpoint["step_id"] == "user"
    assert endpoint["errors"] == {"base": "cannot_connect"}


async def test_config_flow_connect_timeout_closes_temporary_client(
    hass: Any,
    remote_ha: MockRemoteHA,
    enable_custom_integrations: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    remote_ha.delay_connect = True
    remote_ha.release_connect.clear()
    monkeypatch.setattr(client_module, "CONNECT_TIMEOUT", 0.02)
    flow_task = asyncio.create_task(
        hass.config_entries.flow.async_init(
            DOMAIN,
            context={"source": SOURCE_USER},
            data={CONF_URL: remote_ha.url, CONF_TOKEN: TOKEN},
        )
    )
    try:
        async with asyncio.timeout(1):
            await remote_ha.connect_started.wait()
        result = await flow_task
        assert result["type"] == "form"
        assert result["errors"] == {"base": "cannot_connect"}
        assert remote_ha.sockets == []
    finally:
        remote_ha.release_connect.set()
        if not flow_task.done():
            flow_task.cancel()
            await asyncio.gather(flow_task, return_exceptions=True)


async def test_config_flow_unexpected_exception_closes_temporary_client(
    hass: Any,
    remote_ha: MockRemoteHA,
    enable_custom_integrations: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clients: list[HAWebSocketClient] = []
    original_factory = config_flow_module.HAWebSocketClient

    def capture_client(*args: Any, **kwargs: Any) -> HAWebSocketClient:
        client = original_factory(*args, **kwargs)
        clients.append(client)
        return client

    async def fail_get_states(self: HAWebSocketClient) -> list[dict[str, Any]]:
        raise RuntimeError("unexpected config flow failure")

    monkeypatch.setattr(config_flow_module, "HAWebSocketClient", capture_client)
    monkeypatch.setattr(HAWebSocketClient, "async_get_states", fail_get_states)
    with pytest.raises(RuntimeError, match="unexpected config flow failure"):
        await hass.config_entries.flow.async_init(
            DOMAIN,
            context={"source": SOURCE_USER},
            data={CONF_URL: remote_ha.url, CONF_TOKEN: TOKEN},
        )
    assert len(clients) == 1
    assert clients[0]._ws is None
    assert clients[0]._pending == {}
    assert remote_ha.sockets[0].closed


async def test_config_flow_cancellation_closes_temporary_client(
    hass: Any,
    remote_ha: MockRemoteHA,
    enable_custom_integrations: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    remote_ha.ignore_get_states = True
    clients: list[HAWebSocketClient] = []
    original_factory = config_flow_module.HAWebSocketClient

    def capture_client(*args: Any, **kwargs: Any) -> HAWebSocketClient:
        client = original_factory(*args, **kwargs)
        clients.append(client)
        return client

    monkeypatch.setattr(config_flow_module, "HAWebSocketClient", capture_client)
    flow_task = asyncio.create_task(
        hass.config_entries.flow.async_init(
            DOMAIN,
            context={"source": SOURCE_USER},
            data={CONF_URL: remote_ha.url, CONF_TOKEN: TOKEN},
        )
    )
    try:
        async with asyncio.timeout(1):
            while not any(item["type"] == "get_states" for item in remote_ha.requests):
                await asyncio.sleep(0.01)
        flow_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await flow_task
        assert len(clients) == 1
        assert clients[0]._ws is None
        assert clients[0]._pending == {}
        async with asyncio.timeout(1):
            while remote_ha.active_sockets:
                await asyncio.sleep(0.01)
    finally:
        if not flow_task.done():
            flow_task.cancel()
            await asyncio.gather(flow_task, return_exceptions=True)
