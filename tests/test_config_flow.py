"""Tests for connection validation and entity selection."""

from __future__ import annotations

from typing import Any

import pytest
from homeassistant.config_entries import SOURCE_USER

from custom_components.ha_api_bridge.const import (
    CONF_ENTITY_IDS,
    CONF_INSTANCE_ID,
    CONF_REMOTE_VERSION,
    CONF_TOKEN,
    CONF_URL,
    DOMAIN,
)
from tests.conftest import ENTITY_ID, TOKEN, MockRemoteHA

pytestmark = pytest.mark.enable_socket


async def test_config_flow_auth_and_create_entry(
    hass: Any, remote_ha: MockRemoteHA, enable_custom_integrations: None
) -> None:
    result = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": SOURCE_USER},
        data={CONF_URL: remote_ha.url, CONF_TOKEN: TOKEN},
    )
    assert result["type"] == "form"
    assert result["step_id"] == "select_entities"
    assert len(remote_ha.sockets) == 1
    assert [item["type"] for item in remote_ha.requests] == ["get_states"]

    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {CONF_ENTITY_IDS: [ENTITY_ID]}
    )
    assert result["type"] == "create_entry"
    entry = result["result"]
    assert entry.data[CONF_URL] == remote_ha.url
    assert entry.data[CONF_TOKEN] == TOKEN
    assert entry.data[CONF_ENTITY_IDS] == [ENTITY_ID]
    assert entry.data[CONF_REMOTE_VERSION] == "2026.10.0"
    assert entry.data[CONF_INSTANCE_ID]
    first_client = entry.runtime_data
    assert await hass.config_entries.async_unload(entry.entry_id)
    assert not first_client.connected
    assert first_client._reader_task is None
    assert first_client._reconnect_task is None
    assert len(remote_ha.sockets) == 2
    assert len(remote_ha.subscription_ids) == 1

    assert await hass.config_entries.async_setup(entry.entry_id)
    second_client = entry.runtime_data
    assert second_client is not first_client
    assert second_client.connected
    assert len(remote_ha.sockets) == 3
    assert len(remote_ha.subscription_ids) == 2
    assert remote_ha.unsubscriptions == remote_ha.subscription_ids[:1]
    assert await hass.config_entries.async_unload(entry.entry_id)
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
