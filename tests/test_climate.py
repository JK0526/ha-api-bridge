"""Tests for the Climate proxy's mapping and non-optimistic commands."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from homeassistant.components.climate import ClimateEntityFeature, HVACMode
from homeassistant.config_entries import SOURCE_USER
from homeassistant.helpers.entity_platform import async_get_platforms

from custom_components.ha_api_bridge.client import HAWebSocketClient
from custom_components.ha_api_bridge.climate import RemoteClimateEntity
from custom_components.ha_api_bridge.const import (
    CONF_ENTITY_IDS,
    CONF_TOKEN,
    CONF_URL,
    DOMAIN,
)
from tests.conftest import ENTITY_ID, TOKEN, MockRemoteHA

pytestmark = pytest.mark.enable_socket


async def _create_platform_proxy(hass: Any, remote_ha: MockRemoteHA) -> None:
    """Create and set up a real config entry and Climate platform entity."""
    result = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": SOURCE_USER},
        data={CONF_URL: remote_ha.url, CONF_TOKEN: TOKEN},
    )
    assert result["type"] == "form"
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {CONF_ENTITY_IDS: [ENTITY_ID]}
    )
    assert result["type"] == "create_entry"


def _proxy_state(hass: Any) -> Any:
    """Find the Climate state created by the integration platform."""
    states = hass.states.async_all("climate")
    return next(
        state
        for state in states
        if state.attributes.get("friendly_name") == "Office AC"
    )


def _proxy_entity(hass: Any) -> RemoteClimateEntity:
    """Find the live RemoteClimateEntity for its public Climate properties."""
    platforms = async_get_platforms(hass, DOMAIN)
    return next(
        entity
        for platform in platforms
        for entity in platform.entities.values()
        if isinstance(entity, RemoteClimateEntity)
    )


@pytest.mark.parametrize(
    ("entity_unit", "remote_unit", "expected_unit"),
    [
        ("°F", "°C", "°F"),
        (None, "°C", "°C"),
        (None, "°F", "°F"),
        (None, None, None),
        ("unsupported-unit", "°F", "°F"),
        ("unsupported-unit", "unsupported-unit", None),
    ],
)
async def test_temperature_unit_resolution_and_supported_features(
    hass: Any,
    remote_ha: MockRemoteHA,
    entity_unit: str | None,
    remote_unit: str | None,
    expected_unit: str | None,
) -> None:
    """Resolve the entity unit first, then Remote HA's configured unit."""
    attributes = remote_ha.states[0]["attributes"]
    attributes["current_temperature"] = 27
    attributes["temperature"] = 27
    attributes["min_temp"] = 16
    attributes["max_temp"] = 30
    attributes["target_temp_step"] = 1
    attributes["supported_features"] = 393
    if entity_unit is None:
        attributes.pop("temperature_unit")
    else:
        attributes["temperature_unit"] = entity_unit
    remote_ha.unit_system_temperature = remote_unit

    client = HAWebSocketClient(hass, remote_ha.url, TOKEN, [ENTITY_ID])
    await client.async_start()
    try:
        async with asyncio.timeout(1):
            while not client.synced:
                await asyncio.sleep(0.01)
        entity = RemoteClimateEntity(client, "temperature-unit-test", ENTITY_ID)
        assert client.remote_temperature_unit == remote_unit
        assert entity.temperature_unit == expected_unit
        if expected_unit is None:
            assert entity.target_temperature is None
            assert entity.current_temperature is None
            assert entity.min_temp is None
            assert entity.max_temp is None
            assert entity.target_temperature_step is None
            assert (
                not entity.supported_features & ClimateEntityFeature.TARGET_TEMPERATURE
            )
        else:
            assert entity.target_temperature == 27
            assert entity.current_temperature == 27
            assert entity.min_temp == 16
            assert entity.max_temp == 30
            assert entity.target_temperature_step == 1
            assert entity.supported_features & ClimateEntityFeature.TARGET_TEMPERATURE
        assert entity.supported_features & ClimateEntityFeature.FAN_MODE
        assert entity.supported_features & ClimateEntityFeature.TURN_OFF
        assert entity.supported_features & ClimateEntityFeature.TURN_ON
    finally:
        await client.async_close()


async def test_unit_fallback_writes_temperature_state_with_feature_393(
    hass: Any,
    remote_ha: MockRemoteHA,
    enable_custom_integrations: None,
) -> None:
    """A Celsius fallback writes valid Climate values and keeps feature 393."""
    attributes = remote_ha.states[0]["attributes"]
    attributes.pop("temperature_unit")
    attributes.update(
        {
            "current_temperature": 27,
            "temperature": 27,
            "min_temp": 16,
            "max_temp": 30,
            "target_temp_step": 1,
            "supported_features": 393,
        }
    )
    remote_ha.unit_system_temperature = "°C"
    await _create_platform_proxy(hass, remote_ha)

    async with asyncio.timeout(1):
        while _proxy_state(hass).attributes.get("temperature") != 27:
            await asyncio.sleep(0.01)
    entity = _proxy_entity(hass)
    state = _proxy_state(hass)
    assert entity.temperature_unit == "°C"
    assert entity.target_temperature == 27
    assert entity.current_temperature == 27
    assert entity.min_temp == 16
    assert entity.max_temp == 30
    assert entity.target_temperature_step == 1
    assert state.attributes["temperature"] == 27
    assert state.attributes["current_temperature"] == 27
    assert state.attributes["min_temp"] == 16
    assert state.attributes["max_temp"] == 30
    assert state.attributes["target_temp_step"] == 1
    assert state.attributes["supported_features"] == 393
    assert state.attributes["supported_features"] & int(
        ClimateEntityFeature.TARGET_TEMPERATURE
    )


async def test_remote_state_deltas_write_the_client_climate_state(
    hass: Any,
    remote_ha: MockRemoteHA,
    enable_custom_integrations: None,
) -> None:
    """Real platform entities mirror 2026.10.3 compressed state deltas."""
    await _create_platform_proxy(hass, remote_ha)
    state = _proxy_state(hass)
    assert state.state == "cool"
    assert state.attributes["temperature"] == 25.0
    assert state.attributes["current_temperature"] == 26.0
    assert state.attributes["fan_mode"] == "auto"
    assert state.attributes["swing_mode"] == "off"

    subscription_id = remote_ha.subscription_ids[-1]
    await remote_ha.send(
        {
            "id": subscription_id,
            "type": "event",
            "event": {"c": {ENTITY_ID: {"+": {"a": {"temperature": 24.0}}}}},
        }
    )
    async with asyncio.timeout(1):
        while _proxy_state(hass).attributes["temperature"] != 24.0:
            await asyncio.sleep(0.01)

    await remote_ha.send(
        {
            "id": subscription_id,
            "type": "event",
            "event": {"c": {ENTITY_ID: {"+": {"s": "heat"}}}},
        }
    )
    async with asyncio.timeout(1):
        while _proxy_state(hass).state != "heat":
            await asyncio.sleep(0.01)

    await remote_ha.send(
        {
            "id": subscription_id,
            "type": "event",
            "event": {
                "c": {
                    ENTITY_ID: {
                        "+": {"a": {"fan_mode": "low", "swing_mode": "vertical"}},
                        "-": {"a": ["preset_mode"]},
                    }
                }
            },
        }
    )
    async with asyncio.timeout(1):
        while _proxy_state(hass).attributes["fan_mode"] != "low":
            await asyncio.sleep(0.01)
    final_state = _proxy_state(hass)
    assert final_state.state == "heat"
    assert final_state.attributes["temperature"] == 24.0
    assert final_state.attributes["fan_mode"] == "low"
    assert final_state.attributes["swing_mode"] == "vertical"
    assert final_state.attributes["preset_mode"] is None


async def test_client_command_waits_for_remote_subscribe_entities_update(
    hass: Any,
    remote_ha: MockRemoteHA,
    enable_custom_integrations: None,
) -> None:
    """A successful command does not optimistically write the proxy state."""
    remote_ha.states[0]["attributes"]["temperature"] = 24.0
    await _create_platform_proxy(hass, remote_ha)
    state = _proxy_state(hass)
    assert state.attributes["temperature"] == 24.0

    await hass.services.async_call(
        "climate",
        "set_temperature",
        {"entity_id": state.entity_id, "temperature": 25.0},
        blocking=True,
    )
    assert next(item for item in remote_ha.requests if item["type"] == "call_service")[
        "service_data"
    ] == {"temperature": 25.0}
    assert _proxy_state(hass).attributes["temperature"] == 24.0

    remote_ha.states[0]["attributes"]["temperature"] = 25.0
    await remote_ha.send(
        {
            "id": remote_ha.subscription_ids[-1],
            "type": "event",
            "event": {"c": {ENTITY_ID: {"+": {"a": {"temperature": 25.0}}}}},
        }
    )
    async with asyncio.timeout(1):
        while _proxy_state(hass).attributes["temperature"] != 25.0:
            await asyncio.sleep(0.01)


async def test_climate_mapping_and_non_optimistic_command(
    hass: Any, remote_ha: MockRemoteHA
) -> None:
    client = HAWebSocketClient(hass, remote_ha.url, TOKEN, [ENTITY_ID])
    await client.async_start()
    try:
        async with asyncio.timeout(1):
            while not client.synced:
                await asyncio.sleep(0.01)
        entity = RemoteClimateEntity(client, "a-stable-entry-uuid", ENTITY_ID)
        assert entity.name == "Office AC"
        assert entity.hvac_mode == HVACMode.COOL
        assert entity.hvac_modes == [
            HVACMode.OFF,
            HVACMode.COOL,
            HVACMode.HEAT,
            HVACMode.FAN_ONLY,
        ]
        assert entity.current_temperature == 26.0
        assert entity.target_temperature == 25.0
        assert entity.target_temperature_high == 27.0
        assert entity.target_temperature_low == 23.0
        assert entity.temperature_unit == "°C"
        assert entity.min_temp == 16.0
        assert entity.max_temp == 30.0
        assert entity.target_temperature_step == 0.5
        assert entity.supported_features & ClimateEntityFeature.TARGET_TEMPERATURE
        assert entity.supported_features & ClimateEntityFeature.FAN_MODE
        assert entity.supported_features & ClimateEntityFeature.SWING_MODE
        assert entity.supported_features & ClimateEntityFeature.PRESET_MODE
        assert entity.available

        await entity.async_set_temperature(temperature=24)
        assert entity.target_temperature == 25.0
        await entity.async_turn_on()
        await entity.async_set_temperature(target_temp_low=21, target_temp_high=25)
        call = next(
            item for item in remote_ha.requests if item["type"] == "call_service"
        )
        assert call["service"] == "set_temperature"
        assert call["service_data"] == {"temperature": 24}

        await entity.async_set_hvac_mode(HVACMode.FAN_ONLY)
        await entity.async_set_fan_mode("high")
        await entity.async_set_swing_mode("vertical")
        await entity.async_set_preset_mode("eco")
        await entity.async_turn_off()
        assert entity.hvac_mode == HVACMode.COOL
        calls = [item for item in remote_ha.requests if item["type"] == "call_service"]
        assert [item["service"] for item in calls] == [
            "set_temperature",
            "turn_on",
            "set_temperature",
            "set_hvac_mode",
            "set_fan_mode",
            "set_swing_mode",
            "set_preset_mode",
            "turn_off",
        ]
        assert [item.get("service_data", {}) for item in calls] == [
            {"temperature": 24},
            {},
            {"target_temp_low": 21, "target_temp_high": 25},
            {"hvac_mode": "fan_only"},
            {"fan_mode": "high"},
            {"swing_mode": "vertical"},
            {"preset_mode": "eco"},
            {},
        ]
        assert all(item["domain"] == "climate" for item in calls)
        assert all(item["target"] == {"entity_id": ENTITY_ID} for item in calls)
    finally:
        await client.async_close()


async def test_unavailable_removed_and_unknown_states(
    hass: Any, remote_ha: MockRemoteHA
) -> None:
    client = HAWebSocketClient(hass, remote_ha.url, TOKEN, [ENTITY_ID])
    await client.async_start()
    try:
        async with asyncio.timeout(1):
            while not client.synced:
                await asyncio.sleep(0.01)
        entity = RemoteClimateEntity(client, "a-stable-entry-uuid", ENTITY_ID)
        assert entity.available
        subscription_id = remote_ha.subscription_ids[0]
        await remote_ha.send(
            {
                "id": subscription_id,
                "type": "event",
                "event": {"c": {ENTITY_ID: {"+": {"s": "unknown"}}}},
            }
        )
        async with asyncio.timeout(1):
            while client.states[ENTITY_ID]["state"] != "unknown":
                await asyncio.sleep(0.01)
        assert entity.available

        await remote_ha.send(
            {
                "id": subscription_id,
                "type": "event",
                "event": {"c": {ENTITY_ID: {"+": {"s": "unavailable"}}}},
            }
        )
        async with asyncio.timeout(1):
            while client.states[ENTITY_ID]["state"] != "unavailable":
                await asyncio.sleep(0.01)
        assert not entity.available

        await remote_ha.send(
            {
                "id": subscription_id,
                "type": "event",
                "event": {"a": {ENTITY_ID: {"s": "heat", "a": {}}}},
            }
        )
        async with asyncio.timeout(1):
            while client.states[ENTITY_ID]["state"] != "heat":
                await asyncio.sleep(0.01)
        assert entity.available

        await remote_ha.send(
            {
                "id": subscription_id,
                "type": "event",
                "event": {"r": [ENTITY_ID]},
            }
        )
        async with asyncio.timeout(1):
            while ENTITY_ID in client.states:
                await asyncio.sleep(0.01)
        assert not entity.available
    finally:
        await client.async_close()


async def test_missing_climate_attributes_are_not_fabricated(
    hass: Any, remote_ha: MockRemoteHA
) -> None:
    remote_ha.unit_system_temperature = None
    state = remote_ha.states[0]
    state["attributes"].pop("temperature")
    state["attributes"].pop("temperature_unit")
    state["attributes"].pop("min_temp")
    state["attributes"].pop("max_temp")
    state["attributes"].pop("fan_modes")
    state["attributes"].pop("swing_modes")
    state["attributes"].pop("preset_modes")

    client = HAWebSocketClient(hass, remote_ha.url, TOKEN, [ENTITY_ID])
    await client.async_start()
    entity = RemoteClimateEntity(client, "a-stable-entry-uuid", ENTITY_ID)
    try:
        assert entity.target_temperature is None
        assert entity.temperature_unit is None
        assert entity.min_temp is None
        assert entity.max_temp is None
        assert entity.fan_modes is None
        assert entity.swing_modes is None
        assert entity.preset_modes is None
        assert not entity.supported_features & ClimateEntityFeature.TARGET_TEMPERATURE
        assert not entity.supported_features & ClimateEntityFeature.FAN_MODE
        assert not entity.supported_features & ClimateEntityFeature.SWING_MODE
        assert not entity.supported_features & ClimateEntityFeature.PRESET_MODE
    finally:
        await client.async_close()


@pytest.mark.parametrize("temperature_unit", [None, "unsupported-unit"])
async def test_invalid_temperature_unit_is_safe_on_climate_platform(
    hass: Any,
    remote_ha: MockRemoteHA,
    enable_custom_integrations: None,
    temperature_unit: str | None,
) -> None:
    remote_ha.unit_system_temperature = None
    attributes = remote_ha.states[0]["attributes"]
    attributes["temperature_unit"] = temperature_unit
    attributes["current_temperature"] = 24
    attributes["temperature"] = 25
    attributes["target_temp_high"] = 27
    attributes["target_temp_low"] = 23
    await _create_platform_proxy(hass, remote_ha)

    state = _proxy_state(hass)
    assert state.state == "cool"
    assert state.attributes.get("current_temperature") is None
    assert state.attributes.get("target_temperature") is None
    assert state.attributes.get("target_temp_high") is None
    assert state.attributes.get("target_temp_low") is None
    assert state.attributes.get("min_temp") is None
    assert state.attributes.get("max_temp") is None
    features = state.attributes.get("supported_features", 0)
    assert not features & ClimateEntityFeature.TARGET_TEMPERATURE
    assert not features & ClimateEntityFeature.TARGET_TEMPERATURE_RANGE


async def test_boolean_supported_features_disable_all_optional_features(
    hass: Any,
    remote_ha: MockRemoteHA,
    enable_custom_integrations: None,
) -> None:
    remote_ha.states[0]["attributes"]["supported_features"] = True
    await _create_platform_proxy(hass, remote_ha)

    state = _proxy_state(hass)
    assert state.attributes.get("supported_features", 0) == 0
    assert state.attributes.get("target_temperature") is None
    assert state.attributes.get("target_temp_high") is None
    assert state.attributes.get("target_temp_low") is None
    assert state.attributes.get("fan_mode") is None
    assert state.attributes.get("preset_mode") is None
    assert state.attributes.get("swing_mode") is None


async def test_malformed_climate_modes_are_safe_on_climate_platform(
    hass: Any,
    remote_ha: MockRemoteHA,
    enable_custom_integrations: None,
) -> None:
    attributes = remote_ha.states[0]["attributes"]
    attributes["hvac_modes"] = ["off", None, True, 1, {}, ["heat"], "cool", "invalid"]
    attributes["fan_mode"] = {}
    attributes["preset_mode"] = 123
    attributes["swing_mode"] = []
    await _create_platform_proxy(hass, remote_ha)

    state = _proxy_state(hass)
    assert state.state == "cool"
    assert state.attributes.get("fan_mode") is None
    assert state.attributes.get("preset_mode") is None
    assert state.attributes.get("swing_mode") is None
    assert state.attributes["hvac_modes"] == ["off", "cool"]
