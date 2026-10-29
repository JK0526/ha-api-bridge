"""Tests for the Climate proxy's mapping and non-optimistic commands."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from homeassistant.components.climate import ClimateEntityFeature, HVACMode

from custom_components.ha_api_bridge.client import HAWebSocketClient
from custom_components.ha_api_bridge.climate import RemoteClimateEntity
from tests.conftest import ENTITY_ID, TOKEN, MockRemoteHA

pytestmark = pytest.mark.enable_socket


async def test_climate_mapping_and_non_optimistic_command(
    hass: Any, remote_ha: MockRemoteHA
) -> None:
    client = HAWebSocketClient(hass, remote_ha.url, TOKEN, [ENTITY_ID])
    await client.async_start()
    entity = RemoteClimateEntity(client, "a-stable-entry-uuid", ENTITY_ID)
    try:
        async with asyncio.timeout(1):
            while not client.synced:
                await asyncio.sleep(0.01)
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
            "set_hvac_mode",
            "set_fan_mode",
            "set_swing_mode",
            "set_preset_mode",
            "turn_off",
        ]
    finally:
        entity._remove_listener()
        await client.async_close()


async def test_unavailable_removed_and_unknown_states(
    hass: Any, remote_ha: MockRemoteHA
) -> None:
    client = HAWebSocketClient(hass, remote_ha.url, TOKEN, [ENTITY_ID])
    await client.async_start()
    entity = RemoteClimateEntity(client, "a-stable-entry-uuid", ENTITY_ID)
    try:
        assert entity.available
        subscription_id = remote_ha.subscription_ids[0]
        await remote_ha.send(
            {
                "id": subscription_id,
                "type": "event",
                "event": {"c": {ENTITY_ID: {"s": "unknown"}}},
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
                "event": {"c": {ENTITY_ID: {"s": "unavailable"}}},
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
        entity._remove_listener()
        await client.async_close()


async def test_missing_climate_attributes_are_not_fabricated(
    hass: Any, remote_ha: MockRemoteHA
) -> None:
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
        entity._remove_listener()
        await client.async_close()
