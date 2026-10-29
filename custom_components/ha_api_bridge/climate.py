"""Remote climate proxy entities."""

from __future__ import annotations

from typing import Any

from homeassistant.components.climate import (
    ClimateEntity,
    ClimateEntityFeature,
    HVACMode,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .client import HAWebSocketClient
from .const import (
    ATTR_ATTRIBUTES,
    ATTR_STATE,
    CONF_ENTITY_IDS,
    CONF_INSTANCE_ID,
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Create one proxy for each selected remote climate entity."""
    client: HAWebSocketClient = entry.runtime_data
    async_add_entities(
        RemoteClimateEntity(client, entry.data[CONF_INSTANCE_ID], entity_id)
        for entity_id in entry.data[CONF_ENTITY_IDS]
    )


class RemoteClimateEntity(ClimateEntity):
    """Climate entity that mirrors one selected remote entity."""

    _attr_has_entity_name = False

    def __init__(
        self, client: HAWebSocketClient, instance_id: str, remote_entity_id: str
    ) -> None:
        self._client = client
        self._remote_entity_id = remote_entity_id
        self._attr_unique_id = f"{instance_id}:{remote_entity_id}"
        self._sync_from_remote()
        self._remove_listener = client.add_listener(self._handle_update)

    @property
    def name(self) -> str | None:
        """Use Remote HA's friendly name when present."""
        attributes = self._attributes
        if attributes is None:
            return self._remote_entity_id.split(".", 1)[-1].replace("_", " ")
        name = attributes.get("friendly_name")
        return (
            name
            if isinstance(name, str)
            else self._remote_entity_id.split(".", 1)[-1].replace("_", " ")
        )

    @property
    def available(self) -> bool:
        """Require an authenticated, synchronized state that is not unavailable."""
        remote_state = self._state
        return (
            self._client.connected
            and self._client.synced
            and remote_state is not None
            and remote_state.get(ATTR_STATE) != "unavailable"
        )

    @property
    def _state(self) -> dict[str, Any] | None:
        return self._client.states.get(self._remote_entity_id)

    @property
    def _attributes(self) -> dict[str, Any] | None:
        state = self._state
        if state is None:
            return None
        attributes = state.get(ATTR_ATTRIBUTES)
        return attributes if isinstance(attributes, dict) else None

    @staticmethod
    def _number(value: Any) -> float | None:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
        return None

    @classmethod
    def _supported_features(cls, attributes: dict[str, Any]) -> ClimateEntityFeature:
        """Expose only remote features this proxy implements."""
        try:
            remote = ClimateEntityFeature(int(attributes.get("supported_features", 0)))
        except (TypeError, ValueError):
            return ClimateEntityFeature(0)

        supported = ClimateEntityFeature(0)
        for feature in (
            ClimateEntityFeature.TARGET_TEMPERATURE,
            ClimateEntityFeature.TARGET_TEMPERATURE_RANGE,
            ClimateEntityFeature.FAN_MODE,
            ClimateEntityFeature.PRESET_MODE,
            ClimateEntityFeature.SWING_MODE,
            ClimateEntityFeature.TURN_ON,
            ClimateEntityFeature.TURN_OFF,
        ):
            if remote & feature:
                supported |= feature

        # HA validates set_temperature against min/max. Only advertise it when
        # the remote entity supplies both limits, so no local defaults are used.
        if (
            cls._number(attributes.get("min_temp")) is None
            or cls._number(attributes.get("max_temp")) is None
            or not isinstance(attributes.get("temperature_unit"), str)
        ):
            supported &= ~(
                ClimateEntityFeature.TARGET_TEMPERATURE
                | ClimateEntityFeature.TARGET_TEMPERATURE_RANGE
            )
        if not cls._string_list(attributes.get("fan_modes")):
            supported &= ~ClimateEntityFeature.FAN_MODE
        if not cls._string_list(attributes.get("preset_modes")):
            supported &= ~ClimateEntityFeature.PRESET_MODE
        if not cls._string_list(attributes.get("swing_modes")):
            supported &= ~ClimateEntityFeature.SWING_MODE
        return supported

    @staticmethod
    def _string_list(value: Any) -> list[str] | None:
        if not isinstance(value, list):
            return None
        return [item for item in value if isinstance(item, str)]

    @staticmethod
    def _hvac_modes(value: Any) -> list[HVACMode] | None:
        if not isinstance(value, list):
            return None
        modes: list[HVACMode] = []
        for item in value:
            try:
                modes.append(HVACMode(item))
            except (ValueError, TypeError):
                continue
        return modes

    def _sync_from_remote(self) -> None:
        """Copy the latest snapshot to Home Assistant's climate backing fields.

        The legacy ``_attr_*`` fields are still supported by the current Core
        versions and are migrated to the native temperature fields by newer
        Core versions. This keeps the integration compatible across that API
        transition without inventing missing remote values.
        """
        attributes = self._attributes or {}
        state = self._state
        mode = state.get(ATTR_STATE) if state else None
        try:
            self._attr_hvac_mode = HVACMode(mode) if isinstance(mode, str) else None
        except ValueError:
            self._attr_hvac_mode = None

        self._attr_hvac_modes = self._hvac_modes(attributes.get("hvac_modes"))
        self._attr_current_temperature = self._number(
            attributes.get("current_temperature")
        )
        self._attr_native_current_temperature = self._attr_current_temperature
        self._attr_target_temperature = self._number(attributes.get("temperature"))
        self._attr_native_target_temperature = self._attr_target_temperature
        self._attr_target_temperature_high = self._number(
            attributes.get("target_temp_high")
        )
        self._attr_native_target_temperature_high = self._attr_target_temperature_high
        self._attr_target_temperature_low = self._number(
            attributes.get("target_temp_low")
        )
        self._attr_native_target_temperature_low = self._attr_target_temperature_low
        self._attr_temperature_unit = attributes.get("temperature_unit")
        self._attr_native_temperature_unit = self._attr_temperature_unit
        self._attr_min_temp = self._number(attributes.get("min_temp"))
        self._attr_max_temp = self._number(attributes.get("max_temp"))
        self._attr_target_temperature_step = self._number(
            attributes.get("target_temp_step")
        )
        self._attr_fan_mode = attributes.get("fan_mode")
        self._attr_fan_modes = self._string_list(attributes.get("fan_modes"))
        self._attr_swing_mode = attributes.get("swing_mode")
        self._attr_swing_modes = self._string_list(attributes.get("swing_modes"))
        self._attr_preset_mode = attributes.get("preset_mode")
        self._attr_preset_modes = self._string_list(attributes.get("preset_modes"))
        self._attr_supported_features = self._supported_features(attributes)

    @callback
    def _handle_update(self) -> None:
        self._sync_from_remote()
        if self.hass is None or self.entity_id is None:
            return
        self.async_write_ha_state()

    async def async_will_remove_from_hass(self) -> None:
        """Drop the client listener when this proxy is removed."""
        self._remove_listener()
        await super().async_will_remove_from_hass()

    async def async_turn_on(self) -> None:
        await self._client.async_call_service(self._remote_entity_id, "turn_on")

    async def async_turn_off(self) -> None:
        await self._client.async_call_service(self._remote_entity_id, "turn_off")

    async def async_set_hvac_mode(self, hvac_mode: HVACMode) -> None:
        await self._client.async_call_service(
            self._remote_entity_id, "set_hvac_mode", {"hvac_mode": hvac_mode}
        )

    async def async_set_temperature(self, **kwargs: Any) -> None:
        await self._client.async_call_service(
            self._remote_entity_id, "set_temperature", kwargs
        )

    async def async_set_fan_mode(self, fan_mode: str) -> None:
        await self._client.async_call_service(
            self._remote_entity_id, "set_fan_mode", {"fan_mode": fan_mode}
        )

    async def async_set_swing_mode(self, swing_mode: str) -> None:
        await self._client.async_call_service(
            self._remote_entity_id, "set_swing_mode", {"swing_mode": swing_mode}
        )

    async def async_set_preset_mode(self, preset_mode: str) -> None:
        await self._client.async_call_service(
            self._remote_entity_id, "set_preset_mode", {"preset_mode": preset_mode}
        )
