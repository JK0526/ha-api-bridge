"""HA API Bridge integration setup."""

from __future__ import annotations

import asyncio

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady

from .client import HAWebSocketClient, RemoteHAError
from .const import CONF_ENTITY_IDS, CONF_TOKEN, CONF_URL

PLATFORMS: list[Platform] = [Platform.CLIMATE]


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up one persistent remote connection for this config entry."""
    client = HAWebSocketClient(
        hass,
        entry.data[CONF_URL],
        entry.data[CONF_TOKEN],
        entry.data[CONF_ENTITY_IDS],
    )
    try:
        await client.async_start()
    except asyncio.CancelledError:
        await client.async_close()
        raise
    except RemoteHAError as err:
        await client.async_close()
        raise ConfigEntryNotReady("Unable to connect to Remote Home Assistant") from err

    entry.runtime_data = client
    try:
        await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    except asyncio.CancelledError:
        await client.async_close()
        raise
    except Exception:
        await client.async_close()
        raise
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload platforms and close the remote connection."""
    if not await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
        return False

    await entry.runtime_data.async_close()
    return True
