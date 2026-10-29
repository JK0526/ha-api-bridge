"""UI configuration flow for HA API Bridge."""

from __future__ import annotations

import uuid
from typing import Any

import aiohttp
import voluptuous as vol
from homeassistant import config_entries
from homeassistant.helpers import selector

from .client import (
    HAWebSocketClient,
    RemoteAuthError,
    RemoteConnectionError,
    RemoteHAError,
    normalized_remote_url,
)
from .const import (
    CONF_ENTITY_IDS,
    CONF_INSTANCE_ID,
    CONF_REMOTE_VERSION,
    CONF_TOKEN,
    CONF_URL,
    DOMAIN,
)


class ConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Configure one remote Home Assistant instance."""

    VERSION = 1

    def __init__(self) -> None:
        self._url: str | None = None
        self._token: str | None = None
        self._version: str | None = None
        self._entities: dict[str, str] = {}

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> config_entries.FlowResult:
        """Check the URL/token and fetch climate choices."""
        errors: dict[str, str] = {}
        if user_input is not None:
            url = user_input[CONF_URL]
            token = user_input[CONF_TOKEN].strip()
            client: HAWebSocketClient | None = None
            try:
                url = normalized_remote_url(url)
                await self.async_set_unique_id(url)
                self._abort_if_unique_id_configured()
                client = HAWebSocketClient(self.hass, url, token, [])
                await client.async_connect()
                states = await client.async_get_states()
                self._entities = {}
                for item in states:
                    entity_id = item.get("entity_id")
                    attributes = item.get("attributes")
                    if not isinstance(entity_id, str) or not entity_id.startswith(
                        "climate."
                    ):
                        continue
                    if not isinstance(attributes, dict):
                        attributes = {}
                    friendly_name = attributes.get("friendly_name")
                    self._entities[entity_id] = (
                        friendly_name if isinstance(friendly_name, str) else entity_id
                    )
                if not self._entities:
                    return self.async_abort(reason="no_climate_entities")
                self._url = url
                self._token = token
                self._version = client.ha_version
                return await self.async_step_select_entities()
            except RemoteAuthError:
                errors["base"] = "invalid_auth"
            except (
                TimeoutError,
                RemoteConnectionError,
                aiohttp.ClientError,
                ValueError,
            ):
                errors["base"] = "cannot_connect"
            except RemoteHAError:
                errors["base"] = "unknown"
            finally:
                if client is not None:
                    await client.async_close()

        schema = vol.Schema(
            {
                vol.Required(CONF_URL): str,
                vol.Required(CONF_TOKEN): selector.TextSelector(
                    selector.TextSelectorConfig(type=selector.TextSelectorType.PASSWORD)
                ),
            }
        )
        return self.async_show_form(step_id="user", data_schema=schema, errors=errors)

    async def async_step_select_entities(
        self, user_input: dict[str, Any] | None = None
    ) -> config_entries.FlowResult:
        """Let the user select remote climate entities."""
        if self._url is None or self._token is None:
            return self.async_abort(reason="unknown")

        errors: dict[str, str] = {}
        if user_input is not None:
            entity_ids = [
                entity_id
                for entity_id in user_input.get(CONF_ENTITY_IDS, [])
                if entity_id in self._entities
            ]
            if not entity_ids:
                errors["base"] = "no_entities_selected"
            else:
                host = self._url.split("://", 1)[-1].split("/", 1)[0]
                return self.async_create_entry(
                    title=f"HA API Bridge ({host})",
                    data={
                        CONF_URL: self._url,
                        CONF_TOKEN: self._token,
                        CONF_ENTITY_IDS: entity_ids,
                        CONF_REMOTE_VERSION: self._version,
                        CONF_INSTANCE_ID: str(uuid.uuid4()),
                    },
                )

        options = [
            {"label": name, "value": entity_id}
            for entity_id, name in self._entities.items()
        ]
        schema = vol.Schema(
            {
                vol.Required(CONF_ENTITY_IDS): selector.SelectSelector(
                    selector.SelectSelectorConfig(options=options, multiple=True)
                )
            }
        )
        return self.async_show_form(
            step_id="select_entities", data_schema=schema, errors=errors
        )
