"""Constants for HA API Bridge."""

from typing import Final

DOMAIN: Final = "ha_api_bridge"

CONF_URL: Final = "url"
CONF_TOKEN: Final = "token"
CONF_ENTITY_IDS: Final = "entity_ids"
CONF_INSTANCE_ID: Final = "remote_instance_id"

CONNECT_TIMEOUT: Final = 10
AUTH_TIMEOUT: Final = 10
REQUEST_TIMEOUT: Final = 10

RECONNECT_DELAYS: Final = (1, 2, 5, 10, 30, 60)

ATTR_STATE: Final = "state"
ATTR_ATTRIBUTES: Final = "attributes"
