"""Small persistent WebSocket client for a remote Home Assistant instance."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import aiohttp
from aiohttp import WSMsgType
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .const import (
    ATTR_ATTRIBUTES,
    ATTR_STATE,
    AUTH_TIMEOUT,
    CONNECT_TIMEOUT,
    RECONNECT_DELAYS,
    REQUEST_TIMEOUT,
)

_LOGGER = logging.getLogger(__name__)


class RemoteHAError(HomeAssistantError):
    """Base exception for remote Home Assistant errors."""


class RemoteAuthError(RemoteHAError):
    """The remote access token was rejected."""


class RemoteConnectionError(RemoteHAError):
    """The remote WebSocket could not be used."""


class RemoteCommandError(RemoteHAError):
    """The remote Home Assistant rejected a WebSocket command."""


class RemoteRequestTimeout(RemoteHAError):
    """A remote WebSocket command did not respond in time."""


StateListener = Callable[[], None]


def websocket_url(base_url: str) -> str:
    """Convert a Home Assistant HTTP(S) base URL into its WebSocket URL."""
    normalized = normalized_remote_url(base_url)
    parsed = urlsplit(normalized)
    path = parsed.path
    if not path.endswith("/api/websocket"):
        path = f"{path}/api/websocket"
    scheme = "wss" if parsed.scheme == "https" else "ws"
    return urlunsplit((scheme, parsed.netloc, path, "", ""))


def normalized_remote_url(base_url: str) -> str:
    """Return a stable, credential-free HTTP(S) base URL for config identity."""
    base_url = base_url.strip()
    if "://" not in base_url:
        base_url = f"http://{base_url}"

    parsed = urlsplit(base_url)
    scheme = parsed.scheme.lower()
    if (
        scheme not in ("http", "https")
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("Enter an HTTP or HTTPS Home Assistant URL")

    try:
        port = parsed.port
    except ValueError as err:
        raise ValueError("Enter a valid Home Assistant URL") from err

    hostname = parsed.hostname.lower()
    if ":" in hostname:
        hostname = f"[{hostname}]"
    default_port = 443 if scheme == "https" else 80
    netloc = hostname if port in (None, default_port) else f"{hostname}:{port}"

    path = parsed.path.rstrip("/")
    if path.endswith("/api/websocket"):
        raise ValueError(
            "Enter the Remote Home Assistant base URL, not its WebSocket URL"
        )
    return urlunsplit((scheme, netloc, path, "", ""))


class HAWebSocketClient:
    """One socket, one reader and multiplexed request futures."""

    def __init__(
        self,
        hass: HomeAssistant,
        base_url: str,
        token: str,
        entity_ids: list[str] | tuple[str, ...],
    ) -> None:
        self._hass = hass
        self._url = websocket_url(base_url)
        self._token = token
        self.entity_ids = tuple(entity_ids)
        self.ha_version: str | None = None

        self._session = async_get_clientsession(hass)
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._reconnect_task: asyncio.Task[None] | None = None
        self._start_lock = asyncio.Lock()
        self._close_lock = asyncio.Lock()
        self._send_lock = asyncio.Lock()
        self._next_request_id = 0
        self._pending: dict[int, asyncio.Future[Any]] = {}
        self._subscription_id: int | None = None
        self._states: dict[str, dict[str, Any]] = {}
        self._listeners: set[StateListener] = set()
        self._connected = False
        self._synced = False
        self._stopping = False
        self._persistent = False

    @property
    def connected(self) -> bool:
        """Return whether an authenticated socket is open."""
        return self._connected

    @property
    def synced(self) -> bool:
        """Return whether this connection received its initial state snapshot."""
        return self._synced

    @property
    def states(self) -> dict[str, dict[str, Any]]:
        """Return the selected entities' latest complete snapshots."""
        return self._states

    def add_listener(self, listener: StateListener) -> Callable[[], None]:
        """Subscribe to connection or selected-state changes."""
        self._listeners.add(listener)

        def remove_listener() -> None:
            self._listeners.discard(listener)

        return remove_listener

    def redact_for_log(self, value: Any) -> str:
        """Return a string representation with the access token removed."""
        text = str(value)
        return text.replace(self._token, "[redacted]") if self._token else text

    async def async_connect(self) -> None:
        """Connect and authenticate without starting automatic reconnection."""
        if self._ws is not None and not self._ws.closed:
            raise RemoteConnectionError("A WebSocket is already active")

        ws: aiohttp.ClientWebSocketResponse | None = None
        try:
            async with asyncio.timeout(CONNECT_TIMEOUT):
                ws = await self._session.ws_connect(self._url)
            self._ws = ws
            await self._authenticate(ws)
            if self._stopping:
                raise RemoteConnectionError("Remote HA client is stopping")
            self._states.clear()
            self._synced = False
            self._connected = True
            self._reader_task = self._hass.async_create_task(
                self._reader(ws), "ha_api_bridge websocket reader"
            )
            self._notify_listeners()
        except asyncio.CancelledError:
            if ws is not None:
                await ws.close()
            if self._ws is ws:
                self._ws = None
            raise
        except (TimeoutError, aiohttp.ClientError, RemoteHAError, ValueError) as err:
            if ws is not None:
                await ws.close()
            if self._ws is ws:
                self._ws = None
            self._connected = False
            if isinstance(err, RemoteHAError):
                raise
            raise RemoteConnectionError(
                "Unable to connect to Remote Home Assistant"
            ) from err

    async def _authenticate(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        """Complete the official Home Assistant WebSocket auth handshake."""
        try:
            async with asyncio.timeout(AUTH_TIMEOUT):
                required = await ws.receive_json()
                if (
                    not isinstance(required, dict)
                    or required.get("type") != "auth_required"
                ):
                    raise RemoteConnectionError(
                        "Remote HA sent an invalid auth handshake"
                    )
                await ws.send_json({"type": "auth", "access_token": self._token})
                result = await ws.receive_json()
        except TimeoutError as err:
            raise RemoteConnectionError("Remote HA authentication timed out") from err
        except (aiohttp.ClientError, json.JSONDecodeError, TypeError) as err:
            raise RemoteConnectionError(
                "Remote HA sent an invalid auth handshake"
            ) from err

        if not isinstance(result, dict):
            raise RemoteConnectionError("Remote HA sent an invalid auth response")
        if result.get("type") == "auth_invalid":
            raise RemoteAuthError("Remote HA rejected the access token")
        if result.get("type") != "auth_ok":
            raise RemoteConnectionError("Remote HA sent an invalid auth response")
        version = result.get("ha_version")
        self.ha_version = version if isinstance(version, str) else None

    async def async_get_states(self) -> list[dict[str, Any]]:
        """Get initial states during the config flow only."""
        result = await self._request({"type": "get_states"})
        if not isinstance(result, list) or not all(
            isinstance(item, dict) for item in result
        ):
            raise RemoteConnectionError("Remote HA returned invalid entity states")
        return result

    async def async_start(self) -> None:
        """Start the persistent selected-entity subscription and reconnect loop."""
        async with self._start_lock:
            if (
                self._reconnect_task is not None
                or self._ws is not None
                or self._reader_task is not None
            ):
                raise RemoteConnectionError("The Remote HA client is already started")
            self._stopping = False
            try:
                await self.async_connect()
                await self._subscribe()
                if self._stopping:
                    raise RemoteConnectionError("Remote HA client is stopping")
            except asyncio.CancelledError:
                await self.async_close()
                raise
            except Exception:
                await self.async_close()
                raise

            self._persistent = True
            _LOGGER.info("Connected to Remote HA")
            self._reconnect_task = self._hass.async_create_task(
                self._reconnect_loop(), "ha_api_bridge websocket reconnect"
            )

    async def _subscribe(self) -> None:
        """Subscribe to only the selected remote entities."""
        if not self.entity_ids:
            raise RemoteConnectionError("No remote entities were selected")
        await self._request(
            {"type": "subscribe_entities", "entity_ids": list(self.entity_ids)},
            subscription=True,
        )
        _LOGGER.debug(
            "Subscribed to %d selected Remote HA entities", len(self.entity_ids)
        )

    async def async_call_service(
        self, entity_id: str, service: str, service_data: dict[str, Any] | None = None
    ) -> None:
        """Call a climate service and wait for the remote command result."""
        if entity_id not in self.entity_ids:
            raise RemoteCommandError("The entity is not selected in this bridge")
        message: dict[str, Any] = {
            "type": "call_service",
            "domain": "climate",
            "service": service,
            "target": {"entity_id": entity_id},
        }
        if service_data:
            message["service_data"] = service_data
        await self._request(message)
        _LOGGER.debug("Sent climate.%s for %s to Remote HA", service, entity_id)

    async def _request(
        self, message: dict[str, Any], *, subscription: bool = False
    ) -> Any:
        """Send a request and await its matching result frame."""
        ws = self._ws
        if ws is None or ws.closed or not self._connected:
            raise RemoteConnectionError("Remote HA is disconnected")

        self._next_request_id += 1
        request_id = self._next_request_id
        future: asyncio.Future[Any] = self._hass.loop.create_future()
        self._pending[request_id] = future
        if subscription:
            self._subscription_id = request_id

        payload = {"id": request_id, **message}
        try:
            async with self._send_lock:
                await ws.send_json(payload)
            return await asyncio.wait_for(future, timeout=REQUEST_TIMEOUT)
        except TimeoutError as err:
            raise RemoteRequestTimeout(
                f"Remote HA did not respond to {message.get('type')} in time"
            ) from err
        except (aiohttp.ClientError, ConnectionError) as err:
            raise RemoteConnectionError("Remote HA connection failed") from err
        finally:
            self._pending.pop(request_id, None)
            if not future.done():
                future.cancel()

    async def _reader(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        """Read every server message from the socket in this single task."""
        try:
            async for message in ws:
                if message.type == WSMsgType.TEXT:
                    try:
                        payload = json.loads(message.data)
                    except json.JSONDecodeError as err:
                        raise RemoteConnectionError(
                            "Remote HA sent invalid JSON"
                        ) from err
                    if not isinstance(payload, dict):
                        raise RemoteConnectionError("Remote HA sent an invalid message")
                    self._dispatch_message(payload)
                elif message.type == WSMsgType.ERROR:
                    raise RemoteConnectionError("Remote HA WebSocket failed")
                elif message.type in (WSMsgType.CLOSE, WSMsgType.CLOSED):
                    break
        except asyncio.CancelledError:
            raise
        except TimeoutError as err:
            raise RemoteConnectionError(
                "Remote HA WebSocket receive timed out"
            ) from err
        except (aiohttp.ClientError, ConnectionError) as err:
            raise RemoteConnectionError("Remote HA WebSocket failed") from err
        finally:
            if ws is self._ws and not self._stopping:
                self._mark_disconnected()

    def _mark_disconnected(self) -> None:
        """Immediately make all selected proxies unavailable."""
        if not self._connected:
            return
        self._connected = False
        self._synced = False
        self._states.clear()
        self._subscription_id = None
        self._fail_pending(RemoteConnectionError("Remote HA disconnected"))
        self._notify_listeners()
        if self._persistent:
            _LOGGER.info("Disconnected from Remote HA")

    def _dispatch_message(self, payload: dict[str, Any]) -> None:
        """Resolve results and forward subscription events."""
        message_type = payload.get("type")
        request_id = payload.get("id")
        if message_type == "result" and isinstance(request_id, int):
            future = self._pending.get(request_id)
            if future is None or future.done():
                return
            if payload.get("success") is not True:
                error = payload.get("error")
                detail = (
                    error.get("message", "Unknown remote error")
                    if isinstance(error, dict)
                    else "Unknown remote error"
                )
                detail = str(detail).replace(self._token, "[redacted]")
                future.set_exception(
                    RemoteCommandError(f"Remote HA rejected a command: {detail}")
                )
            else:
                future.set_result(payload.get("result"))
            return

        if (
            message_type == "event"
            and request_id == self._subscription_id
            and isinstance(payload.get("event"), dict)
        ):
            self._apply_entities_event(payload["event"])

    def _apply_entities_event(self, event: dict[str, Any]) -> None:
        """Expand compact additions/deltas/removals into complete snapshots."""
        changed = not self._synced
        touched_entities: set[str] = set()
        added = event.get("a")
        if isinstance(added, dict):
            for entity_id, compact in added.items():
                self._log_entity_payload(entity_id, "a", compact)
                if entity_id not in self.entity_ids or not isinstance(compact, dict):
                    continue
                attributes = compact.get("a")
                snapshot = {
                    ATTR_STATE: compact.get("s"),
                    ATTR_ATTRIBUTES: dict(attributes)
                    if isinstance(attributes, dict)
                    else {},
                }
                for compressed_key, state_key in (
                    ("c", "context"),
                    ("lc", "last_changed"),
                    ("lu", "last_updated"),
                ):
                    if compressed_key in compact:
                        snapshot[state_key] = compact[compressed_key]
                self._states[entity_id] = snapshot
                touched_entities.add(entity_id)
                changed = True

        deltas = event.get("c")
        if isinstance(deltas, dict):
            for entity_id, delta in deltas.items():
                self._log_entity_payload(entity_id, "c", delta)
                snapshot = self._states.get(entity_id)
                if (
                    entity_id not in self.entity_ids
                    or snapshot is None
                    or not isinstance(delta, dict)
                ):
                    continue
                additions = delta.get("+")
                if isinstance(additions, dict):
                    if "s" in additions:
                        snapshot[ATTR_STATE] = additions["s"]
                    for compressed_key, state_key in (
                        ("lc", "last_changed"),
                        ("lu", "last_updated"),
                    ):
                        if compressed_key in additions:
                            snapshot[state_key] = additions[compressed_key]
                    if "c" in additions:
                        context = additions["c"]
                        existing_context = snapshot.get("context")
                        if isinstance(context, dict):
                            merged_context = (
                                {"id": existing_context}
                                if isinstance(existing_context, str)
                                else dict(existing_context)
                                if isinstance(existing_context, dict)
                                else {}
                            )
                            merged_context.update(context)
                            snapshot["context"] = merged_context
                        else:
                            snapshot["context"] = context
                    attribute_additions = additions.get("a")
                    if isinstance(attribute_additions, dict):
                        snapshot[ATTR_ATTRIBUTES].update(attribute_additions)

                removals = delta.get("-")
                if isinstance(removals, dict):
                    removed_attributes = removals.get("a")
                    if isinstance(removed_attributes, list):
                        for attribute in removed_attributes:
                            if isinstance(attribute, str):
                                snapshot[ATTR_ATTRIBUTES].pop(attribute, None)

                touched_entities.add(entity_id)
                changed = True

        removed_ids = event.get("r")
        if isinstance(removed_ids, list):
            for entity_id in removed_ids:
                self._log_entity_payload(entity_id, "r", [entity_id])
                if (
                    entity_id in self.entity_ids
                    and self._states.pop(entity_id, None) is not None
                ):
                    touched_entities.add(entity_id)
                    changed = True

        self._synced = True
        for entity_id in touched_entities:
            snapshot = self._states.get(entity_id)
            self._log_merged_snapshot(entity_id, snapshot)
        if changed:
            self._notify_listeners()

    def _log_entity_payload(
        self, entity_id: str, event_type: str, payload: Any
    ) -> None:
        """Log selected compressed payloads at DEBUG while redacting the token."""
        if entity_id not in self.entity_ids or not _LOGGER.isEnabledFor(logging.DEBUG):
            return
        raw_payload = self.redact_for_log(
            json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        )
        _LOGGER.debug(
            "RX subscribe_entities: entity_id=%s event type=%s "
            "raw compressed entity payload=%s",
            entity_id,
            event_type,
            raw_payload,
        )

    def _log_merged_snapshot(
        self, entity_id: str, snapshot: dict[str, Any] | None
    ) -> None:
        """Log selected merged state and attributes at DEBUG."""
        if not _LOGGER.isEnabledFor(logging.DEBUG):
            return
        state = snapshot.get(ATTR_STATE) if snapshot else None
        attributes = snapshot.get(ATTR_ATTRIBUTES) if snapshot else None
        raw_snapshot = self.redact_for_log(
            json.dumps(
                {"state": state, "attributes": attributes},
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )
        _LOGGER.debug("Merged snapshot: entity_id=%s %s", entity_id, raw_snapshot)

    def _notify_listeners(self) -> None:
        """Notify proxy entities without doing network I/O."""
        for listener in tuple(self._listeners):
            try:
                listener()
            except Exception:
                _LOGGER.exception("Failed to update a Remote HA proxy entity")

    async def _reconnect_loop(self) -> None:
        """Reconnect in one task with bounded exponential backoff."""
        delay_index = 0
        while not self._stopping:
            reader_task = self._reader_task
            if reader_task is not None:
                try:
                    await asyncio.shield(reader_task)
                except asyncio.CancelledError:
                    if self._stopping:
                        return
                    raise
                except RemoteHAError as err:
                    _LOGGER.debug("Remote HA reader stopped: %s", err)
                except Exception:
                    _LOGGER.exception(
                        "Unexpected failure in Remote HA WebSocket reader"
                    )

            if self._stopping:
                return
            await self._disconnect()
            delay = RECONNECT_DELAYS[delay_index]
            _LOGGER.debug("Reconnecting to Remote HA in %s seconds", delay)
            await asyncio.sleep(delay)
            if self._stopping:
                return
            try:
                await self.async_connect()
                await self._subscribe()
            except asyncio.CancelledError:
                raise
            except (
                TimeoutError,
                aiohttp.ClientError,
                RemoteHAError,
                ValueError,
            ) as err:
                _LOGGER.warning("Unable to reconnect to Remote HA: %s", err)
                await self._disconnect()
                delay_index = min(delay_index + 1, len(RECONNECT_DELAYS) - 1)
                continue
            except Exception:
                _LOGGER.exception("Unexpected error reconnecting to Remote HA")
                await self._disconnect()
                delay_index = min(delay_index + 1, len(RECONNECT_DELAYS) - 1)
                continue

            _LOGGER.info("Connected to Remote HA")
            delay_index = 0

    async def _disconnect(self) -> None:
        """Mark proxies unavailable and release the old socket state."""
        was_connected = self._connected or bool(self._states) or self._synced
        self._connected = False
        self._synced = False
        self._subscription_id = None
        self._states.clear()
        for future in self._pending.values():
            if not future.done():
                future.set_exception(RemoteConnectionError("Remote HA disconnected"))
        self._pending.clear()
        ws, self._ws = self._ws, None
        reader_task, self._reader_task = self._reader_task, None
        if was_connected:
            self._notify_listeners()
            if self._persistent:
                _LOGGER.info("Disconnected from Remote HA")
        try:
            if ws is not None and not ws.closed:
                await ws.close()
        except asyncio.CancelledError:
            raise
        except Exception:
            _LOGGER.debug("Error closing Remote HA WebSocket", exc_info=True)
        finally:
            if (
                reader_task is not None
                and reader_task is not asyncio.current_task()
                and not reader_task.done()
            ):
                reader_task.cancel()
                await asyncio.gather(reader_task, return_exceptions=True)

    def _fail_pending(self, error: RemoteHAError) -> None:
        """Finish pending requests on shutdown."""
        for future in self._pending.values():
            if not future.done():
                future.set_exception(error)
        self._pending.clear()

    async def async_close(self) -> None:
        """Unsubscribe and close tasks/socket cleanly."""
        async with self._close_lock:
            self._stopping = True
            reconnect_task, self._reconnect_task = self._reconnect_task, None
            try:
                if reconnect_task is not None and not reconnect_task.done():
                    reconnect_task.cancel()
                    await asyncio.gather(reconnect_task, return_exceptions=True)

                if self._connected and self._subscription_id is not None:
                    try:
                        await self._request(
                            {
                                "type": "unsubscribe_events",
                                "subscription": self._subscription_id,
                            }
                        )
                    except asyncio.CancelledError:
                        raise
                    except (TimeoutError, RemoteHAError):
                        _LOGGER.debug(
                            "Remote HA subscription could not be removed "
                            "during shutdown"
                        )
            finally:
                try:
                    await self._disconnect()
                finally:
                    self._fail_pending(
                        RemoteConnectionError("Remote HA client stopped")
                    )
                    self._persistent = False
                    self._listeners.clear()
                    if reconnect_task is not None and not reconnect_task.done():
                        reconnect_task.cancel()
                        await asyncio.gather(reconnect_task, return_exceptions=True)
