from __future__ import annotations

PROTOCOL_ERROR_EVENTS = {
    "websocket_error",
    "subscription_error",
    "list_subscriptions_error",
    "update_subscription_error",
    "get_snapshot_error",
    "reconnect_failed",
}
PROTOCOL_RECOVERY_EVENTS = {
    "reconnect_completed",
}
PROTOCOL_NORMAL_CLOSE_CODES = {1000, 1001}


def protocol_event_counts_as_error(
    event_type: str,
    *,
    close_code: int | None = None,
) -> bool:
    if event_type in PROTOCOL_ERROR_EVENTS:
        return True
    if event_type == "websocket_close":
        return close_code not in {None, *PROTOCOL_NORMAL_CLOSE_CODES}
    return False
