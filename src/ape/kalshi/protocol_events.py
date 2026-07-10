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
