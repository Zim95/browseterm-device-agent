"""
Structured logging for cross-service debugging (OBSERVABILITY.md, Pillar 1).

Every log line is emitted as a single JSON object with the canonical fields:
    timestamp, level, service, logger, message, request_id, user, + any extras.

The `request_id` is a correlation id minted at the entry point (per HTTP request) and threaded
through the whole flow (HTTP header -> contextvar -> gRPC metadata -> Job env), so a request's
lifecycle can be reconstructed across services by filtering on it. `user` is the acting
user (email) or "system:-" for background/unauthenticated lines.

Dependency-free (stdlib logging + json) so every Python service can drop this in identically.
"""
import contextvars
import json
import logging
import sys
import uuid
from datetime import datetime, timezone
from typing import Optional

# Correlation context, populated per-request (or per-run for background jobs).
request_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("request_id", default="-")
user_var: contextvars.ContextVar[str] = contextvars.ContextVar("user", default="system:-")

# Attributes already present on a LogRecord — anything else passed via `extra=` is emitted as a
# structured field (e.g. logger.info("saved", extra={"container_id": cid})).
_RESERVED = set(vars(logging.makeLogRecord({})).keys()) | {"message", "asctime", "taskName"}


class JsonFormatter(logging.Formatter):
    """Render each record as one JSON line with the canonical fields + request context + extras."""

    def __init__(self, service: str) -> None:
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        payload: dict = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "service": self.service,
            "logger": record.name,
            "message": record.getMessage(),
            "request_id": request_id_var.get(),
            "user": user_var.get(),
        }
        # Structured extras passed via logger.*(..., extra={...}).
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(service: str, level: int = logging.INFO) -> None:
    """Install the JSON formatter on the root logger. Idempotent; call once at startup."""
    root = logging.getLogger()
    root.setLevel(level)
    for h in list(root.handlers):
        root.removeHandler(h)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter(service))
    root.addHandler(handler)


def new_request_id() -> str:
    """Mint a fresh correlation id."""
    return uuid.uuid4().hex


def set_request_context(request_id: Optional[str] = None, user: Optional[str] = None) -> str:
    """Set the per-request correlation context; returns the effective request_id."""
    rid = request_id or new_request_id()
    request_id_var.set(rid)
    if user is not None:
        user_var.set(user)
    return rid


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
