"""Exception hierarchy for the Tektii Gateway SDK."""

from __future__ import annotations

from typing import Any, NoReturn


class TektiiError(Exception):
    """Base exception for all SDK errors."""


class TektiiConnectionError(TektiiError):
    """Raised on network / transport failures talking to the gateway.

    Wraps the underlying ``httpx`` exception (timeout, DNS failure, connection
    refused, read error, pool exhaustion) behind the SDK's own error surface
    so users never need to ``import httpx`` to handle them. The original
    exception is preserved as ``__cause__``.
    """


class TektiiProtocolError(TektiiError):
    """Raised when the gateway returns a response the SDK cannot parse.

    Covers unexpected content-types, malformed JSON bodies, and responses
    that exceed the SDK's safety cap on body size. Distinct from
    ``TektiiAPIError`` (which represents a valid error response from the
    gateway) because the wire protocol itself was violated.
    """

    def __init__(
        self,
        status_code: int,
        method: str,
        path: str,
        message: str,
    ) -> None:
        self.status_code = status_code
        self.method = method
        self.path = path
        self.message = message
        super().__init__(f"[{status_code}] {method} {path}: {message}")


class TektiiAPIError(TektiiError):
    """Raised when the gateway returns an error HTTP response."""

    def __init__(
        self,
        status_code: int,
        code: str,
        message: str,
        details: Any = None,
    ) -> None:
        self.status_code = status_code
        self.code = code
        self.message = message
        self.details = details
        super().__init__(f"[{status_code}] {code}: {message}")


class BadRequestError(TektiiAPIError):
    """400 — INVALID_REQUEST, malformed body, missing parameters."""


class AuthenticationError(TektiiAPIError):
    """401 — UNAUTHORIZED."""


class NotFoundError(TektiiAPIError):
    """404 — ORDER_NOT_FOUND, POSITION_NOT_FOUND, SYMBOL_NOT_FOUND."""


class ConflictError(TektiiAPIError):
    """409 — ORDER_NOT_MODIFIABLE, RESET_COOLDOWN."""


class OrderRejectedError(TektiiAPIError):
    """422 — ORDER_REJECTED."""


class RateLimitedError(TektiiAPIError):
    """429 — RATE_LIMITED."""


class ServerError(TektiiAPIError):
    """500 — INTERNAL_ERROR. Unexpected failure inside the gateway."""


class ProviderUnavailableError(TektiiAPIError):
    """503 — PROVIDER_UNAVAILABLE, SHUTTING_DOWN."""


# Maps HTTP status code to exception subclass.
# Falls back to ``TektiiAPIError`` for unmapped status codes.
_ERROR_MAP: dict[int, type[TektiiAPIError]] = {
    400: BadRequestError,
    401: AuthenticationError,
    404: NotFoundError,
    409: ConflictError,
    422: OrderRejectedError,
    429: RateLimitedError,
    500: ServerError,
    503: ProviderUnavailableError,
}


def raise_for_status(status_code: int, code: str, message: str, details: Any = None) -> NoReturn:
    """Raise the appropriate ``TektiiAPIError`` subclass for a gateway error."""
    cls = _ERROR_MAP.get(status_code, TektiiAPIError)
    raise cls(status_code, code, message, details)
