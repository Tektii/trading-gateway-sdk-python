"""Exception hierarchy for the Trading Gateway SDK."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, NoReturn


class TektiiError(Exception):
    """Base exception for all SDK errors."""


class APIConnectionError(TektiiError):
    """Raised on network / transport failures talking to the gateway.

    Wraps the underlying ``httpx`` exception (timeout, DNS failure, connection
    refused, read error, pool exhaustion) behind the SDK's own error surface
    so users never need to ``import httpx`` to handle them. The original
    exception is preserved as ``__cause__``.
    """


class APIProtocolError(TektiiError):
    """Raised when the gateway returns a response the SDK cannot parse.

    Covers unexpected content-types, malformed JSON bodies, and responses
    that exceed the SDK's safety cap on body size. Distinct from
    ``APIStatusError`` (which represents a valid error response from the
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


class APIStatusError(TektiiError):
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


class BadRequestError(APIStatusError):
    """400 — INVALID_REQUEST, malformed body, missing parameters."""


class AuthenticationError(APIStatusError):
    """401 — UNAUTHORIZED."""


class NotFoundError(APIStatusError):
    """404 — ORDER_NOT_FOUND, POSITION_NOT_FOUND, SYMBOL_NOT_FOUND."""


class ConflictError(APIStatusError):
    """409 — ORDER_NOT_MODIFIABLE, RESET_COOLDOWN."""


class OrderRejectedError(APIStatusError):
    """422 — ORDER_REJECTED."""


class RateLimitedError(APIStatusError):
    """429 — RATE_LIMITED."""


class ServerError(APIStatusError):
    """500 — INTERNAL_ERROR. Unexpected failure inside the gateway."""


class PositionUnprotectedError(APIStatusError):
    """502 — an exit leg was cancelled and could not be re-established.

    Raised only by ``modify_position_exits``, which promotes that endpoint's
    502 to this class; a 502 from anywhere else (a proxy, a load balancer)
    stays a plain ``APIStatusError``. See
    :meth:`~tektii.async_client.AsyncTradingGateway.modify_position_exits`
    for what to do about it.
    """


class ProviderUnavailableError(APIStatusError):
    """503 — PROVIDER_UNAVAILABLE, SHUTTING_DOWN."""


class UnsupportedOrderTypeError(TektiiError):
    """The connected provider does not support the requested order type.

    ``OrderType`` covers every type the gateway's API can express, but a
    given provider implements a subset — the backtest engine, for instance,
    has no ``TRAILING_STOP``. Raised by ``submit_order`` before the order is
    sent, so nothing reaches the provider. ``supported`` carries the types
    that *are* available, so a strategy can fall back to one of them.
    """

    def __init__(self, order_type: str, supported: Sequence[str]) -> None:
        self.order_type = order_type
        self.supported = list(supported)
        super().__init__(
            f"Order type {order_type!r} is not supported by the connected provider. "
            f"Supported types: {', '.join(self.supported) or '(none reported)'}."
        )


# Maps HTTP status code to exception subclass.
# Falls back to ``APIStatusError`` for unmapped status codes.
_ERROR_MAP: dict[int, type[APIStatusError]] = {
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
    """Raise the appropriate ``APIStatusError`` subclass for a gateway error."""
    cls = _ERROR_MAP.get(status_code, APIStatusError)
    raise cls(status_code, code, message, details)
