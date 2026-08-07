"""Tests for error hierarchy and status code mapping."""

import pytest

import tektii
from tektii.errors import (
    APIStatusError,
    AuthenticationError,
    BadRequestError,
    ConflictError,
    NotFoundError,
    OrderRejectedError,
    PositionUnprotectedError,
    ProviderUnavailableError,
    RateLimitedError,
    ServerError,
    TektiiError,
    UnsupportedOrderTypeError,
    raise_for_status,
)


def test_error_hierarchy() -> None:
    assert issubclass(APIStatusError, TektiiError)
    assert issubclass(BadRequestError, APIStatusError)
    assert issubclass(NotFoundError, APIStatusError)
    assert issubclass(OrderRejectedError, APIStatusError)
    assert issubclass(AuthenticationError, APIStatusError)
    assert issubclass(RateLimitedError, APIStatusError)
    assert issubclass(ConflictError, APIStatusError)
    assert issubclass(ProviderUnavailableError, APIStatusError)
    assert issubclass(ServerError, APIStatusError)
    assert issubclass(PositionUnprotectedError, APIStatusError)


def test_unsupported_order_type_is_a_client_side_error() -> None:
    """Raised before any request, so it is not an ``APIStatusError``."""
    assert issubclass(UnsupportedOrderTypeError, TektiiError)
    assert not issubclass(UnsupportedOrderTypeError, APIStatusError)


def test_unsupported_order_type_carries_the_alternatives() -> None:
    err = UnsupportedOrderTypeError("trailing_stop", ["MARKET", "LIMIT"])
    assert err.order_type == "trailing_stop"
    assert err.supported == ["MARKET", "LIMIT"]
    assert "trailing_stop" in str(err)
    assert "MARKET, LIMIT" in str(err)


def test_unsupported_order_type_with_no_alternatives_reads_sensibly() -> None:
    """A provider reporting an empty set must not render as a dangling list."""
    assert "(none reported)" in str(UnsupportedOrderTypeError("market", []))


def test_unsupported_order_type_is_exported() -> None:
    assert tektii.UnsupportedOrderTypeError is UnsupportedOrderTypeError


def test_api_error_attributes() -> None:
    err = APIStatusError(400, "INVALID_REQUEST", "Bad input", {"field": "quantity"})
    assert err.status_code == 400
    assert err.code == "INVALID_REQUEST"
    assert err.message == "Bad input"
    assert err.details == {"field": "quantity"}
    assert "[400]" in str(err)
    assert "INVALID_REQUEST" in str(err)


def test_raise_for_status_404() -> None:
    with pytest.raises(NotFoundError) as exc_info:
        raise_for_status(404, "ORDER_NOT_FOUND", "Order not found")
    assert exc_info.value.status_code == 404


def test_raise_for_status_422() -> None:
    with pytest.raises(OrderRejectedError):
        raise_for_status(422, "ORDER_REJECTED", "Insufficient margin")


def test_raise_for_status_401() -> None:
    with pytest.raises(AuthenticationError):
        raise_for_status(401, "UNAUTHORIZED", "Invalid API key")


def test_raise_for_status_429() -> None:
    with pytest.raises(RateLimitedError):
        raise_for_status(429, "RATE_LIMITED", "Too many requests")


def test_raise_for_status_409() -> None:
    with pytest.raises(ConflictError):
        raise_for_status(409, "ORDER_NOT_MODIFIABLE", "Order already filled")


def test_raise_for_status_502_is_not_globally_unprotected() -> None:
    """502 only means "position uncovered" on the exit-move endpoint.

    ``modify_position_exits`` promotes it there. A 502 from anywhere else —
    a proxy or load balancer in front of the gateway — must stay generic, or
    callers would be told to flatten a position over unrelated infra noise.
    """
    with pytest.raises(APIStatusError) as exc_info:
        raise_for_status(502, "PROVIDER_ERROR", "Bad gateway")
    assert type(exc_info.value) is APIStatusError


def test_raise_for_status_503() -> None:
    with pytest.raises(ProviderUnavailableError):
        raise_for_status(503, "PROVIDER_UNAVAILABLE", "Broker down")


def test_raise_for_status_400() -> None:
    with pytest.raises(BadRequestError):
        raise_for_status(400, "INVALID_REQUEST", "Malformed body")


def test_raise_for_status_500() -> None:
    with pytest.raises(ServerError):
        raise_for_status(500, "INTERNAL_ERROR", "Internal server error")


def test_raise_for_status_unknown_maps_to_base() -> None:
    """Unmapped status codes fall through to base APIStatusError."""
    with pytest.raises(APIStatusError) as exc_info:
        raise_for_status(418, "IM_A_TEAPOT", "teapot")
    assert type(exc_info.value) is APIStatusError


def test_catch_all_with_base_class() -> None:
    """Users can catch TektiiError to handle any SDK error."""
    with pytest.raises(TektiiError):
        raise_for_status(404, "ORDER_NOT_FOUND", "Not found")
