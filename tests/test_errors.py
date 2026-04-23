"""Tests for error hierarchy and status code mapping."""

import pytest

from tektii_gateway.errors import (
    AuthenticationError,
    BadRequestError,
    ConflictError,
    NotFoundError,
    OrderRejectedError,
    ProviderUnavailableError,
    RateLimitedError,
    ServerError,
    TektiiAPIError,
    TektiiError,
    raise_for_status,
)


def test_error_hierarchy() -> None:
    assert issubclass(TektiiAPIError, TektiiError)
    assert issubclass(BadRequestError, TektiiAPIError)
    assert issubclass(NotFoundError, TektiiAPIError)
    assert issubclass(OrderRejectedError, TektiiAPIError)
    assert issubclass(AuthenticationError, TektiiAPIError)
    assert issubclass(RateLimitedError, TektiiAPIError)
    assert issubclass(ConflictError, TektiiAPIError)
    assert issubclass(ProviderUnavailableError, TektiiAPIError)
    assert issubclass(ServerError, TektiiAPIError)


def test_api_error_attributes() -> None:
    err = TektiiAPIError(400, "INVALID_REQUEST", "Bad input", {"field": "quantity"})
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
    """Unmapped status codes fall through to base TektiiAPIError."""
    with pytest.raises(TektiiAPIError) as exc_info:
        raise_for_status(418, "IM_A_TEAPOT", "teapot")
    assert type(exc_info.value) is TektiiAPIError


def test_catch_all_with_base_class() -> None:
    """Users can catch TektiiError to handle any SDK error."""
    with pytest.raises(TektiiError):
        raise_for_status(404, "ORDER_NOT_FOUND", "Not found")
