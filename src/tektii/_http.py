"""Shared HTTP utilities: URL building, auth headers, response handling."""

from __future__ import annotations

from typing import Any
from urllib.parse import urlsplit, urlunsplit

import httpx

from tektii.errors import APIProtocolError, raise_for_status

# Safety cap on response body size. A well-behaved gateway will never send a
# response close to this — the cap exists to prevent a misbehaving or hostile
# server from OOMing the strategy process via an unbounded JSON blob.
MAX_RESPONSE_BYTES = 16 * 1024 * 1024  # 16 MiB


def auth_headers(api_key: str | None) -> dict[str, str]:
    """Build auth headers from an optional API key."""
    if api_key is None:
        return {}
    return {"Authorization": f"Bearer {api_key}"}


def http_to_ws_url(url: str) -> str:
    """Convert an ``http``/``https`` base URL to its ``ws``/``wss`` equivalent.

    Uses ``urllib.parse`` so that only the scheme is rewritten — hostnames or
    paths containing the literal substring ``http`` are left untouched.
    Already-``ws``/``wss`` URLs pass through unchanged.
    """
    parts = urlsplit(url)
    if parts.scheme == "http":
        new_scheme = "ws"
    elif parts.scheme == "https":
        new_scheme = "wss"
    elif parts.scheme in ("ws", "wss"):
        new_scheme = parts.scheme
    else:
        raise ValueError(f"Unsupported base URL scheme: {parts.scheme!r}")
    return urlunsplit((new_scheme, parts.netloc, parts.path, parts.query, parts.fragment))


def _safe_url(request: httpx.Request) -> str:
    """Return a logging-safe representation of a request URL.

    Strips the query string so that client-supplied values (``client_order_id``,
    datetimes, filter params) are never embedded in exception messages — those
    propagate into user logs and observability stacks and have been known to
    carry PII in trading workloads.
    """
    u = request.url
    return f"{u.scheme}://{u.host}{u.path}"


def _is_json_content_type(content_type: str) -> bool:
    """Case-insensitive check for ``application/json`` media type."""
    return content_type.lower().split(";")[0].strip() == "application/json"


def _check_response_size(response: httpx.Response, method: str, path: str) -> None:
    """Reject responses that exceed the SDK's safety cap.

    httpx has already buffered the body by the time we see it, so this is a
    defence against a server that advertised a sane size but then sent more.
    For the advertised case we trust ``Content-Length`` when present.
    """
    content_length = response.headers.get("content-length")
    if content_length is not None:
        try:
            length = int(content_length)
        except ValueError:
            length = 0
        if length > MAX_RESPONSE_BYTES:
            raise APIProtocolError(
                response.status_code,
                method,
                path,
                f"response body size {length} exceeds cap {MAX_RESPONSE_BYTES}",
            )
    # Defence-in-depth: check the actually-received body too.
    if len(response.content) > MAX_RESPONSE_BYTES:
        raise APIProtocolError(
            response.status_code,
            method,
            path,
            f"response body size {len(response.content)} exceeds cap {MAX_RESPONSE_BYTES}",
        )


def handle_response(response: httpx.Response) -> Any:
    """Parse a gateway response, raising APIStatusError on error status codes."""
    method = response.request.method
    safe_url = _safe_url(response.request)
    # Path-only variant for protocol errors that shouldn't leak hostnames.
    path = response.request.url.path or "/"

    _check_response_size(response, method, path)

    content_type = response.headers.get("content-type", "")

    if response.is_success:
        if not _is_json_content_type(content_type):
            raise APIProtocolError(
                response.status_code,
                method,
                path,
                f"Expected JSON response from {method} {safe_url}, got "
                f"content-type={content_type!r}",
            )
        try:
            return response.json()
        except ValueError as err:
            raise APIProtocolError(
                response.status_code,
                method,
                path,
                f"Gateway returned malformed JSON for {method} {safe_url}: {err}",
            ) from err

    # Error response — try to parse the gateway's structured error envelope.
    try:
        body = response.json()
        code = body.get("code", "UNKNOWN")
        message = body.get("message", response.reason_phrase or "Unknown error")
        details = body.get("details")
    except (ValueError, AttributeError):
        code = "UNKNOWN"
        message = response.reason_phrase or f"HTTP {response.status_code}"
        details = None

    raise_for_status(response.status_code, code, message, details)


def build_params(**kwargs: Any) -> dict[str, Any]:
    """Build query params dict, dropping None values."""
    return {k: v for k, v in kwargs.items() if v is not None}
