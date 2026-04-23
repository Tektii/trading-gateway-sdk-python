# Security Policy

## Supported versions

The Tektii Gateway Python SDK follows a rolling-latest support model during
its beta period. Only the most recent minor release receives security fixes.

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |

Once the SDK reaches 1.0.0 this policy will be revised to support at least
the current and previous minor releases.

## Reporting a vulnerability

**Please do not open a public GitHub issue for security vulnerabilities.**

If you discover a security vulnerability in this SDK, report it privately
via one of the following channels:

1. **GitHub Private Vulnerability Reporting** — preferred. Use the "Report
   a vulnerability" button on the repository's
   [Security tab](https://github.com/Tektii/tektii-gateway-sdk-python/security).
2. **Email** — `security@tektii.com`.

When reporting, please include:

- A description of the vulnerability and its impact.
- Steps to reproduce, or a proof-of-concept if available.
- The affected SDK version.
- Any mitigations you've already identified.

## Safe harbour

We will not pursue legal action against researchers who:

- Act in good faith and make a genuine effort to avoid privacy violations,
  destruction of data, or interruption or degradation of our services.
- Report the vulnerability to us privately via one of the channels above
  and give us a reasonable opportunity to address it before any public
  disclosure.
- Do not exploit the vulnerability beyond what is necessary to demonstrate it.

If you're unsure whether a specific test is in scope, email us first and
we'll talk it through.

## Response expectations

- **Acknowledgement**: within 3 business days.
- **Initial assessment**: within 7 business days.
- **Fix timeline**: depends on severity; critical issues are prioritised.

We will credit reporters in release notes if desired, after the fix ships.

## Scope

This policy covers the `tektii-gateway` Python package and its packaging
(PyPI distribution, GitHub Actions workflows).

The Tektii Trading Gateway itself (the Rust daemon this SDK wraps) has its
own security policy at
<https://github.com/Tektii/trading-gateway/blob/main/SECURITY.md>.

## Handling `TektiiAPIError.details`

`TektiiAPIError.details` exposes the gateway's structured error envelope.
Today the Tektii gateway returns opaque, well-defined codes (e.g.
`{"reject_reason": "INSUFFICIENT_MARGIN"}`). When third-party provider
adapters ship (Alpaca, IBKR, Saxo, etc.) they may echo broker-provided
payloads that contain account identifiers, position sizes, or other
sensitive values.

**Treat `details` as potentially sensitive.** Do not blindly forward it
into third-party observability or log sinks without auditing what the
adapter you are using actually puts there. The SDK does not scrub or
redact `details` — it is your application's responsibility to sanitise
error payloads before they leave the process.

## Credentials

The SDK reads the API key from the `TEKTII_API_KEY` environment variable
when none is passed to the constructor, and refuses to transmit it over
plain `http://` to a non-local host. Do not override this check unless you
have audited the network path (private VPN, loopback proxy, etc.) — the
escape hatch is `allow_insecure=True`, and it is there for test doubles,
not production.

## Out of scope

- Vulnerabilities in third-party dependencies (report to the dependency's
  own security contact).
- Issues requiring physical access to a user's machine.
- Denial of service from providing pathological input to a local strategy
  (the strategy process is trusted).
