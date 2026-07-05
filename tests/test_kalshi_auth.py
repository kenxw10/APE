from __future__ import annotations

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from ape.kalshi.auth import (
    build_signature_payload,
    create_auth_headers,
    normalize_private_key_pem,
    private_key_parseable,
    strip_query_for_signature,
)


def test_private_key_newline_normalization_supports_railway_single_line_env() -> None:
    raw = "-----BEGIN PRIVATE KEY-----\\nabc\\n-----END PRIVATE KEY-----"
    expected = "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----"

    assert normalize_private_key_pem(raw) == expected


def test_signature_payload_uses_timestamp_method_and_path_without_query() -> None:
    assert strip_query_for_signature("/trade-api/v2/markets?limit=5") == "/trade-api/v2/markets"
    assert (
        build_signature_payload(123456, "get", "/trade-api/v2/markets?limit=5")
        == "123456GET/trade-api/v2/markets"
    )


def test_auth_headers_are_deterministic_with_mocked_signer() -> None:
    seen: dict[str, str] = {}

    def signer(private_key_pem: str, payload: str) -> str:
        seen["private_key_pem"] = private_key_pem
        seen["payload"] = payload
        return "mock-signature"

    headers = create_auth_headers(
        api_key_id="key-id",
        private_key_pem="line1\\nline2",
        method="GET",
        request_path="/trade-api/v2/exchange/status?ignored=true",
        timestamp_ms=999,
        signer=signer,
    )

    assert headers == {
        "KALSHI-ACCESS-KEY": "key-id",
        "KALSHI-ACCESS-TIMESTAMP": "999",
        "KALSHI-ACCESS-SIGNATURE": "mock-signature",
    }
    assert seen["private_key_pem"] == "line1\nline2"
    assert seen["payload"] == "999GET/trade-api/v2/exchange/status"


def test_private_key_parseable_accepts_escaped_newlines() -> None:
    private_key = _test_private_key_pem()
    escaped_private_key = private_key.replace("\n", "\\n")

    assert private_key_parseable(escaped_private_key) is True
    assert private_key_parseable("not-a-private-key") is False


def _test_private_key_pem() -> str:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")
