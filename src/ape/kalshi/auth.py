from __future__ import annotations

import base64
import time
from collections.abc import Callable

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from ape.kalshi.errors import KalshiAuthError, KalshiConfigurationError

KalshiSigner = Callable[[str, str], str]


def normalize_private_key_pem(raw_value: str) -> str:
    return raw_value.strip().replace("\\n", "\n")


def strip_query_for_signature(request_path: str) -> str:
    return request_path.split("?", 1)[0]


def current_timestamp_ms() -> int:
    return int(time.time() * 1000)


def build_signature_payload(timestamp_ms: int, method: str, request_path: str) -> str:
    return f"{timestamp_ms}{method.upper()}{strip_query_for_signature(request_path)}"


def load_private_key(private_key_pem: str) -> rsa.RSAPrivateKey:
    normalized = normalize_private_key_pem(private_key_pem).encode("utf-8")
    try:
        key = serialization.load_pem_private_key(
            normalized,
            password=None,
            backend=default_backend(),
        )
    except Exception as exc:
        raise KalshiAuthError("Kalshi private key is not parseable.") from exc

    if not isinstance(key, rsa.RSAPrivateKey):
        raise KalshiAuthError("Kalshi private key must be an RSA private key.")

    return key


def private_key_parseable(private_key_pem: str | None) -> bool:
    if not private_key_pem:
        return False

    try:
        load_private_key(private_key_pem)
    except KalshiAuthError:
        return False

    return True


def sign_payload(private_key_pem: str, payload: str) -> str:
    private_key = load_private_key(private_key_pem)
    try:
        signature = private_key.sign(
            payload.encode("utf-8"),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
    except InvalidSignature as exc:
        raise KalshiAuthError("Kalshi RSA-PSS signing failed.") from exc

    return base64.b64encode(signature).decode("utf-8")


def create_auth_headers(
    *,
    api_key_id: str | None,
    private_key_pem: str | None,
    method: str,
    request_path: str,
    timestamp_ms: int | None = None,
    signer: KalshiSigner = sign_payload,
) -> dict[str, str]:
    if not api_key_id or not private_key_pem:
        raise KalshiConfigurationError("Kalshi API key id and private key are required.")

    timestamp = timestamp_ms if timestamp_ms is not None else current_timestamp_ms()
    normalized_key = normalize_private_key_pem(private_key_pem)
    payload = build_signature_payload(timestamp, method, request_path)
    signature = signer(normalized_key, payload)

    return {
        "KALSHI-ACCESS-KEY": api_key_id,
        "KALSHI-ACCESS-TIMESTAMP": str(timestamp),
        "KALSHI-ACCESS-SIGNATURE": signature,
    }

