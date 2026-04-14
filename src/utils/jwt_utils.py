import base64
import json
import time
from typing import Any, Dict


class JWTDecodeError(ValueError):
    pass


def extract_bearer_token(authorization_header: str | None) -> str:
    if not authorization_header:
        raise JWTDecodeError("Authorization header is required")

    scheme, _, token = authorization_header.partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise JWTDecodeError("Authorization header must be a Bearer token")

    return token.strip()


def decode_jwt_payload(token: str) -> Dict[str, Any]:
    parts = token.split(".")
    if len(parts) != 3:
        raise JWTDecodeError("JWT must contain header, payload, and signature")

    payload = parts[1]
    padding = "=" * (-len(payload) % 4)

    try:
        decoded_payload = base64.urlsafe_b64decode(payload + padding)
        claims = json.loads(decoded_payload.decode("utf-8"))
    except (ValueError, json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise JWTDecodeError("JWT payload could not be decoded") from exc

    exp = claims.get("exp")
    if isinstance(exp, (int, float)) and exp < time.time():
        raise JWTDecodeError("JWT token has expired")

    return claims
