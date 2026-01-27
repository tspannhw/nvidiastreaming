import base64
import hashlib
import time
from dataclasses import dataclass
from typing import Optional

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa


@dataclass
class JwtConfig:
    account_identifier: str
    user: str
    private_key_path: str
    private_key_passphrase: Optional[str] = None
    public_key_fp: Optional[str] = None
    lifetime_seconds: int = 3600


def _load_private_key(path: str, passphrase: Optional[str]) -> rsa.RSAPrivateKey:
    with open(path, "rb") as key_file:
        key_bytes = key_file.read()
    password = passphrase.encode("utf-8") if passphrase else None
    return serialization.load_pem_private_key(key_bytes, password=password)


def _public_key_fingerprint(private_key: rsa.RSAPrivateKey) -> str:
    public_key = private_key.public_key()
    der_bytes = public_key.public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    sha256 = hashlib.sha256(der_bytes).digest()
    fp = base64.b64encode(sha256).decode("utf-8")
    return f"SHA256:{fp}"


def _normalize_identifier(value: str) -> str:
    return value.upper()


def generate_jwt(config: JwtConfig) -> str:
    private_key = _load_private_key(config.private_key_path, config.private_key_passphrase)
    public_key_fp = config.public_key_fp or _public_key_fingerprint(private_key)

    account = _normalize_identifier(config.account_identifier)
    user = _normalize_identifier(config.user)

    now = int(time.time())
    payload = {
        "iss": f"{account}.{user}.{public_key_fp}",
        "sub": f"{account}.{user}",
        "iat": now,
        "exp": now + config.lifetime_seconds,
    }
    return jwt.encode(payload, private_key, algorithm="RS256")
