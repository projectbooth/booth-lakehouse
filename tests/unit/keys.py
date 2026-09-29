"""Test signing keys and token factories: real RSA signatures, verified by the real code path."""

from __future__ import annotations

import time
from dataclasses import dataclass, field

import jwt
from cryptography.hazmat.primitives.asymmetric import rsa

from booth_lakehouse_server.identity import KeySource, TrustedIssuer, Verifier

IDP = "https://keycloak.example.test/realms/booth"
CORE = "https://core.example.test/workload"


@dataclass
class Signer:
    issuer: str
    key: rsa.RSAPrivateKey = field(default_factory=lambda: rsa.generate_private_key(public_exponent=65537, key_size=2048))

    def token(self, sub="user-1", groups=("/workspaces/acme/editor",), ttl=300, alg="RS256", **extra) -> str:
        now = int(time.time())
        claims = {"iss": self.issuer, "sub": sub, "iat": now, "exp": now + ttl, "groups": list(groups), **extra}
        return jwt.encode(claims, self.key, algorithm=alg)


class StaticKeys(KeySource):
    def __init__(self, *signers: Signer) -> None:
        self._keys = {s.issuer: s.key.public_key() for s in signers}

    def signing_key(self, issuer_url, raw_token):
        return self._keys[issuer_url]


def verifier(*signers: Signer) -> Verifier:
    return Verifier([TrustedIssuer(s.issuer) for s in signers], "groups", StaticKeys(*signers))
