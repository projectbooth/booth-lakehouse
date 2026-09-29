"""Everything the API process needs, from the environment the chart sets. Nothing here has a
hardcoded storage credential: storage access only ever comes from the ADR 0080 broker."""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from .identity import TrustedIssuer


@dataclass(frozen=True)
class Settings:
    lakekeeper_url: str
    database_dsn: str = ""
    issuers: list[TrustedIssuer] = field(default_factory=list)
    groups_claim: str = "groups"
    broker_url: str = ""
    workload_mint_url: str = ""
    workload_mint_credential: str = field(default="", repr=False)
    warehouse_ttl_seconds: int = 3600
    renew_margin_seconds: int = 900
    renew_interval_seconds: int = 60

    @classmethod
    def from_env(cls, env=None) -> Settings:
        env = os.environ if env is None else env
        issuers = []
        if env.get("BOOTH_OIDC_ISSUER_URL"):
            issuers.append(TrustedIssuer(env["BOOTH_OIDC_ISSUER_URL"], env.get("BOOTH_OIDC_AUDIENCE", "")))
        if env.get("BOOTH_WORKLOAD_ISSUER_URL"):
            # Core's workload tokens (ADR 0056): notebooks' kernels and pipeline tasks.
            issuers.append(TrustedIssuer(env["BOOTH_WORKLOAD_ISSUER_URL"], env.get("BOOTH_WORKLOAD_AUDIENCE", "")))
        if not env.get("LAKEKEEPER_URL"):
            raise ValueError("LAKEKEEPER_URL is required")
        return cls(
            lakekeeper_url=env["LAKEKEEPER_URL"],
            database_dsn=env.get("DATABASE_DSN", ""),
            issuers=issuers,
            groups_claim=env.get("BOOTH_GROUPS_CLAIM", "groups"),
            broker_url=env.get("BOOTH_CREDENTIAL_BROKER_URL", ""),
            workload_mint_url=env.get("BOOTH_WORKLOAD_MINT_URL", ""),
            workload_mint_credential=env.get("BOOTH_WORKLOAD_MINT_CREDENTIAL", ""),
            warehouse_ttl_seconds=int(env.get("WAREHOUSE_CREDENTIAL_TTL_SECONDS", "3600")),
            renew_margin_seconds=int(env.get("WAREHOUSE_CREDENTIAL_RENEW_MARGIN_SECONDS", "900")),
            renew_interval_seconds=int(env.get("WAREHOUSE_CREDENTIAL_RENEW_INTERVAL_SECONDS", "60")),
        )
