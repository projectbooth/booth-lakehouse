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
    # ADR 0088: the broker caps a request at 300 s; a provider can grant longer (MinIO: >= 15 min).
    # The margin must stay well under the shortest grant, or every pass renews.
    warehouse_ttl_seconds: int = 300
    renew_margin_seconds: int = 120
    renew_interval_seconds: int = 60
    events_url: str = ""
    events_creds_file: str = ""
    events_interval_seconds: float = 10
    events_update_min_gap_seconds: float = 60

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
            warehouse_ttl_seconds=int(env.get("WAREHOUSE_CREDENTIAL_TTL_SECONDS", "300")),
            renew_margin_seconds=int(env.get("WAREHOUSE_CREDENTIAL_RENEW_MARGIN_SECONDS", "120")),
            renew_interval_seconds=int(env.get("WAREHOUSE_CREDENTIAL_RENEW_INTERVAL_SECONDS", "60")),
            # ADR 0050's booth-event-bus-credentials (url + nats.creds). Empty URL = no table.* events.
            events_url=env.get("BOOTH_EVENTS_URL", ""),
            events_creds_file=env.get("BOOTH_EVENTS_CREDS_FILE", ""),
            events_interval_seconds=float(env.get("TABLE_EVENTS_INTERVAL_SECONDS", "10")),
            events_update_min_gap_seconds=float(env.get("TABLE_EVENTS_UPDATE_MIN_GAP_SECONDS", "60")),
        )
