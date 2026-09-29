"""PyIceberg authentication for the platform gateway: a fresh bearer token on every catalog request.

PyIceberg's built-in ``token`` property is a fixed string, but a platform token is short-lived (a
notebook's or pipeline run's workload token lasts minutes, ADR 0056) — a long session would start
401ing halfway through. This asks the caller's token source each time instead; the source is
expected to cache and refresh (booth-notebooks' ``booth`` client already does).
"""

from __future__ import annotations

from collections.abc import Callable

from pyiceberg.catalog.rest.auth import AuthManager


class GatewayTokenAuth(AuthManager):
    def __init__(self, token: Callable[[], str]) -> None:
        self._token = token

    def auth_header(self) -> str:
        return "Bearer " + self._token()
