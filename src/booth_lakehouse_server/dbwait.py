"""Retry a database's first connection at process startup (ARCHITECTURE.md item 66 (c)).

Found 2026-10-12 on the homelab: a freshly scheduled pod's first connections to PostgreSQL are
refused for about 3 seconds, most likely while k3s's kube-router adds the new pod's IP to the
NetworkPolicy allow sets. Without this, the API exits on that first refusal and the container
restarts once. Retrying for about a minute rides the window out. A server still unreachable after
that long is a real outage, so the last error is raised and the process fails loudly.

A copy of booth-core's ``internal/dbwait`` pattern, not an import (the fleet ruling in item 66 (c)):
same schedule (first retry after 500 ms, doubling to 8 s, 60 s in all), every retry logged. One
addition the Go version leaves to its callers: only connection-level failures are retried. A wrong
password, a missing database or an SQL error fails at once, because waiting cannot fix it.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from typing import TypeVar

log = logging.getLogger("booth_lakehouse")

T = TypeVar("T")

DEFAULT_MAX_ELAPSED = 60.0
INITIAL_BACKOFF = 0.5
MAX_BACKOFF = 8.0

# Never retried, even when the text also mentions a transient cause (libpq joins the message of
# every address it tried): waiting doesn't make a password right or a database exist.
_PERMANENT = ("password authentication failed", "no pg_hba.conf entry", "does not exist")
# Connection-level failures worth waiting out: refused or unanswered while the pod's IP isn't yet
# allowed, a Service name not yet resolvable, a server still starting.
_TRANSIENT = (
    "connection refused",
    "timeout expired",
    "timed out",
    "could not translate host name",
    "failed to resolve host",  # psycopg's own resolver (psycopg 3 resolves names itself)
    "name or service not known",
    "temporary failure in name resolution",
    "no route to host",
    "network is unreachable",
    "the database system is starting up",
    "server closed the connection unexpectedly",
    "connection reset",
)
_TRANSIENT_SQLSTATES = {"57P03"}  # cannot_connect_now: "the database system is starting up"


class DatabaseUnreachable(RuntimeError):
    """The bound ran out: still failing after every retry. Fatal at startup."""


class Cancelled(RuntimeError):
    """Startup was asked to stop while waiting between attempts."""


def is_transient(err: BaseException) -> bool:
    """A connection-level PostgreSQL failure (refused, timeout, DNS, "starting up"). Everything
    else is permanent: auth failures, a missing database, SQL errors, and any non-psycopg error."""
    import psycopg

    if not isinstance(err, psycopg.OperationalError):
        return False  # SQL errors are ProgrammingError, DataError, IntegrityError, ...
    if isinstance(err, psycopg.errors.ConnectionTimeout):
        return True
    state = getattr(err, "sqlstate", None)
    if state:
        return state in _TRANSIENT_SQLSTATES or state.startswith("08")  # class 08: connection exception
    text = str(err).lower()
    if any(p in text for p in _PERMANENT):
        return False
    return any(t in text for t in _TRANSIENT)


def retry(
    what: str,
    fn: Callable[[], T],
    *,
    max_elapsed: float = DEFAULT_MAX_ELAPSED,
    retryable: Callable[[BaseException], bool] = is_transient,
    stop: threading.Event | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> T:
    """Call ``fn`` (one connection attempt, or one connection-dependent operation) until it
    succeeds, retrying a ``retryable`` error with exponential backoff (500 ms doubling to 8 s)
    until ``max_elapsed`` has passed since the first attempt. A non-retryable error is raised at
    once, unchanged. Setting ``stop`` ends a wait early with :class:`Cancelled`."""
    stop = stop or threading.Event()
    deadline = clock() + max_elapsed
    backoff = INITIAL_BACKOFF
    attempt = 0
    while True:
        attempt += 1
        try:
            return fn()
        except Exception as e:
            if not retryable(e):
                raise
            if clock() >= deadline:
                raise DatabaseUnreachable(f"{what}: still failing after {max_elapsed:g}s ({attempt} attempts): {e}") from e
            log.warning("%s: not ready yet (attempt %d: %s); retrying in %gs", what, attempt, e, backoff)
            if stop.wait(backoff):
                raise Cancelled(f"{what}: stopped while waiting to retry ({attempt} attempts made)") from e
            backoff = min(backoff * 2, MAX_BACKOFF)
