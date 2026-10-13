"""ARCHITECTURE.md item 66 (c): the startup retry rides out a few seconds of refused connections,
gives up loudly after its bound, never waits on an error that waiting can't fix, and stops when asked."""

from __future__ import annotations

import logging
import socket
import threading

import psycopg
import pytest

from booth_lakehouse_server import dbwait
from booth_lakehouse_server.dbwait import Cancelled, DatabaseUnreachable, is_transient, retry

REFUSED = psycopg.OperationalError('connection failed: connection to server at "10.43.0.7", port 5432 failed: Connection refused')


class FakeTime:
    """A clock plus a stop Event whose wait() advances the clock instead of sleeping, recording each delay."""

    def __init__(self, stop_after_waits: int | None = None) -> None:
        self.now = 0.0
        self.delays: list[float] = []
        self._stop_after = stop_after_waits

    def clock(self) -> float:
        return self.now

    def wait(self, delay: float) -> bool:
        if self._stop_after is not None and len(self.delays) >= self._stop_after:
            return True  # cancelled mid-wait
        self.delays.append(delay)
        self.now += delay
        return False


def failing(n: int, err: BaseException = REFUSED):
    calls = {"n": 0}

    def fn():
        calls["n"] += 1
        if calls["n"] <= n:
            raise err
        return "connected"

    return fn, calls


def test_first_attempts_fail_then_it_succeeds_and_every_retry_is_logged(caplog):
    caplog.set_level(logging.WARNING)
    t = FakeTime()
    fn, calls = failing(3)
    assert retry("connecting to PostgreSQL at db:5432", fn, stop=t, clock=t.clock) == "connected"
    assert calls["n"] == 4
    assert t.delays == [0.5, 1, 2]
    msgs = [r.getMessage() for r in caplog.records]
    assert msgs[0] == f"connecting to PostgreSQL at db:5432: not ready yet (attempt 1: {REFUSED}); retrying in 0.5s"
    assert [m.split("(attempt ")[1].split(":")[0] for m in msgs] == ["1", "2", "3"]


def test_never_succeeding_fails_loudly_after_the_bound_with_the_last_error_and_attempt_count():
    t = FakeTime()
    fn, calls = failing(10_000)
    with pytest.raises(DatabaseUnreachable, match=r"still failing after 60s \(12 attempts\): .*Connection refused") as e:
        retry("connecting to PostgreSQL at db:5432", fn, stop=t, clock=t.clock)
    assert e.value.__cause__ is REFUSED
    assert t.delays == [0.5, 1, 2, 4, 8, 8, 8, 8, 8, 8, 8]  # doubling, capped at 8 s; the 12th attempt is at 63.5 s
    assert calls["n"] == 12


def test_a_short_bound_override_with_real_time():
    fn, calls = failing(10_000)
    with pytest.raises(DatabaseUnreachable, match=r"still failing after 0.1s \(2 attempts\)"):
        retry("db", fn, max_elapsed=0.1)
    assert calls["n"] == 2  # one 0.5 s wait outlasts the bound


@pytest.mark.parametrize("err", [
    psycopg.OperationalError('connection to server at "db", port 5432 failed: FATAL:  password authentication failed for user "lakehouse"'),
    psycopg.OperationalError('connection to server at "db", port 5432 failed: FATAL:  database "booth_mod_lakehouse" does not exist'),
    psycopg.OperationalError('connection to server at "db" failed: FATAL:  no pg_hba.conf entry for host "10.42.0.9"'),
    psycopg.errors.lookup("28P01")("password authentication failed"),
    psycopg.errors.lookup("3D000")("database does not exist"),
    psycopg.errors.lookup("42601")("syntax error at or near \"CREAT\""),
    ValueError("not a database error at all"),
])
def test_a_non_retryable_error_fails_at_once(err):
    t = FakeTime()
    fn, calls = failing(10_000, err)
    with pytest.raises(type(err)) as e:
        retry("db", fn, stop=t, clock=t.clock)
    assert e.value is err and calls["n"] == 1 and t.delays == []


def test_cancelled_mid_wait():
    t = FakeTime(stop_after_waits=2)
    fn, calls = failing(10_000)
    with pytest.raises(Cancelled, match=r"stopped while waiting to retry \(3 attempts made\)"):
        retry("db", fn, stop=t, clock=t.clock)
    assert calls["n"] == 3 and t.delays == [0.5, 1]


def test_cancelled_with_a_real_event_ends_the_wait_early():
    stop = threading.Event()
    threading.Timer(0.05, stop.set).start()
    fn, _ = failing(10_000)
    with pytest.raises(Cancelled):
        retry("db", fn, stop=stop)  # would otherwise wait 0.5 s, 1 s, ... for a minute


@pytest.mark.parametrize("err", [
    REFUSED,
    psycopg.OperationalError("connection failed: timeout expired"),
    psycopg.errors.ConnectionTimeout("connection timeout expired"),
    psycopg.OperationalError("failed to resolve host 'booth-core-postgresql': [Errno -2] Name or service not known"),
    psycopg.OperationalError('could not translate host name "booth-core-postgresql" to address: Name or service not known'),
    psycopg.OperationalError("FATAL:  the database system is starting up"),
    psycopg.errors.lookup("57P03")("the database system is starting up"),
    psycopg.errors.lookup("08006")("connection failure"),
])
def test_connection_level_failures_are_transient(err):
    assert is_transient(err)


def test_a_real_refused_connection_is_retried_until_the_bound():
    """Against a real closed port: psycopg's own error for a refusal is classified as transient."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]  # bound but not listening: connections are refused
        with pytest.raises(DatabaseUnreachable, match=r"\(1 attempts\)") as e:
            retry("db", lambda: psycopg.connect(f"host=127.0.0.1 port={port} dbname=x user=x connect_timeout=2"), max_elapsed=0)
    assert is_transient(e.value.__cause__)


def test_the_schedule_matches_booth_cores_dbwait():
    assert (dbwait.INITIAL_BACKOFF, dbwait.MAX_BACKOFF, dbwait.DEFAULT_MAX_ELAPSED) == (0.5, 8.0, 60.0)


def test_the_postgres_store_retries_its_first_connection(monkeypatch, caplog):
    """The wiring: PostgresStore's startup (schema creation) goes through the retry, and the log
    names the server but never the password."""
    from booth_lakehouse_server.store import PostgresStore

    executed = []

    class Conn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, sql):
            executed.append(sql)

    attempts = {"n": 0}

    def connect(dsn, autocommit):
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise REFUSED
        return Conn()

    monkeypatch.setattr(psycopg, "connect", connect)
    caplog.set_level(logging.WARNING)
    PostgresStore("postgresql://lakehouse:s3cret@booth-core-postgresql.booth-system.svc:5432/booth_mod_lakehouse")
    assert attempts["n"] == 2 and len(executed) == 1 and "CREATE SCHEMA" in executed[0].upper()
    (msg,) = [r.getMessage() for r in caplog.records]
    assert msg.startswith("connecting to PostgreSQL at booth-core-postgresql.booth-system.svc:5432: not ready yet (attempt 1:")
    assert "s3cret" not in msg
