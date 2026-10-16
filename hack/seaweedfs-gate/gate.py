"""ADR 0118 verification gates 2 and 3 for booth-lakehouse: Lakekeeper v0.13.6 end to end on SeaweedFS
4.48 with prefix-scoped IAM user keys, and which checksum mode Lakekeeper's S3 client uses.

Keys are minted the way ADR 0118 says booth-storage's seaweedfs provider will: an ephemeral IAM user,
an attached policy limited to the bucket/prefix, and a bare access key (no session token). Expiry is
emulated the way the provider's sweeper will do it: delete the key, the policy and the user.

The warehouse is created and its credential rotated with this module's own Lakekeeper client
(booth_lakehouse_server.lakekeeper), so the storage profile is exactly what the module sends. The
engine (PyIceberg) holds its own separate scoped key, as a real engine holds its own broker grant.
Both Lakekeeper and the engine reach SeaweedFS through the recorder (s3log), told apart by address.
Exit status 0 only if every check passes.
"""

from __future__ import annotations

import base64
import json
import os
import socket
import sys
import time
import uuid
import zlib

import boto3
import httpx
import pyarrow as pa
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.config import Config
from botocore.credentials import Credentials
from botocore.exceptions import ClientError
from pyiceberg.catalog.rest import RestCatalog

from booth_lakehouse.broker import S3Grant
from booth_lakehouse_server.lakekeeper import Lakekeeper, storage_profile, warehouse_name

LAKEKEEPER_URL = os.environ["LAKEKEEPER_URL"]
S3_DIRECT = os.environ["S3_DIRECT"]  # the gate's own admin and checking calls (not recorded)
S3_VIA_LOG = os.environ["S3_VIA_LOG"]  # Lakekeeper and the engine: the recorder in front of SeaweedFS
ADMIN = (os.environ["S3_ADMIN_KEY"], os.environ["S3_ADMIN_SECRET"])
S3_LOG = os.environ["S3_LOG"]
BUCKET, PREFIX, WORKSPACE = "lake", "acme-lake", "acme"
REGION = "us-east-1"

failures: list[str] = []


def check(ok: bool, what: str) -> None:
    print(("PASS " if ok else "FAIL ") + what, flush=True)
    if not ok:
        failures.append(what)


def s3(key: str, secret: str, endpoint: str = S3_DIRECT):
    return boto3.client("s3", endpoint_url=endpoint, aws_access_key_id=key, aws_secret_access_key=secret,
                        region_name=REGION, config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 1}))


def iam():
    return boto3.client("iam", endpoint_url=S3_DIRECT, aws_access_key_id=ADMIN[0], aws_secret_access_key=ADMIN[1], region_name=REGION)


def policy(prefix: str) -> dict:
    # Two statements: an s3:prefix condition on object actions evaluates false and denies everything
    # (booth-storage spike b3c7863, section 6). SeaweedFS 4.48 rejects s3:ListMultipartUploadParts as
    # "not a valid action" (MalformedPolicyDocument); Lakekeeper never lists parts, so it is left out.
    return {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow",
             "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"],
             "Resource": [f"arn:aws:s3:::{BUCKET}/{prefix}/*"]},
            {"Effect": "Allow", "Action": ["s3:ListBucket"], "Resource": [f"arn:aws:s3:::{BUCKET}"],
             "Condition": {"StringLike": {"s3:prefix": [f"{prefix}/*"]}}},
        ],
    }


def mint(prefix: str, label: str) -> tuple[str, S3Grant]:
    """CreateUser, PutUserPolicy, CreateAccessKey: a bare pair, no session token."""
    user = f"booth-{label}-{uuid.uuid4().hex[:8]}"
    c = iam()
    c.create_user(UserName=user)
    c.put_user_policy(UserName=user, PolicyName="booth-scope", PolicyDocument=json.dumps(policy(prefix)))
    key = c.create_access_key(UserName=user)["AccessKey"]
    assert "SessionToken" not in key
    grant = S3Grant(
        lease_id=user, backend_id="lake", path=prefix, access="readwrite", expires_at=time.time() + 900,
        endpoint=S3_VIA_LOG, region=REGION, bucket=BUCKET, key_prefix=prefix, path_style=True,
        access_key_id=key["AccessKeyId"], secret_access_key=key["SecretAccessKey"],
    )
    return user, grant


def expire(user: str, grant: S3Grant) -> None:
    """What the provider's sweeper does when a lease ends."""
    c = iam()
    c.delete_access_key(UserName=user, AccessKeyId=grant.access_key_id)
    c.delete_user_policy(UserName=user, PolicyName="booth-scope")
    c.delete_user(UserName=user)


def refused(fn) -> str:
    try:
        fn()
    except ClientError as e:
        return e.response["Error"]["Code"]
    return ""


def log_lines() -> list[dict]:
    try:
        with open(S3_LOG) as f:
            return [json.loads(line) for line in f if line.strip()]
    except FileNotFoundError:
        return []


def wait_log(n_before: int, until, timeout: float = 15) -> list[dict]:
    """Log lines after n_before, once `until` holds (the recorder writes after each response)."""
    deadline = time.time() + timeout
    while True:
        new = log_lines()[n_before:]
        if until(new) or time.time() > deadline:
            return new
        time.sleep(0.5)


def probe_ops(lines: list[dict], lk_ip: str) -> dict:
    """Lakekeeper's credential probe (service/storage/mod.rs): PUT .../test, read it, DELETE it, then
    remove_all on the scratch location (a list; nothing is left, so no batch delete) and an empty
    check (a second list)."""
    lk = [r for r in lines if r["client"] == lk_ip]
    return {
        "put_test": [r["status"] for r in lk if r["method"] == "PUT" and r["path"].endswith("/test")],
        "get_test": [r["status"] for r in lk if r["method"] == "GET" and r["path"].endswith("/test")],
        "delete_test": [r["status"] for r in lk if r["method"] == "DELETE" and r["path"].endswith("/test")],
        "list": [r["status"] for r in lk if r["method"] == "GET" and "list-type" in r["query"]],
    }


def probe_ok(p: dict) -> bool:
    return p["put_test"] == [200] and p["get_test"] in ([200], [206]) and p["delete_test"] == [204] and p["list"] == [200, 200]


def probe_done(lk_ip: str):
    return lambda new: sum(1 for r in new if r["client"] == lk_ip and "list-type" in r["query"]) >= 2


def trailer_mode(h: dict) -> bool:
    return ("aws-chunked" in h.get("content-encoding", "") or "x-amz-trailer" in h
            or h.get("x-amz-content-sha256", "").startswith("STREAMING-"))


def report_modes(who: str, lines: list[dict]) -> None:
    modes: dict[str, dict] = {}
    for r in [r for r in lines if r["method"] in ("PUT", "POST")]:
        op = "DeleteObjects" if "delete" in r["query"] else ("UploadPart" if "partNumber" in r["query"] else r["method"])
        h = r["headers"]
        m = modes.setdefault(op, {"count": 0, "trailer": 0, "header": 0, "statuses": set(), "example": None})
        m["count"] += 1
        m["trailer" if trailer_mode(h) else "header"] += 1
        m["statuses"].add(r["status"])
        if m["example"] is None:
            m["example"] = {k: v for k, v in h.items() if k != "user-agent"}
    print(f"{who} user-agent:", next((r["headers"].get("user-agent", "") for r in lines), ""), flush=True)
    for op, m in modes.items():
        print(f"{who} checksums {op}: {m['count']} requests, trailer={m['trailer']} header={m['header']} statuses={sorted(m['statuses'])}", flush=True)
        print(f"  example headers: {json.dumps(m['example'])}", flush=True)


def crc32(b: bytes) -> str:
    return base64.b64encode(zlib.crc32(b).to_bytes(4, "big")).decode()


class _TrailerSigV4(S3SigV4Auth):
    """botocore otherwise replaces x-amz-content-sha256 with the body's hash, which tells the server
    the body is NOT aws-chunked (the server then stores the framing as content)."""

    def payload(self, request):
        return "STREAMING-UNSIGNED-PAYLOAD-TRAILER"


def send_trailer(grant: S3Grant, method: str, key: str, payload: bytes, query: str = "", checksum: str | None = None) -> httpx.Response:
    """One genuine aws-chunked request with a trailing CRC32 (the shape newer AWS SDKs send for a
    streaming body), SigV4-signed with the minted key, through the recorder."""
    trailer = f"x-amz-checksum-crc32:{checksum or crc32(payload)}"
    body = f"{len(payload):x}\r\n".encode() + payload + b"\r\n0\r\n" + trailer.encode() + b"\r\n\r\n"
    url = f"{S3_VIA_LOG}/{BUCKET}/{key}" + (f"?{query}" if query else "")
    req = AWSRequest(method=method, url=url, data=body, headers={
        "Content-Encoding": "aws-chunked",
        "X-Amz-Content-SHA256": "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
        "X-Amz-Decoded-Content-Length": str(len(payload)),
        "X-Amz-Trailer": "x-amz-checksum-crc32",
        "X-Amz-Sdk-Checksum-Algorithm": "CRC32",
        "Content-Length": str(len(body)),
    })
    _TrailerSigV4(Credentials(grant.access_key_id, grant.secret_access_key), "s3", REGION).add_auth(req)
    assert req.headers["X-Amz-Content-SHA256"] == "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
    return httpx.request(method, url, content=body, headers=dict(req.headers.items()), timeout=30)


def trailer_checks(grant: S3Grant) -> None:
    """Gate 3, the store's side. A wrong trailer checksum must be refused, or SeaweedFS isn't verifying it."""
    c = s3(grant.access_key_id, grant.secret_access_key)
    payload = b"trailer-mode body " * 1000
    r = send_trailer(grant, "PUT", f"{PREFIX}/trailer-check/a", payload)
    got = c.get_object(Bucket=BUCKET, Key=f"{PREFIX}/trailer-check/a")["Body"].read() if r.status_code == 200 else b""
    check(r.status_code == 200 and got == payload, f"SeaweedFS accepts a PUT with an aws-chunked CRC32 trailer ({r.status_code}); stored body intact")
    r = send_trailer(grant, "PUT", f"{PREFIX}/trailer-check/bad", payload, checksum=crc32(b"something else"))
    check(r.status_code >= 400, f"SeaweedFS refuses a PUT whose trailer checksum is wrong ({r.status_code} {r.text[:120]!r})")
    c.put_object(Bucket=BUCKET, Key=f"{PREFIX}/trailer-check/b", Body=b"b")
    xml = (f"<Delete><Object><Key>{PREFIX}/trailer-check/a</Key></Object>"
           f"<Object><Key>{PREFIX}/trailer-check/b</Key></Object></Delete>").encode()
    r = send_trailer(grant, "POST", "", xml, query="delete=")
    left = c.list_objects_v2(Bucket=BUCKET, Prefix=f"{PREFIX}/trailer-check/").get("KeyCount", 0)
    check(r.status_code == 200 and "<Error>" not in r.text and left == 0,
          f"SeaweedFS accepts DeleteObjects with an aws-chunked CRC32 trailer ({r.status_code}); both objects gone")
    # Recorded, not gated: SeaweedFS 4.48 verifies no checksum on DeleteObjects (AWS requires and checks
    # one). Lakekeeper's own requests carry correct checksums, so this doesn't affect it.
    for mode, extra in (("trailer CRC32", None), ("header CRC32", {"x-amz-sdk-checksum-algorithm": "CRC32", "x-amz-checksum-crc32": crc32(b"nope")}),
                        ("no checksum", {})):
        key = f"{PREFIX}/trailer-check/unverified"
        c.put_object(Bucket=BUCKET, Key=key, Body=b"x")
        body = f"<Delete><Object><Key>{key}</Key></Object></Delete>".encode()
        if extra is None:
            r = send_trailer(grant, "POST", "", body, query="delete=", checksum=crc32(b"nope"))
        else:
            url = f"{S3_VIA_LOG}/{BUCKET}/?delete="
            req = AWSRequest(method="POST", url=url, data=body, headers=extra)
            S3SigV4Auth(Credentials(grant.access_key_id, grant.secret_access_key), "s3", REGION).add_auth(req)
            r = httpx.post(url, content=body, headers=dict(req.headers.items()), timeout=30)
        gone = c.list_objects_v2(Bucket=BUCKET, Prefix=key).get("KeyCount", 0) == 0
        print(f"FINDING DeleteObjects with {'a wrong ' if mode != 'no checksum' else ''}{mode}: HTTP {r.status_code}, "
              f"{'object deleted' if gone else 'object kept'}", flush=True)


def main() -> int:
    lk_ip = socket.gethostbyname("lakekeeper")
    gate_ip = socket.gethostbyname(socket.gethostname())
    admin = s3(*ADMIN)
    admin.create_bucket(Bucket=BUCKET)
    admin.put_object(Bucket=BUCKET, Key="other-tenant/secret.txt", Body=b"not yours")

    # ---- the minted key's scope holds ------------------------------------------------------------
    u1, k1 = mint(PREFIX, "wh")
    c1 = s3(k1.access_key_id, k1.secret_access_key)
    c1.put_object(Bucket=BUCKET, Key=f"{PREFIX}/scope-check", Body=b"x")
    c1.delete_object(Bucket=BUCKET, Key=f"{PREFIX}/scope-check")
    check(refused(lambda: c1.put_object(Bucket=BUCKET, Key="other-tenant/x", Body=b"x")) != "", "minted key: write outside its prefix refused")
    check(refused(lambda: c1.get_object(Bucket=BUCKET, Key="other-tenant/secret.txt")) != "", "minted key: read outside its prefix refused")
    check(refused(lambda: c1.list_objects_v2(Bucket=BUCKET)) != "", "minted key: list at the bucket root refused")

    # ---- 1. create the warehouse (Lakekeeper probes the credential) ------------------------------
    lk = Lakekeeper(LAKEKEEPER_URL)
    lk.ensure_bootstrapped()
    print("warehouse storage profile:", json.dumps(storage_profile(k1)), flush=True)
    n = len(log_lines())
    wh = lk.create_warehouse(warehouse_name(WORKSPACE), k1)
    probe = probe_ops(wait_log(n, probe_done(lk_ip)), lk_ip)
    print("create probe:", probe, flush=True)
    check(probe_ok(probe), "create warehouse: Lakekeeper's probe wrote, read, deleted and listed with the minted key")

    # ---- 2. namespace, table, data and metadata, read back ---------------------------------------
    ue, ke = mint(PREFIX, "engine")  # the engine's own grant, as from the broker
    cat = RestCatalog("gate", **{
        "uri": f"{LAKEKEEPER_URL}/catalog", "warehouse": warehouse_name(WORKSPACE),
        "s3.endpoint": S3_VIA_LOG, "s3.region": REGION, "s3.path-style-access": "true",
        "s3.access-key-id": ke.access_key_id, "s3.secret-access-key": ke.secret_access_key,
    })
    cat.create_namespace("gate")
    schema = pa.schema([("id", pa.int64()), ("amount", pa.int64())])
    n = len(log_lines())
    tbl = cat.create_table("gate.sales", schema=schema)
    tbl.append(pa.table({"id": [1, 2, 3], "amount": [10, 14, 18]}, schema=schema))
    lk_meta = [r for r in wait_log(n, lambda new: sum(1 for r in new if r["client"] == lk_ip and r["method"] == "PUT") >= 2)
               if r["client"] == lk_ip and r["method"] == "PUT"]
    check(len(lk_meta) >= 2 and all(r["status"] == 200 and r["path"].endswith(".metadata.json") for r in lk_meta),
          f"create table + append: Lakekeeper wrote {len(lk_meta)} metadata files itself")
    location = tbl.location()
    print("table location:", location, flush=True)
    check(location.startswith(f"s3://{BUCKET}/{PREFIX}/"), "table location is inside the warehouse prefix")
    rows = cat.load_table("gate.sales").scan().to_arrow()
    check(rows.num_rows == 3 and sum(rows["amount"].to_pylist()) == 42, "read back 3 rows, sum 42")

    # ---- 3. rotation: a fresh pair, the old one expires; the warehouse keeps working -------------
    current_user, current = u1, k1
    for i, extra in enumerate(([4, 5], [6, 7]), start=1):
        new_user, new = mint(PREFIX, "wh")
        n = len(log_lines())
        lk.update_credential(wh.id, new)
        probe = probe_ops(wait_log(n, probe_done(lk_ip)), lk_ip)
        print(f"renewal {i} probe:", probe, flush=True)
        check(probe_ok(probe), f"renewal {i}: Lakekeeper probed the fresh key (write, read, delete, list)")
        expire(current_user, current)
        old = s3(current.access_key_id, current.secret_access_key)
        code = refused(lambda old=old: old.put_object(Bucket=BUCKET, Key=f"{PREFIX}/after-expiry", Body=b"x"))
        check(code != "", f"renewal {i}: the expired key is refused ({code})")
        n = len(log_lines())
        cat.load_table("gate.sales").append(pa.table({"id": extra, "amount": [1, 1]}, schema=schema))
        puts = [r for r in wait_log(n, lambda new_: any(r["client"] == lk_ip and r["method"] == "PUT" for r in new_))
                if r["client"] == lk_ip and r["method"] == "PUT"]
        check(bool(puts) and all(r["status"] == 200 for r in puts), f"renewal {i}: a commit after rotation wrote metadata with the fresh key")
        rows = cat.load_table("gate.sales").scan().to_arrow()
        check(rows.num_rows == 3 + 2 * i, f"renewal {i}: read back {rows.num_rows} rows")
        current_user, current = new_user, new

    # ---- 4. drop with purge ----------------------------------------------------------------------
    table_key = location.removeprefix(f"s3://{BUCKET}/")
    before = admin.list_objects_v2(Bucket=BUCKET, Prefix=table_key + "/").get("KeyCount", 0)
    n = len(log_lines())
    cat.purge_table("gate.sales")
    deadline, left = time.time() + 90, before
    while time.time() < deadline:
        left = admin.list_objects_v2(Bucket=BUCKET, Prefix=table_key + "/").get("KeyCount", 0)
        if left == 0:
            break
        time.sleep(1)
    purge = [r for r in wait_log(n, lambda new: any(r["client"] == lk_ip and "delete" in r["query"] for r in new))
             if r["client"] == lk_ip and r["method"] == "POST" and "delete" in r["query"]]
    check(before > 0 and left == 0, f"purge: Lakekeeper deleted all {before} objects (data, manifests, metadata) under the table")
    check(bool(purge) and all(r["status"] == 200 for r in purge), "purge: Lakekeeper's batch delete (DeleteObjects) succeeded")
    check(admin.get_object(Bucket=BUCKET, Key="other-tenant/secret.txt")["Body"].read() == b"not yours", "purge left other prefixes alone")

    # ---- 5. trailer checksums: what SeaweedFS does with them -------------------------------------
    trailer_checks(current)
    expire(current_user, current)
    expire(ue, ke)

    # ---- 6. checksum modes the real clients used -------------------------------------------------
    lines = log_lines()
    lk_lines = [r for r in lines if r["client"] == lk_ip]
    report_modes("Lakekeeper", lk_lines)
    report_modes("PyIceberg engine (pyarrow)", [r for r in lines if r["client"] == gate_ip and "trailer-check" not in r["path"]
                                                and not (r["method"] == "POST" and "delete" in r["query"] and trailer_mode(r["headers"]))])
    bad = [r for r in lk_lines if r["status"] >= 400]
    for r in bad:
        print("Lakekeeper S3 error:", json.dumps(r), flush=True)
    check(not bad, f"every one of Lakekeeper's {len(lk_lines)} S3 requests succeeded")
    check(not any(trailer_mode(r["headers"]) for r in lk_lines), "Lakekeeper sent no aws-chunked trailer checksums (header mode only)")

    print(f"\n{'GATE PASSED' if not failures else 'GATE FAILED: ' + '; '.join(failures)}", flush=True)
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
