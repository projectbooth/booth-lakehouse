"""mitmproxy addon: one JSON line per S3 request that passed through, with the headers that say how
the client sent its checksum (header mode vs aws-chunked trailer mode) and how SeaweedFS answered.
Never records Authorization or bodies."""

import json

HEADERS = (
    "content-encoding",
    "content-length",
    "transfer-encoding",
    "content-md5",
    "x-amz-content-sha256",
    "x-amz-decoded-content-length",
    "x-amz-trailer",
    "x-amz-sdk-checksum-algorithm",
    "x-amz-checksum-crc32",
    "x-amz-checksum-crc32c",
    "x-amz-checksum-crc64nvme",
    "x-amz-checksum-sha1",
    "x-amz-checksum-sha256",
    "if-match",
    "if-none-match",
    "range",
    "user-agent",
)


def response(flow):
    req, resp = flow.request, flow.response
    error = ""
    if resp.status_code >= 400:
        error = (resp.get_text(strict=False) or "")[:300]
    line = {
        "client": flow.client_conn.peername[0],
        "method": req.method,
        "path": req.path.split("?")[0],
        "query": sorted(req.query.keys()),
        "headers": {h: req.headers[h] for h in HEADERS if h in req.headers},
        "status": resp.status_code,
        "error": error,
    }
    with open("/log/s3.jsonl", "a") as f:
        f.write(json.dumps(line) + "\n")
