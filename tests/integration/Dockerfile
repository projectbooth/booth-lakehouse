# The integration test runner and fakecore image (test tooling only). Runs on the compose network so
# every service reaches every other under one name — the same as in-cluster — which the host can't
# do on every platform (Lakekeeper and the client must see MinIO at the same endpoint).
FROM python:3.13-slim@sha256:7c61056e61ac89e852de05f3dc6fa51a6dd2181797bceed46aa725dd7cb2cd3b
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /src
COPY client client
COPY pyproject.toml pyproject.toml
COPY src src
RUN pip install ./client ".[dev]" boto3
# Baked in for the kind run (a Job has no bind mount); compose mounts ./tests over it for fast iteration.
COPY tests tests
