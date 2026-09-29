# booth-lakehouse's API process (src/booth_lakehouse_server): the authorizing Iceberg REST proxy and
# warehouse management. Lakekeeper itself runs from its upstream image, pinned by digest in the chart.
FROM python:3.13-slim@sha256:7c61056e61ac89e852de05f3dc6fa51a6dd2181797bceed46aa725dd7cb2cd3b

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app

# Dependencies first, so a source-only change rebuilds one small layer.
COPY client/pyproject.toml client/pyproject.toml
COPY pyproject.toml pyproject.toml
RUN mkdir -p client/src/booth_lakehouse src/booth_lakehouse_server \
 && touch client/src/booth_lakehouse/__init__.py src/booth_lakehouse_server/__init__.py \
 && pip install ./client . \
 && pip uninstall -y booth-lakehouse booth-lakehouse-client

COPY client client
COPY src src
RUN pip install --no-deps ./client . && rm -rf client src

USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["booth-lakehouse"]
