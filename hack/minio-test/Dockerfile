# ghcr.io/projectbooth/minio-test (ADR 0087, amended by ADR 0091): MinIO server + mc, BUILT FROM SOURCE
# at pinned upstream release tags, for integration tests only. Shared by booth-lakehouse and
# booth-storage, and consumed by digest only.
#
# Why from source: quay.io/minio/minio no longer serves anonymous pulls and both upstream repos are
# archived, so a pre-built image can't be re-fetched; a source build at a frozen tag can always be
# reproduced. The build refuses to proceed unless each tag resolves to the expected commit, and
# refuses the result unless each binary reports the expected release.
#
# Built by .github/workflows/mirror-minio-test.yml; locally: see that file's header.
ARG GO_IMAGE=golang:1.24-alpine@sha256:8bee1901f1e530bfb4a7850aa7a479d17ae3a18beb6e09064ed54cfd245b7191
ARG BASE_IMAGE=alpine:3.22@sha256:5291449c3df73caf6ed85e649dec1b9e818b39a5d8c871e97afc13e9cd5e8fa8

FROM ${GO_IMAGE} AS build
ARG MINIO_TAG
ARG MINIO_COMMIT
ARG MC_TAG
ARG MC_COMMIT
ENV CGO_ENABLED=0 GOFLAGS=-trimpath GOTOOLCHAIN=auto
RUN apk add --no-cache git
WORKDIR /src

# checkout <repo> <tag> <expected commit>: the tag must resolve to exactly that commit.
RUN cat > /usr/local/bin/checkout <<'SH' && chmod +x /usr/local/bin/checkout
#!/bin/sh
set -eu
git clone --quiet --depth 1 --branch "$2" "https://github.com/$1.git" "$(basename "$1")"
got=$(git -C "$(basename "$1")" rev-parse HEAD)
[ "$got" = "$3" ] || { echo "REFUSING: $1 $2 is $got, expected $3" >&2; exit 1; }
echo "$1 $2 = $got"
SH

RUN test -n "$MINIO_TAG" && test -n "$MINIO_COMMIT" && test -n "$MC_TAG" && test -n "$MC_COMMIT"
RUN checkout minio/minio "$MINIO_TAG" "$MINIO_COMMIT" && checkout minio/mc "$MC_TAG" "$MC_COMMIT"

# Upstream's own version stamping (buildscripts/gen-ldflags.go), given the tag's timestamp, so the
# binaries report RELEASE.<timestamp> exactly as upstream's releases do.
RUN cd minio && MINIO_RELEASE=RELEASE go build -tags kqueue \
      -ldflags "$(MINIO_RELEASE=RELEASE go run buildscripts/gen-ldflags.go "${MINIO_TAG#RELEASE.}")" -o /out/minio .
RUN cd mc && MC_RELEASE=RELEASE go build \
      -ldflags "$(MC_RELEASE=RELEASE go run buildscripts/gen-ldflags.go "${MC_TAG#RELEASE.}")" -o /out/mc .

RUN /out/minio --version | tee /dev/stderr | grep -q "version ${MINIO_TAG} " \
 && /out/mc --version | tee /dev/stderr | grep -q "version ${MC_TAG} " \
 || { echo "REFUSING: built binaries don't report the expected releases" >&2; exit 1; }

FROM ${BASE_IMAGE}
ARG MINIO_TAG
ARG MINIO_COMMIT
ARG MC_TAG
ARG MC_COMMIT
# ca-certificates: TLS to other endpoints if a test ever needs it; sh + wget come with alpine.
RUN apk add --no-cache ca-certificates
COPY --from=build /out/minio /out/mc /usr/bin/
LABEL org.opencontainers.image.source="https://github.com/projectbooth/booth-lakehouse" \
      org.opencontainers.image.description="MinIO ${MINIO_TAG} + mc ${MC_TAG}, built from source for Project Booth integration tests (ADR 0087/0091). Not for production." \
      org.opencontainers.image.licenses="AGPL-3.0-only" \
      io.projectbooth.minio.tag="${MINIO_TAG}" io.projectbooth.minio.commit="${MINIO_COMMIT}" \
      io.projectbooth.mc.tag="${MC_TAG}" io.projectbooth.mc.commit="${MC_COMMIT}"
EXPOSE 9000
VOLUME ["/data"]
ENTRYPOINT ["minio"]
CMD ["server", "/data"]
