#!/usr/bin/env sh
# Regenerates tests/integration/natsauth/{auth.conf,*.creds} with booth-core's own credential code
# (see gen.go). Needs Go and a booth-core checkout (BOOTH_CORE_DIR, default ../booth-core). Built
# from `git archive` of core's HEAD, so core's working tree is never touched; the commit is recorded.
set -eu
ROOT=$(cd "$(dirname "$0")/.." && pwd)
CORE=${BOOTH_CORE_DIR:-$ROOT/../booth-core}
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
git -C "$CORE" archive HEAD | tar -x -C "$WORK"
mkdir -p "$WORK/cmd/lakehouse-nats-fixtures"
cp "$ROOT/tests/integration/natsauth/gen.go" "$WORK/cmd/lakehouse-nats-fixtures/main.go"
sed -i 's|^//go:build ignore$||' "$WORK/cmd/lakehouse-nats-fixtures/main.go"
OUT="$ROOT/tests/integration/natsauth"
(cd "$WORK" && go run ./cmd/lakehouse-nats-fixtures "$OUT")
echo "booth-core $(git -C "$CORE" rev-parse --short HEAD)" > "$OUT/GENERATED_FROM"
echo "wrote $OUT (from booth-core $(git -C "$CORE" rev-parse --short HEAD))"
