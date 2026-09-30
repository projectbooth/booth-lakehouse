#!/usr/bin/env sh
# Layer 3 (contracts/testing-strategy.md): the real chart on a real kind cluster.
#
#   sh hack/kind-integration.sh            # needs docker, kind, kubectl, helm
#   KEEP_CLUSTER=1 sh hack/kind-integration.sh
#
# 1. booth-core's real BoothModule CRD is installed, so the API server validates this chart's manifest.
# 2. The Secrets core would provision (ADR 0020/0053/0056) are created by hand, pointing at a real
#    Postgres; MinIO and fakecore (stand-in IdP/gateway/minter/broker) run next to the module.
# 3. The chart is installed; its Lakekeeper migrates and serves, its API becomes ready.
# 4. The whole real-stack suite runs in-cluster as a Job against the chart's own Services.
# 5. Both Deployments are restarted and a warehouse + table are checked to have survived.
set -eu

CLUSTER=${CLUSTER:-booth-lakehouse-it}
NS=booth-lakehouse
RELEASE=it
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"

if ! kind get clusters | grep -qx "$CLUSTER"; then
  kind create cluster --name "$CLUSTER" --wait 120s
fi
CTX="kind-$CLUSTER"
k() { kubectl --context "$CTX" "$@"; }

cleanup() {
  status=$?
  if [ "$status" -ne 0 ]; then
    echo "---- FAILED: diagnostics ----"
    k -n "$NS" get pods -o wide || true
    k -n "$NS" logs deploy/$RELEASE-booth-lakehouse-api --tail=80 || true
    k -n "$NS" logs deploy/$RELEASE-booth-lakehouse-lakekeeper --tail=40 || true
    k -n "$NS" logs job/lakehouse-it --tail=200 || true
  fi
  if [ -z "${KEEP_CLUSTER:-}" ]; then kind delete cluster --name "$CLUSTER"; fi
  exit "$status"
}
trap cleanup EXIT

echo "---- images"
docker build -q -t booth-lakehouse:it .
docker build -q -t booth-lakehouse-tests:it -f tests/integration/Dockerfile .
# Third-party images go through the local docker cache too: quay.io/minio/minio now answers anonymous
# pulls with 401 (found on this script's first run), so the node can't always pull it itself.
LAKEKEEPER_IMAGE=$(grep -o 'quay.io/lakekeeper/catalog@sha256:[0-9a-f]*' charts/booth-lakehouse/values.yaml)
for img in postgres:16-alpine quay.io/minio/minio:latest "$LAKEKEEPER_IMAGE"; do
  docker image inspect "$img" >/dev/null 2>&1 || docker pull "$img"
done
# A digest-only reference can't be `kind load`ed by name: tag it, and point the chart at the tag.
docker tag "$LAKEKEEPER_IMAGE" lakekeeper:it
# Not `kind load`: with docker's containerd image store it imports --all-platforms, which fails on a
# multi-platform index whose other platforms were never pulled ("content digest ... not found").
PLATFORM=linux/$(docker version -f '{{.Server.Arch}}')
for img in booth-lakehouse:it booth-lakehouse-tests:it postgres:16-alpine quay.io/minio/minio:latest lakekeeper:it; do
  docker save --platform "$PLATFORM" "$img" | docker exec -i "$CLUSTER-control-plane" ctr --namespace=k8s.io images import --snapshotter=overlayfs -
done

echo "---- booth-core's CRD, namespace, core-provisioned Secrets"
k apply -f tests/integration/fixtures/boothmodule-crd.yaml
k wait --for=condition=Established crd/boothmodules.booth.projectbooth.io --timeout=60s
k create namespace "$NS" --dry-run=client -o yaml | k apply -f -
k -n "$NS" create secret generic booth-database-credentials \
  --from-literal=dsn="postgres://booth_mod_lakehouse:lakehouse-test@postgres:5432/booth_mod_lakehouse?sslmode=disable" \
  --from-literal=host=postgres --from-literal=port=5432 --from-literal=database=booth_mod_lakehouse \
  --from-literal=username=booth_mod_lakehouse --from-literal=password=lakehouse-test \
  --dry-run=client -o yaml | k apply -f -
k -n "$NS" create secret generic booth-workload-minting-credentials \
  --from-literal=url=http://fakecore:9090/api/internal/workload-tokens \
  --from-literal=credential=test-mint-credential \
  --from-literal=issuer=http://fakecore:9090 \
  --dry-run=client -o yaml | k apply -f -

echo "---- surroundings: postgres, minio, fakecore"
k -n "$NS" apply -f tests/integration/fixtures/stack.yaml
k -n "$NS" rollout status deploy/postgres deploy/minio deploy/fakecore --timeout=180s

echo "---- the chart"
helm --kube-context "$CTX" upgrade --install "$RELEASE" charts/booth-lakehouse -n "$NS" --wait --timeout 5m \
  --set image.repository=booth-lakehouse --set image.tag=it --set image.pullPolicy=Never \
  --set lakekeeper.image=lakekeeper:it \
  --set identity.oidcIssuerUrl=http://fakecore:9090 \
  --set identity.workloadIssuerUrl=http://fakecore:9090 \
  --set broker.url=http://fakecore:9090/api/credentials \
  -f tests/integration/fixtures/kind-values.yaml \
  --set warehouseCredential.ttlSeconds=300 --set warehouseCredential.renewMarginSeconds=880 \
  --set warehouseCredential.renewIntervalSeconds=5
k -n "$NS" get boothmodule lakehouse -o jsonpath='{.spec.id} {.spec.healthCheckPath} {.spec.serviceRef.name}{"\n"}'

run_suite() { # $1 = job name suffix, $2 = extra env name=value, $3 = pytest target
  k -n "$NS" delete job "lakehouse-$1" --ignore-not-found
  cat <<EOF | k -n "$NS" apply -f -
apiVersion: batch/v1
kind: Job
metadata: {name: lakehouse-$1}
spec:
  backoffLimit: 0
  template:
    # The suite also calls the API directly (health, forged-role checks), so it stands in for core too.
    metadata: {labels: {booth-it/core: "true"}}
    spec:
      restartPolicy: Never
      containers:
        - name: tests
          image: booth-lakehouse-tests:it
          imagePullPolicy: Never
          command: ["pytest", "$3", "-v", "-rs"]
          env:
            - {name: BOOTH_INTEGRATION, value: "1"}
            - {name: API_URL, value: "http://$RELEASE-booth-lakehouse-api:8080"}
            - {name: FAKECORE_URL, value: "http://fakecore:9090"}
            - {name: MINIO_ENDPOINT, value: "http://minio:9000"}
            - {name: ${2%%=*}, value: "${2#*=}"}
EOF
  if ! k -n "$NS" wait --for=condition=complete "job/lakehouse-$1" --timeout=15m; then
    k -n "$NS" logs "job/lakehouse-$1" --tail=300
    return 1
  fi
  k -n "$NS" logs "job/lakehouse-$1" | tail -40
}

echo "---- NetworkPolicy is enforced (kind's kindnet does; k3s's default flannel doesn't)"
probe() { # $1 = pod labels (k=v,...) or "", prints "api=<ok|blocked> lakekeeper=<ok|blocked>"
  k -n "$NS" run "np-probe-$$" --rm -i --restart=Never --image=booth-lakehouse-tests:it --image-pull-policy=Never     ${1:+--labels="$1"} --command -- python -c "
import urllib.request
def t(u):
    try:
        urllib.request.urlopen(u, timeout=5); return 'ok'
    except Exception:
        return 'blocked'
print('api=' + t('http://$RELEASE-booth-lakehouse-api:8080/health'), 'lakekeeper=' + t('http://$RELEASE-booth-lakehouse-lakekeeper:8181/health'))
" 2>/dev/null | grep '^api='
}
got=$(probe ""); echo "unlabelled pod: $got"; [ "$got" = "api=blocked lakekeeper=blocked" ]
got=$(probe "booth-it/core=true"); echo "core pod: $got"; [ "$got" = "api=ok lakekeeper=blocked" ]

echo "---- the real-stack suite, in-cluster"
run_suite it "BOOTH_SLOW=" tests/integration/test_real_stack.py

echo "---- restart both Deployments; state must survive"
k -n "$NS" rollout restart deploy/$RELEASE-booth-lakehouse-api deploy/$RELEASE-booth-lakehouse-lakekeeper
k -n "$NS" rollout status deploy/$RELEASE-booth-lakehouse-api deploy/$RELEASE-booth-lakehouse-lakekeeper --timeout=180s
run_suite restart "BOOTH_AFTER_RESTART=1" tests/integration/test_after_restart.py

echo "---- PASS"
