"""Contract tests (testing-strategy.md layer 2): the BoothModule manifest against
contracts/module-manifest.md, and the chart's credential/network topology, from a rendered
``helm template`` — no cluster. Skips locally without helm; CI sets BOOTH_TEST_REQUIRE_HELM=1 so a
missing helm FAILS instead of skipping green.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

CHART = Path(__file__).resolve().parents[2] / "charts" / "booth-lakehouse"
REQUIRED = ["--set", "identity.oidcIssuerUrl=http://keycloak.booth-system.svc:8080/realms/booth"]
RELEASE = "lh"
FULL = f"{RELEASE}-booth-lakehouse"


def helm(*args: str) -> subprocess.CompletedProcess[str]:
    if shutil.which("helm") is None:
        if os.environ.get("BOOTH_TEST_REQUIRE_HELM") == "1":
            pytest.fail("helm is not installed but BOOTH_TEST_REQUIRE_HELM=1")
        pytest.skip("helm not installed; CI has it")
    return subprocess.run(["helm", *args], capture_output=True, text=True, check=False)


def render(*extra: str) -> list[dict]:
    out = helm("template", RELEASE, str(CHART), "--namespace", "booth-lakehouse", *REQUIRED, *extra)
    assert out.returncode == 0, out.stderr
    return [d for d in yaml.safe_load_all(out.stdout) if d]


def one(docs: list[dict], kind: str, name: str) -> dict:
    (found,) = [d for d in docs if d["kind"] == kind and d["metadata"]["name"] == name]
    return found


def pod(d: dict) -> dict:
    return d["spec"]["template"]["spec"]


def env_of(container: dict) -> dict:
    return {e["name"]: e for e in container.get("env", [])}


def secret_refs(d: dict) -> set[tuple[str, str]]:
    refs = set()
    for c in pod(d).get("initContainers", []) + pod(d)["containers"]:
        for e in c.get("env", []):
            ref = e.get("valueFrom", {}).get("secretKeyRef")
            if ref:
                refs.add((ref["name"], ref["key"]))
    return refs


@pytest.fixture(scope="module")
def chart() -> list[dict]:
    return render()


@pytest.fixture(scope="module")
def spec(chart) -> dict:
    return one(chart, "BoothModule", "lakehouse")["spec"]


@pytest.fixture(scope="module")
def api(chart) -> dict:
    return one(chart, "Deployment", f"{FULL}-api")


@pytest.fixture(scope="module")
def lakekeeper(chart) -> dict:
    return one(chart, "Deployment", f"{FULL}-lakekeeper")


# ---- the manifest (module-manifest.md) ----------------------------------------------------------


def test_manifest_required_fields(chart, spec):
    m = one(chart, "BoothModule", "lakehouse")
    assert (m["apiVersion"], m["kind"]) == ("booth.projectbooth.io/v1alpha1", "BoothModule")  # ADR 0019
    assert spec["id"] == "lakehouse" and re.fullmatch(r"[a-z][a-z0-9-]*", spec["id"])  # repo name minus booth-
    assert spec["displayName"] == "Lakehouse"
    assert re.match(r"^\d+\.\d+\.\d+", spec["version"]) and re.match(r"^\d+\.\d+\.\d+", spec["contractVersion"])
    assert spec["healthCheckPath"] == "/health"


def test_one_native_manage_view_and_no_admin_split(spec):
    """ADR 0093: a single native view under navPath, no adminNavPath (module-manifest.md UI rules)."""
    assert spec["hasOwnUi"] is True
    assert spec["uiIntegrationMode"] == "native"
    assert spec["navGroup"] == "manage"
    assert spec["navPath"] == "/lakehouse"
    assert "adminNavPath" not in spec


def test_operator_workspaces_default_to_none(api):
    """Fail closed: nobody sees another tenant's warehouse unless an operator names workspaces."""
    assert env_of(pod(api)["containers"][0])["BOOTH_LAKEHOUSE_OPERATOR_WORKSPACES"]["value"] == ""
    docs = render("--set", "access.operatorWorkspaces={ops,platform}")
    api2 = one(docs, "Deployment", f"{FULL}-api")
    assert env_of(pod(api2)["containers"][0])["BOOTH_LAKEHOUSE_OPERATOR_WORKSPACES"]["value"] == "ops,platform"


def test_declares_database_workload_identity_and_exactly_the_table_events(spec):
    assert spec["database"] == {"enabled": True}  # ADR 0053
    assert spec["workloadIdentity"] == {"mint": True}  # ADR 0056, for credential renewal
    # ADR 0085 via ADR 0050: publish only, only these three, nothing to subscribe to.
    assert spec["events"] == {"publish": ["table.created", "table.updated", "table.deleted"]}
    pattern = re.compile(r"^[a-z][a-z0-9]*(\.([a-z][a-z0-9]*|\*))+$")  # core's CRD validation
    assert all(pattern.match(p) for p in spec["events"]["publish"])


def test_table_events_can_be_turned_off_everywhere():
    docs = render("--set", "tableEvents.enabled=false")
    assert "events" not in one(docs, "BoothModule", "lakehouse")["spec"]
    api = one(docs, "Deployment", f"{FULL}-api")
    assert "booth-event-bus-credentials" not in {n for n, _ in secret_refs(api)}
    assert all(v.get("secret", {}).get("secretName") != "booth-event-bus-credentials" for v in pod(api)["volumes"])


def test_event_bus_credential_is_mounted_only_into_the_api_and_is_optional(api, lakekeeper):
    (vol,) = [v for v in pod(api)["volumes"] if "secret" in v]
    assert vol["secret"]["secretName"] == "booth-event-bus-credentials" and vol["secret"]["optional"] is True
    env = env_of(pod(api)["containers"][0])
    mount = next(m for m in pod(api)["containers"][0]["volumeMounts"] if m["name"] == vol["name"])
    assert env["BOOTH_EVENTS_CREDS_FILE"]["value"] == mount["mountPath"] + "/nats.creds" and mount["readOnly"] is True
    assert env["BOOTH_EVENTS_URL"]["valueFrom"]["secretKeyRef"] == {"name": "booth-event-bus-credentials", "key": "url", "optional": True}
    assert not [v for v in pod(lakekeeper).get("volumes", []) if "secret" in v]


def test_workload_identity_can_be_turned_off_everywhere():
    docs = render("--set", "workloadIdentity.enabled=false")
    spec = one(docs, "BoothModule", "lakehouse")["spec"]
    assert "workloadIdentity" not in spec
    names = {n for n, _ in secret_refs(one(docs, "Deployment", f"{FULL}-api"))}
    assert "booth-workload-minting-credentials" not in names


def test_core_routes_to_the_api_and_polls_its_real_health_path(spec, chart, api):
    svc = one(chart, "Service", f"{FULL}-api")
    assert spec["serviceRef"] == {"name": svc["metadata"]["name"], "port": 8080}
    assert pod(api)["containers"][0]["readinessProbe"]["httpGet"]["path"] == spec["healthCheckPath"]


# ---- credentials: who holds what ----------------------------------------------------------------


def test_lakekeeper_holds_only_its_database_and_encryption_key(lakekeeper):
    assert secret_refs(lakekeeper) == {("booth-database-credentials", "dsn"), (f"{FULL}-lakekeeper", "encryption-key")}
    assert pod(lakekeeper)["automountServiceAccountToken"] is False


def test_api_never_holds_lakekeepers_encryption_key(api):
    refs = secret_refs(api)
    assert (f"{FULL}-lakekeeper", "encryption-key") not in refs
    assert refs == {
        ("booth-database-credentials", "dsn"),
        ("booth-event-bus-credentials", "url"),
        ("booth-workload-minting-credentials", "issuer"),
        ("booth-workload-minting-credentials", "url"),
        ("booth-workload-minting-credentials", "credential"),
    }
    assert pod(api)["automountServiceAccountToken"] is False


def test_no_storage_credential_is_configured_anywhere(chart):
    """Storage access comes only from the ADR 0080 broker; nothing in the chart can smuggle a static key in."""
    text = yaml.safe_dump_all(chart).lower()
    for needle in ("access-key", "secret-access", "aws_access", "aws_secret", "minio_root", "s3.access"):
        assert needle not in text


def test_encryption_key_survives_upgrades_and_uninstall(chart):
    s = one(chart, "Secret", f"{FULL}-lakekeeper")
    assert s["metadata"]["annotations"]["helm.sh/resource-policy"] == "keep"


def test_lakekeeper_image_is_pinned_by_digest(lakekeeper):
    images = {c["image"] for c in pod(lakekeeper)["initContainers"] + pod(lakekeeper)["containers"]}
    assert len(images) == 1 and re.search(r"@sha256:[0-9a-f]{64}$", images.pop())


def test_lakekeeper_migrates_before_serving(lakekeeper):
    assert [c["args"] for c in pod(lakekeeper)["initContainers"]] == [["migrate"]]
    assert pod(lakekeeper)["containers"][0]["args"] == ["serve"]


def test_api_config_points_at_lakekeeper_and_the_broker(api):
    env = env_of(pod(api)["containers"][0])
    assert env["LAKEKEEPER_URL"]["value"] == f"http://{FULL}-lakekeeper:8181"
    assert env["BOOTH_CREDENTIAL_BROKER_URL"]["value"] == "http://booth-core.booth-system.svc.cluster.local:8080/api/credentials"
    assert env["BOOTH_OIDC_ISSUER_URL"]["value"].endswith("/realms/booth")


def test_the_issuer_is_required():
    out = helm("template", RELEASE, str(CHART))
    assert out.returncode != 0 and "identity.oidcIssuerUrl is required" in out.stderr


def test_containers_run_unprivileged(api, lakekeeper):
    for d in (api, lakekeeper):
        assert pod(d)["securityContext"]["runAsNonRoot"] is True
        # A numeric UID: kubelet can't verify runAsNonRoot against an image that sets no USER (Lakekeeper's).
        assert isinstance(pod(d)["securityContext"]["runAsUser"], int) and pod(d)["securityContext"]["runAsUser"] > 0
        for c in pod(d).get("initContainers", []) + pod(d)["containers"]:
            sc = c["securityContext"]
            assert sc["allowPrivilegeEscalation"] is False and sc["readOnlyRootFilesystem"] is True and sc["capabilities"]["drop"] == ["ALL"]


# ---- network boundaries -------------------------------------------------------------------------


def test_only_the_api_can_reach_lakekeeper(chart, api):
    np = one(chart, "NetworkPolicy", f"{FULL}-lakekeeper")
    assert np["spec"]["podSelector"]["matchLabels"]["app.kubernetes.io/component"] == "lakekeeper"
    (rule,) = np["spec"]["ingress"]
    (peer,) = rule["from"]
    assert set(peer) == {"podSelector"}  # same namespace, no namespaceSelector
    api_labels = api["spec"]["template"]["metadata"]["labels"]
    assert all(api_labels.get(k) == v for k, v in peer["podSelector"]["matchLabels"].items())
    assert rule["ports"] == [{"protocol": "TCP", "port": 8181}]


def test_only_core_can_reach_the_api(chart):
    np = one(chart, "NetworkPolicy", f"{FULL}-api")
    (rule,) = np["spec"]["ingress"]
    (peer,) = rule["from"]
    assert peer["namespaceSelector"]["matchLabels"] == {"kubernetes.io/metadata.name": "booth-system"}
    assert peer["podSelector"]["matchLabels"] == {"app.kubernetes.io/name": "booth-core"}


def test_every_pod_carries_the_module_label(api, lakekeeper):
    for d in (api, lakekeeper):
        assert d["spec"]["template"]["metadata"]["labels"]["booth.projectbooth.io/module"] == "lakehouse"
        assert "booth.projectbooth.io/workspace" not in d["spec"]["template"]["metadata"]["labels"]  # shared pods (ADR 0077)
