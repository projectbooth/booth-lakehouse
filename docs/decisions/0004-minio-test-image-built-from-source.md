# 0004: The shared MinIO test image — built from source, pinned by digest (ADR 0087, 0091)

Status: Published (2026-09-30).

```
ghcr.io/projectbooth/minio-test@sha256:04c918e8877a410f40ccab0842f724177af453f236a5fda96df219867aefe3c3
```

Public (anonymous pull verified with an empty Docker config). Shared with booth-storage; reference
it **by this digest only**, never the `RELEASE.…` tag it was also pushed under.

## What's in it

| | Release | Commit |
|---|---|---|
| MinIO server | `RELEASE.2025-10-15T17-29-55Z` | `9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a` |
| `mc` | `RELEASE.2025-08-13T08-35-41Z` | `7394ce0dd2a80935aded936b09fa12cbb3cb8096` |

Both are the **final releases**: `minio/minio` and `minio/mc` are archived upstream, so these tags
can't move and there's nothing newer to drift to. Alpine 3.22 base (pinned), `sh`/`wget` for healthchecks,
`mc` included because both modules' harnesses use `mc ready` / `mc mb`. Labels carry the tags and commits.

## How it's built (hack/minio-test/Dockerfile, .github/workflows/mirror-minio-test.yml)

From source, statically, with upstream's own version stamping (`buildscripts/gen-ldflags.go`). The
publish refuses unless:

1. each tag resolves to exactly the expected commit;
2. each built binary reports exactly the expected release;
3. the image serves: MinIO passes `mc ready`, and `mc` can create and list a bucket in it.

Checked the refusal path locally (a wrong expected commit fails the build).

## Verified against it

- booth-lakehouse's full real-stack suite (`hack/docker-compose.yml`): every test passes, including
  the broker behaviour MinIO actually enforces — expiring service accounts (15-min floor still holds
  in this release), STS session policies scoping a grant to one table, read grants refused writes.
- This is also the first real-MinIO run of `cc105ed` (top-level `access`) and `8a0d0fc`
  (renewal on behalf of a current owner) — both previously covered by unit/contract tests only.

## Judgment calls

- **Newer than what the first pass was verified on** (`2025-09-07` → `2025-10-15`): the final release,
  so it includes upstream's last fixes, and the suite passes unchanged on it.
- **The old workflow's "refuse unless it matches" is kept, moved from an image ID to provenance**: a
  source build isn't byte-reproducible across runs, so the gate is tag→commit, binary→release, and a
  live serve check, and the digest printed by the publishing run is what everyone pins.
