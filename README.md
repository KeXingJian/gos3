# gos3

A minimal, S3-compatible object storage server written in Go, built as a learning project.

> Goal: understand Go and object storage by rebuilding a small slice of MinIO.
> Scope (M1–M2): single-node filesystem backend, AWS SigV4 auth, basic S3 API.

## Features

- Path-style S3 API: bucket CRUD, object PUT/GET/HEAD/DELETE, ListObjectsV2, batch DeleteObjects
- Object versioning: enable/suspend, version IDs, delete markers, ListObjectVersions
- Multipart upload: Initiate / UploadPart / ListParts / Complete / Abort / ListMultipartUploads
- Erasure coding across N drives (Reed-Solomon), write/read quorum, recovery from drive loss
- Distributed clusters: drives on peer nodes accessed over **gRPC + protobuf**; a whole node can
  fail and objects remain readable (reconstructed from parity shards on survivors)
- AWS Signature V4: header signing, presigned URLs, streaming chunk signatures
- Versioned JSON metadata (`<root>/.meta/<bucket>/<object>.json` holds a list of versions),
  with per-version data under `<root>/.data/<bucket>/<object>/<versionId>`
- Multipart staging under `<root>/.multipart/<bucket>/<uploadId>/` with stale-upload cleanup
- Range requests and conditional requests via `http.ServeContent`
- Graceful shutdown, structured logging (`log/slog`), request IDs
- External dependencies: `klauspost/reedsolomon`, `google.golang.org/grpc`, `google.golang.org/protobuf`

## Build & Run

```sh
make build

# single-drive filesystem backend
./gos3 server /tmp/gos3-data

# erasure-coded backend across 4 drives (default layout: 2 data + 2 parity)
./gos3 server /data/d1 /data/d2 /data/d3 /data/d4

# override shard counts explicitly
./gos3 server -data-shards 3 -parity-shards 1 /data/d1 /data/d2 /data/d3 /data/d4

# distributed: 2 nodes x 2 drives each (run on each node, swapping -advertise/-peers)
./gos3 server -advertise node1:9001 -peers node2:9001 /data/d1 /data/d2
./gos3 server -advertise node2:9001 -peers node1:9001 /data/d1 /data/d2
```

Default credentials: `minioadmin` / `minioadmin`
(override with `-root-user` / `-root-password` or `GOS3_ROOT_USER` / `GOS3_ROOT_PASSWORD`).

## Verify with mc

```sh
mc alias set local http://127.0.0.1:9000 minioadmin minioadmin
mc mb local/demo
mc cp ./README.md local/demo/
mc ls local/demo/
mc cat local/demo/README.md
mc rm local/demo/README.md
mc rb local/demo
```

Health endpoints: `GET /healthz`, `GET /minio/health/live`, `GET /minio/health/ready`.

## Verify with Docker

Builds a static server image and runs an end-to-end suite (`mc` + `curl`) against it.
The suite checks SigV4, bucket/object CRUD, listing, multipart upload integrity,
presigned URLs, anonymous denial, error cases, erasure recovery after simulated
drive loss, and object versioning (27 checks).

```sh
make verify-docker
# or manually:
docker compose build
docker compose up -d --wait gos3      # host port 19000 -> container 9000
docker compose run --rm verify
docker compose down -v
```

The host port is `19000` by default to avoid clashing with a local MinIO on `9000`;
override with `GOS3_PORT=...`. The `verify` image bundles `mc` (copied from `minio/mc`).

Distributed cluster (2 nodes x 2 drives): writes across both nodes, then stops `node1`
and reads back through `node2` (reconstructing data from parity shards):

```sh
make verify-dist
```

## Architecture

```mermaid
flowchart TD
    C[S3 Client: mc / aws cli / minio-go] -->|HTTP + SigV4| MW
    subgraph Server
        MW[Middlewares: recover / request-id / access-log / auth] --> R[Router]
        R --> H[api.Handler]
        H --> ST[store.Store]
        ST --> FS["store.FS (1 dir)"]
        ST --> ER["store.Erasure (N dirs)"]
        ER --> RS["internal/erasure (Reed-Solomon)"]
    end
    FS --> DATA[<root>/.data/bucket/object/versionId + .meta]
    ER --> DK{"disk.Disk"}
    DK --> LOCAL["disk.Local (this node's drives)"]
    DK --> REMOTE["disk.Remote (gRPC -> peer drives)"]
    LOCAL --> SH[<drive>/.data/bucket/object/versionId = shard_i]
    REMOTE --> SH2[peer drive shards]
```

The `Erasure` object layer is written only against the `disk.Disk` interface, so the
same code drives local directories and remote nodes. `internal/cluster` starts the gRPC
disk service, discovers peers, and assembles the global ordered drive list (sorted by
node address), which every node computes identically.

Request pipeline:

```mermaid
sequenceDiagram
    participant C as Client
    participant MW as Middleware
    participant H as api.Handler
    participant S as store.FS
    C->>MW: PUT /bucket/key (SigV4)
    MW->>MW: verify signature; unwrap streaming chunks
    MW->>H: PutObject
    H->>S: PutObject(reader)
    S->>S: write temp + md5, rename, write meta.json
    S-->>H: ObjectInfo
    H-->>C: 200 + ETag
```

## Package layout

| Package | Responsibility |
| --- | --- |
| `internal/cli` | Subcommand parsing, startup, graceful shutdown |
| `internal/config` | Runtime configuration and defaults |
| `internal/auth` | Credential store (access key -> secret key) |
| `internal/sign` | SigV4 verification and streaming chunk decoding |
| `internal/store` | Storage interface, filesystem (`FS`) and erasure (`Erasure`) backends |
| `internal/erasure` | Reed-Solomon encode/decode (`klauspost/reedsolomon`) |
| `internal/disk` | `Disk` abstraction: `Local` (filesystem) and `Remote` (gRPC), plus generated proto |
| `internal/cluster` | gRPC disk service, peer discovery, global drive assembly |
| `internal/api` | S3 handlers, XML responses, error model |
| `internal/server` | Router and middleware chain |
| `internal/version` | Version info injected via ldflags |

## Known limitations (M1–M5)

- `ListObjectVersions` pagination supports `key-marker` but not `version-id-marker`.
- No MFA-delete or version-level legal hold/retention.
- Erasure coding is currently **whole-object and in-memory** (no per-block streaming), so very large
  objects are bounded by RAM. This also bounds the gRPC shard message size (raised to 128 MiB);
  MinIO streams block-by-block, which is a future step.
- Erasure layout (drive count, data/parity) is fixed at startup and not persisted in a `format.json`,
  so data must be read back with the same layout.
- Cluster membership is static (flags), with no distributed lock/leader election; concurrent writes
  to the same key from different nodes are not coordinated.
- Multipart staging lives on the first global drive only; the assembled object is erasure-coded on completion.
- Composite multipart ETag is `md5(concat(part md5s))-N`; no server-side checksum verification of the assembled body.
- Minimum part size (5 MiB except the last) is not enforced yet.
- Streaming signature **trailer** variant (`...-TRAILER`) not supported.
- IAM, lifecycle, and event notification not implemented.
- Path-style addressing only (no virtual-host style).
