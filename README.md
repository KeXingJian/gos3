# gos3

A minimal, S3-compatible object storage server written in Go, built as a learning project.

> Goal: understand Go and object storage by rebuilding a small slice of MinIO.
> Scope (M1–M2): single-node filesystem backend, AWS SigV4 auth, basic S3 API.

## Features

- Path-style S3 API: bucket CRUD, object PUT/GET/HEAD/DELETE, ListObjectsV2, batch DeleteObjects
- Multipart upload: Initiate / UploadPart / ListParts / Complete / Abort / ListMultipartUploads
- AWS Signature V4: header signing, presigned URLs, streaming chunk signatures
- Sidecar JSON metadata (`<root>/.meta/<bucket>/<object>.json`)
- Multipart staging under `<root>/.multipart/<bucket>/<uploadId>/` with stale-upload cleanup
- Range requests and conditional requests via `http.ServeContent`
- Graceful shutdown, structured logging (`log/slog`), request IDs
- Zero external dependencies (standard library only)

## Build & Run

```sh
make build
./gos3 server /tmp/gos3-data
# gos3 server -address :9000 -region us-east-1 /tmp/gos3-data
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
presigned URLs, anonymous denial, and error cases.

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

## Architecture

```mermaid
flowchart TD
    C[S3 Client: mc / aws cli / minio-go] -->|HTTP + SigV4| MW
    subgraph Server
        MW[Middlewares: recover / request-id / access-log / auth] --> R[Router]
        R --> H[api.Handler]
        H --> ST[store.Store]
        ST --> FS[store.FS]
    end
    FS --> DATA[<root>/bucket/object]
    FS --> META[<root>/.meta/bucket/object.json]
    FS --> MP[<root>/.multipart/bucket/uploadId/part.N]
```

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
| `internal/store` | Storage interface + filesystem implementation |
| `internal/api` | S3 handlers, XML responses, error model |
| `internal/server` | Router and middleware chain |
| `internal/version` | Version info injected via ldflags |

## Known limitations (M1–M3)

- Composite multipart ETag is `md5(concat(part md5s))-N`; no server-side checksum verification of the assembled body.
- Minimum part size (5 MiB except the last) is not enforced yet.
- Streaming signature **trailer** variant (`...-TRAILER`) not supported.
- Versioning, IAM, lifecycle, replication, erasure coding, distribution not implemented.
- Path-style addressing only (no virtual-host style).
