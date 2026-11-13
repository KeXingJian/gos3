# gos3

一个用 Go 编写的**最小化、S3 兼容对象存储服务**，定位相当于"迷你版 MinIO"，用于学习 Go 语言与对象存储系统设计。

- 模块名：`github.com/kxj/gos3`
- Go 版本：`1.24`
- 入口：`main.go` → `internal/cli.Main`

## 特性

- **S3 API 兼容**：桶、对象、分片上传、对象版本化、批量删除、生命周期过期、预签名 URL
- **SigV4 签名**：请求签名校验 + streaming chunk 解码 + 预签名 GET
- **多种存储后端**：
  - 单目录文件系统（`store.FS`）
  - Reed-Solomon 纠删码（`store.Erasure`，容忍盘 / 节点丢失）
- **元数据检索（Catalog）**：对标 MinIO AIStor 的 Metadata Search，为对象元数据建立主键 + 二级索引，支持按 contentType / 大小 / 修改时间 / tag / user-meta 检索；WAL + 快照持久化，可全量重建；管理端提供 `search` / `stats` / `status` / `reindex` API
- **分布式集群**：节点间 gRPC 通信，对等发现，全局磁盘装配
- **IAM 鉴权**：用户 / 策略管理、跨节点复制、内置 Web 控制台
- **可观测性**：OpenTelemetry 链路追踪（HTTP / gRPC / 存储三层 span），日志携带 `trace_id` / `span_id`
- **后台任务**：生命周期扫描、过期分片上传清理、盘健康探测 + 最小自愈

## 快速开始

### 构建

```bash
make build            # 生成静态二进制 gos3
make test             # 运行全部测试
make vet              # 静态检查
```

或直接使用 Go 构建：

```bash
CGO_ENABLED=0 go build -o gos3 .
```

### 单机运行

```bash
# 单盘文件系统后端
./gos3 server /tmp/gos3-data

# 多盘纠删码后端（4 盘，自动推导 data/parity 布局）
./gos3 server /tmp/d1 /tmp/d2 /tmp/d3 /tmp/d4
```

默认账号：`minioadmin / minioadmin`（可用环境变量 `GOS3_ROOT_USER` / `GOS3_ROOT_PASSWORD` 覆盖）。

用 `mc` 访问：

```bash
mc alias set local http://127.0.0.1:9000 minioadmin minioadmin
mc mb local/demo
mc cp ./file.txt local/demo/file.txt
mc ls local/demo
```

### 分布式运行

至少需要两个节点，每个节点声明自己的 gRPC 地址和对端：

```bash
# 节点 1
./gos3 server -address :9000 -advertise node1:9001 -peers node2:9001 /data/d1 /data/d2

# 节点 2
./gos3 server -address :9000 -advertise node2:9001 -peers node1:9001 /data/d1 /data/d2
```

纠删码布局跨所有节点的磁盘计算，单个节点整机宕机后，存活节点可用校验片重建对象。

### Docker

```bash
docker compose up -d --wait gos3    # 单节点 4 盘纠删码
```

## 命令行

```
gos3 server [flags] <data-dir> [data-dir...]
gos3 version
gos3 help
```

| Flag | 默认值 | 说明 |
| --- | --- | --- |
| `-address` | `:9000` | HTTP 监听地址 |
| `-region` | `us-east-1` | 区域名 |
| `-root-user` | `minioadmin` | root 访问密钥（`GOS3_ROOT_USER`） |
| `-root-password` | `minioadmin` | root 密钥（`GOS3_ROOT_PASSWORD`） |
| `-data-shards` | `0`（自动） | 纠删码数据分片数 |
| `-parity-shards` | `0`（自动） | 纠删码校验分片数 |
| `-grpc-address` | `:9001` | 节点间 gRPC 监听地址 |
| `-advertise` | — | 其他节点访问本节点的 gRPC 地址 |
| `-peers` | — | 逗号分隔的对端 gRPC 地址（分布式模式） |
| `-scan-interval` | `1m` | 生命周期扫描间隔 |
| `-catalog` | `false` | 启用元数据索引 |
| `-catalog-dir` | `<data-dir>/.catalog` | 索引目录 |
| `-catalog-meta-whitelist` | — | 逗号分隔的 user-meta key（小写、不含 `x-amz-meta-` 前缀） |
| `-catalog-tag-whitelist` | — | 逗号分隔的 tag key |

端口约定：S3 服务 `9000`，节点间 gRPC `9001`。

## S3 API 支持

**桶级（`/{bucket}`）**

| 操作 | 方法 / 参数 |
| --- | --- |
| ListBuckets | `GET /` |
| CreateBucket | `PUT /{bucket}` |
| HeadBucket | `HEAD /{bucket}` |
| DeleteBucket | `DELETE /{bucket}` |
| ListObjects | `GET /{bucket}` |
| ListObjectVersions | `GET /{bucket}?versions` |
| DeleteObjects | `POST /{bucket}?delete` |
| GetBucketVersioning | `GET /{bucket}?versioning` |
| SetBucketVersioning | `PUT /{bucket}?versioning` |
| GetBucketLifecycle | `GET /{bucket}?lifecycle` |
| SetBucketLifecycle | `PUT /{bucket}?lifecycle` |
| DeleteBucketLifecycle | `DELETE /{bucket}?lifecycle` |

**对象级（`/{bucket}/{object}`）**

| 操作 | 方法 / 参数 |
| --- | --- |
| PutObject | `PUT` |
| GetObject | `GET` |
| HeadObject | `HEAD` |
| DeleteObject | `DELETE` |
| CreateMultipartUpload | `POST ?uploads` |
| UploadPart | `PUT ?uploadId=&partNumber=` |
| CompleteMultipartUpload | `POST ?uploadId=` |
| AbortMultipartUpload | `DELETE ?uploadId=` |
| ListParts | `GET ?uploadId=` |
| ListMultipartUploads | `GET /{bucket}?uploads` |

**健康检查**

| 路径 | 说明 |
| --- | --- |
| `/healthz` | 存活探针 |
| `/minio/health/ready` | 就绪探针 |
| `/minio/health/cluster` | 集群健康详情 |

## 管理接口与控制台

内置 Web 控制台：`http://127.0.0.1:9000/ui`（登录后可视化创建用户、绑定策略、管理桶，预签名 URL 一键生成）。

管理 API 位于 `/gos3/admin/*`（HTTP Basic 鉴权，用户 / 策略管理仅限 root）：

```
GET  /gos3/admin/whoami                 当前身份
GET|PUT|DELETE /gos3/admin/users        用户管理
GET|PUT|DELETE /gos3/admin/policies     策略管理
PUT  /gos3/admin/attach                 绑定策略
PUT  /gos3/admin/detach                 解绑策略
GET  /gos3/admin/presign                预签名下载 URL
GET  /gos3/admin/catalog/search         元数据检索（仅 root）
GET  /gos3/admin/catalog/stats          元数据聚合（仅 root）
GET  /gos3/admin/catalog/status         索引状态（仅 root）
POST /gos3/admin/catalog/reindex        全量重建索引（仅 root）
     /gos3/admin/buckets...             桶 / 对象管理（按策略授权）
```

## 可观测性

通过 `GOS3_OTEL_EXPORTER` 选择导出器：

- `stdout`（默认）：trace 输出到标准输出
- `otlp`：导出到 OTLP gRPC 收集器（如 Jaeger / Tempo）

`docker-compose-influxdb.yaml` 提供了 InfluxDB 作为指标后端参考。

## 元数据索引（Catalog）

可选开启（`-catalog`），对标 MinIO AIStor 的 **Catalog / Metadata Search**：以 sidecar 元数据为唯一真相，旁挂建立索引，按对象属性检索，替代逐目录遍历。

启动示例（user-meta 白名单决定哪些自定义元数据进二级索引）：

```bash
./gos3 server -catalog -catalog-meta-whitelist owner,project /tmp/gos3-data
```

**已实现**

- 主键 + 二级索引：`contentType`、修改时间、大小分档、`tag`（S3 Object Tagging per-version）、`user-meta`（白名单 key）
  - `user-meta` 必须通过 `-catalog-meta-whitelist` 显式开启，否则只索引系统字段
  - Tagging API 支持 GET/PUT/DELETE，并在 sidecar 中随对象版本持久化
- WAL + 快照 + MANIFEST 持久化，崩溃（kill -9）后恢复已 fsync 的记录，支持全量重建
- 写路径异步投递（write-behind），队列满自动降级计数；后台定时 fsync 兜底低写入速率
- 查询 API：`search`（过滤 + 分页）、`stats`（聚合）、`status`、`reindex`、`sql`（受限 SQL 子集）
- 控制台 root 用户可打开 Metadata Catalog 筛选页；多节点节点本地索引默认每 5 分钟从共享 sidecar 全量 reconcile

**查询示例**

```bash
BASE=http://127.0.0.1:9000/gos3/admin/catalog
AUTH=minioadmin:minioadmin

# 按 bucket / 前缀 / 类型 / 大小检索；默认只返回最新版本、不含删除标记
curl -u "$AUTH" "$BASE/search?bucket=photos&prefix=2026/&contentType=image/jpeg&minSize=1048576&limit=50"

# user-meta / tag 等值过滤
curl -u "$AUTH" "$BASE/search?bucket=photos&meta.owner=alice&tag.env=prod"

# 按 contentType 聚合
curl -u "$AUTH" "$BASE/stats?bucket=photos&groupBy=contentType"

# 索引状态 / 从 sidecar 全量重建
curl -u "$AUTH" "$BASE/status"
curl -u "$AUTH" -X POST "$BASE/reindex"
```

分页：把响应中的 `nextToken` 原样作为 `token` 参数传回。索引重建会改变 `gen`，旧 token 返回 `409`，需要重新发起查询。搜索响应头 `X-Gos3-Catalog-Scan` 表示扫描方式：`index`（命中二级索引）、`bucket`（bucket 内主键扫描）或 `full`（全量扫描）。`versions=true` 可包含历史版本，`deleteMarkers=true` 可包含删除标记。

SQL 示例：

```bash
curl -u "$AUTH" -H 'Content-Type: application/json' -d '{"query":"SELECT key,size FROM S3Object WHERE bucket = '\''photos'\'' AND key LIKE '\''2026/%'\'' LIMIT 100"}' "$BASE/sql"
```

SQL 仅是元数据 DSL，允许 SELECT projection/count、AND 比较、前缀 LIKE、IN；非通用 SQL 引擎。控制台筛选页仅 root 可见。多节点仍保留节点本地索引，靠定期 sidecar 全量重建最终对账，尚无实时事件复制。

## 项目结构

```
main.go                 入口
internal/
  cli/                  子命令解析、启动流程、后台循环
  config/               运行时配置与默认值
  auth/                 凭证存储
  sign/                 SigV4 校验、streaming chunk、预签名
  server/               路由与中间件链
  api/                  S3 处理器、Admin API、内置 UI
  store/                Store 接口 + FS / Erasure 实现
  erasure/              Reed-Solomon 编解码
  disk/                 Disk 抽象（Local / Remote，gRPC）
  cluster/              gRPC 磁盘服务、对等发现、磁盘装配
  lock/                 命名空间锁（单机 / 分布式）
  iam/                  用户、策略、鉴权、快照
  iamsync/              节点间 IAM 复制
  lifecycle/            生命周期模型与过期判定
  catalog/              元数据索引引擎（WAL + 快照 + 索引）
  heal/                 最小自愈队列
  health/               盘健康探测 + 集群健康视图
  telemetry/            OpenTelemetry 初始化
  format/               磁盘格式（format.json）
  version/              版本信息
docs/                   架构设计文档
scripts/                E2E 验证脚本
```

## 文档

| 文档 | 内容 |
| --- | --- |
| [docs/architecture-summary.md](docs/architecture-summary.md) | 架构总结与包职责 |
| [docs/runtime-architecture.md](docs/runtime-architecture.md) | 运行时架构 |
| [docs/cluster-architecture.md](docs/cluster-architecture.md) | 集群架构 |
| [docs/cluster-communication.md](docs/cluster-communication.md) | 集群通信 |
| [docs/metadata-catalog.md](docs/metadata-catalog.md) | 元数据目录设计 |
| [docs/catalog-architecture.md](docs/catalog-architecture.md) | 元数据存储引擎架构 |
| [docs/catalog-storage-engine.md](docs/catalog-storage-engine.md) | 引擎层深挖 |
| [docs/file-metadata-reference.md](docs/file-metadata-reference.md) | 文件元数据参考 |

## 测试与验证

```bash
make test            # 单元测试
make verify-docker   # 单机端到端验证（Docker）
make verify-dist     # 分布式端到端验证（2 节点，含节点宕机重建）
```

验证套件用 `mc` + `curl` 覆盖 SigV4 签名、CRUD、分片上传、预签名、纠删恢复、版本化、IAM、生命周期等 30+ 项。

## 已知限制

- 纠删码为**整对象、内存内**编解码，超大对象受内存限制，gRPC 分片消息上限 128 MiB
- 纠删码布局（盘数、数据 / 校验片）启动时固定，未持久化到 `format.json`
- 集群成员静态配置，无分布式锁 / 选主，跨节点并发写同 key 不协调
- IAM 为最终一致复制（全量快照 + 修订号，最后写入者胜）
- 生命周期仅支持 `Expiration`（Days / Date）
- 分片上传暂存仅落在第一个全局盘
- `ListObjectVersions` 不支持 `version-id-marker`
- 仅支持 path-style 寻址，无 virtual-host 风格
- Catalog 索引仍为节点本地内存态，使用定时全量 reconcile（默认 5 分钟）而非实时事件复制；reconcile 有可见性延迟且重建会使旧分页 token 失效
- Catalog 未实现 `CatalogMaxDocs` / 换用磁盘 KV，文档数达到千万级时内存会吃紧

更多细节见 [docs/architecture-summary.md](docs/architecture-summary.md) 的"已知限制"一节。
