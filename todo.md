# gos3 · 实施 Todo

> 一个用于学习 Go 的 S3 兼容对象存储。命名直接：`gos3`。
> 本文件是"技术决策 + 任务清单"，随实现推进持续更新。

---

## 1. 已敲定的技术细节

| 决策项 | 结论 | 理由 |
| --- | --- | --- |
| 项目名 / 二进制 | `gos3` | 直白，学习项目 |
| module path | `github.com/kxj/gos3`（可改） | 后续可 push |
| Go 版本 | 1.24（与宿主机一致） | 可用 `log/slog`、`errors.Join`、`http.ServeContent` 等 |
| 外部依赖 | **M1–M2 零依赖，纯标准库** | 先吃透标准库，避免依赖干扰 |
| 节点间通信 | **gRPC + protobuf**（M5 分布式阶段引入） | 简历通用技能 |
| 对象元数据 | **sidecar JSON**（`<root>/.meta/<bucket>/<object>.json`） | 可读可调试，贴近学习目标 |
| 数据存储 | 本地文件系统，`<root>/<bucket>/<object>` | 最小闭环 |
| 元数据原子写 | 临时文件 + `rename` | 复用文件系统原子性 |
| ETag | 单部分对象 = MD5 十六进制 | 与 S3 行为一致 |
| 签名 | **AWS SigV4**：Header 签名 + 预签名 URL + 流式 chunk 校验 | `mc`/`aws cli` 必需 |
| 路由 | 自研轻量路由（不用 `ServeMux`，避免路径清洗破坏签名） | 保留原始路径 |
| 配置 | flag 优先 + 环境变量兜底 | `GOS3_ROOT_USER` / `GOS3_ROOT_PASSWORD` |
| 日志 | 标准库 `log/slog` | 结构化日志 |
| 本次范围 | **M1–M2**：骨架 + 基础 S3 + SigV4，可用 `mc` 操作 | 见下文 |

### 明确不做（本阶段）

- 多部分上传的最小分片大小（5MiB）与合成体校验和验证尚未强制。
- 纠删码为整对象内存编码（非流式分块），超大对象受内存与 gRPC 消息上限（128MiB）限制。
- 版本控制分页仅支持 key-marker；无 MFA-delete。
- 分布式为静态成员、无分布式锁/leader；不做并发写同一 key 的协调。
- IAM、生命周期、事件通知。
- 流式签名的 **Trailer 变体**（`STREAMING-...-TRAILER`）：仅支持标准流式 chunk。
- 虚拟主机风格（virtual-host style）寻址：仅支持 path-style。

---

## 2. 目录结构（本次落地）

```
gos3/
├── go.mod
├── main.go                     # 入口，仅调用 cli.Main
├── Makefile
├── README.md
├── todo.md
├── internal/
│   ├── version/version.go      # 版本注入(ldflags)
│   ├── config/config.go        # 运行配置与默认值
│   ├── auth/credentials.go     # 凭证库（accessKey -> secretKey）
│   ├── sign/
│   │   ├── v4.go               # SigV4 验签（header + query）
│   │   └── chunked.go          # 流式签名 chunk 解码/校验
│   ├── store/
│   │   ├── store.go            # Store 接口 + 领域类型 + 哨兵错误
│   │   └── fs.go               # 文件系统实现（sidecar JSON）
│   ├── api/
│   │   ├── errors.go           # S3 错误模型 + context + WriteError
│   │   ├── response.go         # XML 响应结构体 + writeXML
│   │   └── handler.go          # bucket/object/list handlers
│   ├── server/
│   │   ├── server.go           # Server 组装 + 路由分发
│   │   └── middleware.go       # recover/requestID/accessLog/auth
│   └── cli/cli.go              # 子命令解析、启动、优雅停机
```

---

## 3. 任务清单

### M1 · 工程骨架

- [x] `go.mod` / `main.go` / `version` / `config` / `Makefile` / `.gitignore`
- [x] CLI：`gos3 server PATH`、`gos3 version`、`gos3 help`
- [x] 优雅停机（SIGINT/SIGTERM）
- [x] 健康检查：`/minio/health/live`、`/minio/health/ready`、`/healthz`

### M2 · 基础 S3 + SigV4

- [x] `auth.Store` 凭证库
- [x] SigV4 Header 验签（CanonicalRequest / StringToSign / SigningKey）
- [x] SigV4 预签名 URL 验签
- [x] 流式签名 `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` 解码与逐 chunk 校验
- [x] 时钟偏移校验（默认 15 分钟）
- [x] 存储层：Bucket CRUD、Object PUT/GET/HEAD/DELETE、ListObjectsV2
- [x] 批量删除 `POST /bucket?delete`（`mc rm` 使用的 DeleteObjects API）
- [x] sidecar JSON 元数据（ETag / Content-Type / 用户元数据 / ModTime）
- [x] 原子写入 + Range 下载（`http.ServeContent`）
- [x] XML 响应与 S3 错误码
- [x] 中间件：request-id / 日志 / 鉴权 / panic 恢复

### 验收（Docker 端到端，已完成 ✅ 15/15）

- [x] `Dockerfile`（多阶段：builder + runtime + verify）与 `docker-compose.yml`
- [x] `scripts/verify.sh`：SigV4 / bucket+object CRUD / 列表 / 多部分完整性 / 预签名 / 匿名拒绝 / 错误码
- [x] `make verify-docker` 一键构建并验证
- [x] 结果：15 passed, 0 failed

### M3 · 多部分上传（已完成）

- [x] `NewMultipartUpload`（Initiate）
- [x] `PutObjectPart` / `ListObjectParts`
- [x] `CompleteMultipartUpload`（合成 ETag = `md5(concat(part md5))-N`）
- [x] `AbortMultipartUpload`
- [x] `ListMultipartUploads`
- [x] 过期分片上传清理（后台 ticker，默认 24h / 6h）
- [x] 路由分发（`?uploads` / `?uploadId` / `?partNumber`）

### M4 · 纠删码（单机多盘，已完成 ✅）

- [x] `internal/erasure`：Reed-Solomon Encode/Decode（`klauspost/reedsolomon`）
- [x] `store.Erasure`：多盘分片、写 quorum、读 quorum、任一盘丢失可重建
- [x] 元数据复制到所有盘；列表/桶操作走首个盘
- [x] 多部分上传：分片暂存首盘，Complete 时整对象纠删码落盘
- [x] CLI 支持多数据目录 + `-data-shards`/`-parity-shards`
- [x] Docker 验证：4 盘（2 data + 2 parity）拔盘恢复测试
- [ ] 流式分块纠删码（当前为整对象内存编码，见限制）

### 版本控制（已完成 ✅）

- [x] 桶版本状态：enable / suspend（`GET/PUT /bucket?versioning`）
- [x] 版本化元数据：一个对象多条版本记录，最新在前
- [x] 每版本独立数据路径：`.data/<bucket>/<object>/<versionId>`（FS 与 Erasure 均支持）
- [x] 删除标记：版本化删除生成删除标记；删除指定版本为永久删除
- [x] `ListObjectVersions`（`GET /bucket?versions`）+ `versionId` 读写
- [x] 批量删除支持 `<VersionId>`（`mc rm --version-id`）
- [x] Docker 验证：enable / 多版本 / 删除标记 / 永久删除 / suspend
- [ ] `version-id-marker` 分页、MFA-delete（见 README 限制）

### M5 · gRPC 分布式（已完成 ✅）

- [x] `internal/disk`：`Disk` 抽象 + `Local`（文件系统）+ `Remote`（gRPC 客户端）
- [x] `disk.proto` + 生成代码（protoc-gen-go / protoc-gen-go-grpc）
- [x] `internal/cluster`：gRPC 磁盘服务、对等节点发现（Info）、全局盘集有序装配
- [x] `store.Erasure` 重构为面向 `[]disk.Disk`，本地/远程同一套代码
- [x] 静态成员 + 读写 quorum；杀掉整个节点后由存活节点的校验分片重建
- [x] Docker 验证：2 节点 × 2 盘，杀 node1 后经 node2 重建读取
- [ ] 分布式锁 / leader 选举（当前假设无并发写冲突）
- [ ] 流式分块纠删码（当前整对象内存编码，限制 gRPC 消息 128MiB）

### 后续里程碑（占位）

- [ ] M6 IAM / 生命周期
- [ ] M7 OTel / slog 增强 / 构造器注入重构

---

## 4. 已知取舍

1. **不用 `http.ServeMux`**：Go 1.22+ 的 ServeMux 会清洗路径（`//`、`..`、`%2F`），会破坏签名与对象名；自研分发保留原始 `r.URL.Path`。
2. **签名 canonical URI 使用 `r.URL.Path` 重新按 AWS 规则编码**：与 `minio-go` 的编码方式对齐。
3. **`Content-Length` 特殊处理**：Go 会把该头从 `r.Header` 移除，验签时从 `r.ContentLength` 取。
4. **流式签名不支持 Trailer**：新版 AWS SDK 可能使用 trailer 变体，遇到时明确报错而非静默错误。
5. **列表分页**：continuation-token 就是 base64 后的"上一个条目的排序键"；delimiter 分组后按条目分页。
