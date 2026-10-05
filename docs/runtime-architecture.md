# gos3 运行时类架构图

本文档描述 gos3 运行时的对象装配、存储后端结构，以及核心类型/接口关系。

## 1. 进程内运行时装配与请求调用链

```mermaid
flowchart TB
    CLI["main → cli.Main → runServer"] --> CFG["config.Config"]
    CLI --> LOG["*slog.Logger<br/>(telemetry.ContextHandler 包装 TextHandler)"]
    CLI --> IAM["*iam.Store"]
    CLI --> STORE["store.Store (interface)"]
    CLI --> SRV["*server.Server"]
    CLI --> HTTP["http.Server"]
    CLI --> SCAN["lifecycleLoop / cleanupLoop"]

    HTTP -->|"Handler()"| SRV
    SRV -->|"持有 api"| API["*api.Handler"]
    SRV -->|"持有 iam"| IAM
    API -->|"Store 字段"| STORE
    API -->|"IAM 字段"| IAM

    CLIENT["S3 客户端 / 浏览器"] -->|"HTTP"| MW["中间件链<br/>tracing → recoverer → requestID → accessLog → auth"]
    MW -->|"sign.VerifyHeader/Query"| IAM
    MW --> ROUTE["server.route"]
    ROUTE --> ADM["ServeAdmin / ServeUI"]
    ROUTE --> S3H["S3 handlers<br/>CreateBucket/PutObject/GetObject/..."]
    ADM --> STORE
    S3H --> STORE
    SCAN --> STORE
```

## 2. 三种存储后端的运行时对象

```mermaid
flowchart LR
    subgraph S1["单盘模式  store.NewFS"]
        FS["*store.FS"] --> FSD[".meta / .data / .multipart<br/>普通目录"]
    end

    subgraph S2["纠删码模式  store.NewErasure"]
        ER["*store.Erasure"] --> ENC["*erasure.Encoder"]
        ENC --> RS["reedsolomon.Encoder"]
        ER --> DL1["*disk.Local d1"]
        ER --> DL2["*disk.Local d2"]
        ER --> DL3["*disk.Local d3"]
        ER --> DL4["*disk.Local d4"]
    end

    subgraph S3["分布式模式  cluster.Build"]
        CL["*cluster.Cluster"] --> LL["*disk.Local 本节点盘"]
        CL --> RM["*disk.Remote"]
        RM -->|"gRPC DiskService"| PS["peer *disk.Server"]
        PS --> PL["peer *disk.Local"]
        CL --> GSRV["grpc.Server"]
    end
```

## 3. 核心类型 / 接口类图

```mermaid
classDiagram
    class Store {
        <<interface>>
        +MakeBucket(ctx, bucket)
        +PutObject(...)
        +GetObject(ctx, bucket, object, versionID)
        +ListObjects(ctx, bucket, opts)
        +ListObjectVersions(...)
        +NewMultipartUpload(...)
        +CleanupStaleUploads(...)
    }
    class FS {
        -string root
        -slog.Logger log
    }
    class Erasure {
        -[]disk.Disk disks
        -erasure.Encoder encoder
        -int dataShards
        -int parityShards
        -slog.Logger log
    }
    class Disk {
        <<interface>>
        +ID() string
        +ReadFile(ctx, path)
        +WriteFile(ctx, path, data)
        +DeleteFile(ctx, path)
        +DeleteDir(ctx, path)
        +MakeDir(ctx, path)
        +Stat(ctx, path)
        +ListDir(ctx, path)
        +Walk(ctx, path)
        +Health(ctx)
    }
    class Local {
        -string id
        -string root
    }
    class Remote {
        -string id
        -int32 drive
        -DiskServiceClient client
    }
    class DiskServer {
        +UnimplementedDiskServiceServer
        -string address
        -[]disk.Disk disks
    }
    class Encoder {
        -int dataShards
        -int parityShards
        -reedsolomon.Encoder enc
        +Encode(data)
        +Decode(shards, size)
    }
    class Cluster {
        -[]disk.Disk disks
        -grpc.Server grpcServer
        -[]ClientConn conns
    }
    class IAMStore {
        -sync.RWMutex mu
        -string dir
        -map~string,User~ users
        -map~string,Policy~ policies
        -auth.Credentials root
        +Get(accessKey) Credentials
        +IsAllowed(accessKey, action, resource)
    }
    class CredentialsProvider {
        <<interface>>
        +Get(accessKey) Credentials
    }
    class User {
        +string AccessKey
        +string SecretKey
        +[]string Policies
        +string Status
    }
    class Policy {
        +string Version
        +Statement[] Statement
    }
    class Statement {
        +string Effect
        +stringOrSlice Action
        +stringOrSlice Resource
    }
    class Server {
        -api.Handler api
        -iam.Store iam
        -slog.Logger logger
        -http.Handler handler
    }
    class Handler {
        +store.Store Store
        +iam.Store IAM
        +string Region
        +string OwnerID
        +string RootUser
        +string RootPass
        +slog.Logger Logger
    }
    class SignResult {
        +auth.Credentials Credentials
        +bool Streaming
        +[]byte SigningKey
        +string Scope
        +string Signature
    }
    class LifecycleConfiguration {
        +Rule[] Rules
        +Validate()
        +Expired(key, modTime, now)
    }
    class Rule {
        +string Status
        +Filter Filter
        +Expiration Expiration
    }

    Store <|.. FS
    Store <|.. Erasure
    Erasure *-- Encoder
    Erasure o-- "1..*" Disk
    Disk <|.. Local
    Disk <|.. Remote
    Remote ..> DiskServer : gRPC 调用
    DiskServer o-- "*" Local
    Cluster o-- "*" Disk
    Cluster o-- DiskServer : 本节点暴露
    IAMStore ..|> CredentialsProvider
    IAMStore *-- "0..*" User
    IAMStore *-- "0..*" Policy
    Policy *-- "1..*" Statement
    Server *-- Handler
    Handler --> Store
    Handler --> IAMStore
    Server ..> SignResult : auth 中间件
    Store ..> LifecycleConfiguration
    LifecycleConfiguration *-- Rule
```

关系图例：`<|..` 实现接口、`..|>` 实现接口、`*--` 组合、`o--` 聚合、`-->` 关联、`..>` 依赖。

## 要点

- `store.Store` 是唯一存储抽象，运行期只会实例化为 `*store.FS`（单盘）或 `*store.Erasure`（多盘/分布式）。
- `disk.Disk` 把「本地目录」和「远端节点盘」统一，`*store.Erasure` 完全不感知是本地还是 gRPC。
- `*iam.Store` 同时是 `sign.CredentialsProvider`，签名校验与鉴权共用同一份内存状态。
- `*api.Handler` 是 HTTP 处理器集合，`*server.Server` 只负责装配与路由/中间件。
