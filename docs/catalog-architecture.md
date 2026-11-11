# gos3 · 元数据存储引擎 架构图

> 配套文档：docs/metadata-catalog.md（总体设计）、docs/catalog-storage-engine.md（引擎层深挖）。
> 图例：实线=同步调用/数据流，虚线=异步，圆角框=进程/服务，菱形=决策，圆柱=持久化。

---

## 图 1 · 总体挂载：Catalog 如何插进现有 gos3

```mermaid
flowchart LR
    subgraph CLIENT[客户端]
        S3[S3 客户端 / mc]
        ADM[Console UI / 管理脚本]
    end
    subgraph NODE[gos3 节点]
        subgraph API[api.Handler]
            HO[对象 API<br/>PUT/GET/DELETE/LIST]
            HA[管理 API<br/>/gos3/admin/catalog/*]
        end
        IDX["catalog.Indexed<br/>装饰器 implements store.Store"]
        ST["store.Erasure / store.FS<br/>sidecar 元数据 + 数据"]
        LC["lifecycleLoop<br/>周期扫描"]
        subgraph CAT[catalog Engine]
            ENG["engine.Engine<br/>sequencer + applier 编排"]
            MEM[(内存索引 memoryIndex)]
            FS[(WAL + 快照 + MANIFEST)]
        end
    end
    S3 --> HO --> IDX -->|同步透传| ST
    ADM --> HA -->|读视图| MEM
    IDX -.->|非阻塞 Enqueue| ENG
    ENG --> MEM
    ENG --> FS
    LC -->|走 store.Store 接口| IDX
```

- `catalog.Indexed` 内嵌 `store.Store`，未覆盖的方法原样透传；只拦写类方法投递事件。
- 生命周期扫描器 `lifecycleLoop` 收到装饰后的 store，删除自动入索引，无需额外埋点。
- 查询走 `HA -> MEM` 只读路径，**不回表 sidecar**。

---

## 图 2 · 写路径：从 PUT 到索引可见

```mermaid
flowchart TD
    P["PUT / Delete / CompleteMultipart 完成<br/>得到 ObjectInfo"] -->|装饰器返回前| E["Enqueue Event<br/>非阻塞"]
    E --> R{ring buffer<br/>cap 64k 满?}
    R -->|否| SQ["Sequencer x1<br/>Seq = seq++<br/>先落 WAL 缓冲再分发"]
    R -->|是| DROP["dropped++ 置 lag=dirty<br/>交给 reconcile 补"]
    SQ --> WB["WAL 缓冲<br/>组提交 K=256 条 / T=20ms"]
    WB -->|fsync| WALF[(wal-*.log 段)]
    SQ -->|"route: fnv key % G"| SH["Applier xG<br/>每分片串行"]
    SH --> IX["memoryIndex.apply<br/>按 shardSeq 幂等<br/>维护主键 + 二级索引"]
    IX --> CHK{"快照阈值?<br/>docs>=1M 或 WAL>=256MiB"}
    CHK -->|是| SNAP["快照轮转<br/>写新 gen + 截断 WAL"]
    CHK -->|否| DONE["可见（秒级 lag）"]
```

关键顺序：**先 WAL 缓冲、再分发 apply**，保证任意时刻「WAL 的 seq 前缀 >= 已 apply 的 seq」，
可见性先于持久化（apply 不等 fsync），崩溃丢尾由 reconcile 补。

---

## 图 3 · 内存索引：一个 skip list 原语，五个实例

```mermaid
flowchart LR
    DOC["一条 Doc<br/>bucket+key+versionID"] --> OM["omap skip list 原语<br/>Get / Upsert / Delete / Seek 前缀"]
    OM --> P[(primary<br/>bucket+NUL+key+NUL+versionID)]
    OM --> C[(ct<br/>bucket+NUL+contentType+...)]
    OM --> T[(timeIx<br/>bucket+NUL+modTimeNano+...)]
    OM --> S[(sizeIx<br/>bucket+NUL+sizeBucket+...)]
    OM --> M[(tagIx / metaIx<br/>bucket+NUL+k+NUL+v+... 多值)]
    P -.->|"IsLatest 标志<br/>非 versionID 排序"| L[最新版本判定]
```

- 所有索引是**同一个 `omap`（skip list）的实例**，键统一 `[]byte`、NUL 分隔，查询=前缀范围扫描。
- 主键与二级索引存的是**同一个 `*Doc` 指针**，不复制；更新时用旧 Doc 反查摘除旧二级条目。
- `IsLatest` 是布尔字段（对应 sidecar `fsMeta.Versions[0]`），versionID 是随机 hex，不能当排序依据。

---

## 图 4 · 磁盘布局与文件生命周期

```mermaid
flowchart TD
    ROOT["data-dir-0/.catalog"] --> MNF[(MANIFEST<br/>gen / appliedSeq / 快照与段清单)]
    ROOT --> SNAP[(snap-42.sst<br/>按 PK 升序二进制流 + 尾部 crc)]
    ROOT --> WAL[(wal-42-000001.wal<br/>追加式 记录=seq+crc+op+doc)]
    ROOT --> TMP[(tmp/ 崩溃残留 启动清空)]
    MNF -.->|唯一指针 决定谁有效| SNAP
    MNF -.->|唯一指针 决定谁有效| WAL
    WAL -->|"size>=64MiB 或 docs>=100k"| SEAL[封口 seal]
    SEAL -->|快照阈值| ROTATE[写快照 -> 截断 WAL 段]
```

- MANIFEST 是唯一真相指针，tmp+rename 原子写（复用 `writeJSONFile` 思路）。
- WAL 记录带 crc32；坏记录/gap 在恢复时触发全量重建，不试图修复。

---

## 图 5 · 启动恢复决策树

```mermaid
flowchart TD
    START[启动 Recover] --> READ{读 MANIFEST 成功?}
    READ -->|否| REBUILD[全量重建]
    READ -->|是| LOADSNAP[载入快照 建 primary 索引]
    LOADSNAP --> REPLAY["按 seq 重放 WAL<br/>seq > snapshot.maxSeq"]
    REPLAY --> CRC{"每条 crc 校验<br/>seq 连续?"}
    CRC -->|"坏记录 / gap"| REBUILD
    CRC -->|通过| VERIFY{"appliedSeq 吻合?"}
    VERIFY -->|是| READY[ready 恢复耗时秒级]
    VERIFY -->|否| REBUILD
    REBUILD --> WALK["walkMetas / walkMetasQuorum<br/>读 sidecar 全量"]
    WALK --> NEWSNAP["写新 gen 快照 + MANIFEST<br/>原子切换"]
    NEWSNAP --> READY
```

---

## 图 6 · 原子切换协议（快照/重建共用）

```mermaid
sequenceDiagram
    participant A as apply / 重建线程
    participant T as tmp 目录
    participant D as 正式目录
    participant M as MANIFEST
    participant R as 读侧
    A->>T: 写 snap-newGen.sst + fsync
    T->>D: rename 到正式目录（原子）
    A->>M: 写 MANIFEST.tmp + fsync
    M->>M: rename 覆盖 MANIFEST（原子点）
    R->>R: atomic.Pointer swap 到新 gen
    A->>D: 删除旧快照 + 已截断 WAL 段
    Note over R: 任一步崩溃：重启后指向旧 gen 或新 gen，两者都可正确恢复
```

---

## 图 7 · 集群形态：每节点全量索引

```mermaid
flowchart LR
    subgraph N1[节点 1]
        A1[(.catalog 全量)]
    end
    subgraph N2[节点 2]
        A2[(.catalog 全量)]
    end
    subgraph N3[节点 3]
        A3[(.catalog 全量)]
    end
    Q[查询请求] --> N1
    Q --> N2
    Q --> N3
    W["writeMetaAll<br/>元数据全盘冗余"] -.->|"每个节点读到完整命名空间"| N1
    W -.-> N2
    W -.-> N3
```

因为 `writeMetaAll` 把对象 meta 写到所有盘，每个节点本就拥有完整命名空间，索引无需分片/Raft；
代价是索引写放大 xN（只是元数据，可接受）。查询打任意节点答案一致（允许秒级 lag）。

> **实现备注（C4 后）**：图 7 是目标形态，当前尚未实现跨节点事件复制/reconcile。
> 每个节点的 `Indexed` 只收到本节点 API 写入的事件；节点 A 写入、节点 B 查询时，
> B 的索引不会自动更新（只有 B 启动时从 sidecar 全量重建可补历史）。该问题属于 C6。
> 另外，当前 applier 是单队列单 worker，图 2 的 `Applier xG` 是后续优化目标。

---
