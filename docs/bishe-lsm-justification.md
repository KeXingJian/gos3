# 为什么选 LSM 树作为元数据存储引擎核心数据结构（毕设论证）

> 用途：毕设论文 / 答辩的「存储引擎选型」章节素材。
> 对应实现：internal/catalog/engine（退化 LSM：内存 skip list + WAL + 全量快照）。
> 参照案例均已核对官方文档/博客，附链接。

---

## 一、核心结论

对象存储的元数据索引是典型的「**写密集 + 需要有序范围查询 + 要求崩溃可恢复**」负载，
LSM 树把随机写变成顺序写，是这三者最省的折中。

---

## 二、五个理由

### 1. 负载是写密集的（最关键）

索引的写入由每个 PUT / DELETE / CompleteMultipartUpload 触发，写入频率远高于查询
（查询是低频管理操作 + ListObjects）。LSM 树的核心设计就是把「写」做便宜：

- 写先落内存 memtable + 追加式 WAL，O(1) 顺序追加，没有随机原地更新；
- B+ 树每次写要定位叶子页、可能页分裂、多次随机写、还要写 journal。

### 2. 顺序 I/O 对磁盘/SSD 友好

LSM 的写入都是追加，SSTable 是只读不可变文件；WAL 用组提交摊薄 fsync。
B+ 树的原地更新天然产生随机 I/O。

### 3. 天然支持有序范围/前缀查询

二级索引（contentType、时间、大小分档、tag）全靠 []byte 前缀范围扫描。
LSM 的 SSTable 按 key 有序落盘，范围扫描和归并是它的本行。
哈希表无序做不了范围查询，直接出局。

### 4. 崩溃恢复简单

不可变 SSTable + WAL 重放 = 恢复就是「重放日志」，语义干净；
B+ 树要做 redo/undo + 页级一致性，复杂得多。

### 5. 本项目的落点：退化 LSM

实现为退化 LSM：memtable（内存 skip list）+ WAL + 单层全量快照（唯一 SSTable），
没有多级 compaction——因为索引是旁挂派生数据、可全量重建，不需要完整 LSM 的多层设计，
但保留了「顺序写 + 有序读 + 易恢复」的全部核心收益。

理论出处：LSM-Tree 出自 O'Neil、Cheng、Gawlick、O'Neil 1996 年论文
《The Log-Structured Merge-Tree (LSM-Tree)》。

---

## 三、对比表

| 维度 | LSM 树 | B+ 树 | 哈希表 |
| --- | --- | --- | --- |
| 写性能 | 顺序追加，优 | 随机原地 + 页分裂 | 点写快，但无序 |
| 点查 | 中（可能跨层） | 优 | 最优 O(1) |
| 范围/前缀查询 | 优 | 优 | 不支持 |
| 崩溃恢复 | 简单（WAL 重放） | 需 redo/undo | 需全量落盘 |
| 本场景适配 | 写多读少+范围查，最匹配 | 更适合读密集 | 无法满足范围查 |

---

## 四、参照案例（开源项目实证）

### 第一档 · 同域直接参照（分布式对象存储用 LSM 存元数据）

**1. Ceph（BlueStore + RocksDB）——最像本项目**

Ceph 是业界标杆级分布式对象存储，BlueStore 后端用 RocksDB（LSM）管理内部元数据。
官方文档：

> BlueStore uses RocksDB's key/value database to map object names to block locations on disk.

对象网关 RGW 的场景与本项目元数据检索几乎一致，官方博客：

> When using the Ceph Object Gateway (RGW), all rich metadata associated with an object,
> such as its name, size, ACLs, and custom user tags, is stored in RocksDB… the bucket index,
> which lists all objects within a bucket, is maintained as omap entries in this same database.

对象名、大小、ACL、自定义 tag、bucket 索引——正是本项目 Doc 结构里的字段，
Ceph 就存在 LSM 引擎（RocksDB）里。这是最有力的论据。

- https://docs.ceph.com/en/latest/rados/configuration/storage-devices/
- https://ceph.com/en/news/blog/2025/rocksdb-compression-ftw

**2. CockroachDB + Pebble——同语言（Go）的 LSM**

CockroachDB 是分布式数据库，自研 Pebble（纯 Go 的 LSM 引擎）替换 RocksDB。官方文档：

> Pebble uses a Log-structured Merge-tree (LSM) to manage data storage…
> SST files are immutable.

同时证明两点：① LSM 是分布式系统存储引擎的主流选择；
② Go 生态里也有成熟 LSM 实现（对应本项目用 Go 自研 LSM）。

- https://www.cockroachlabs.com/docs/stable/architecture/storage-layer

### 第二档 · 经典 LSM 引擎（理论出处）

| 项目 | 组织 | 说明 |
| --- | --- | --- |
| LevelDB | Google | LSM 的奠基性实现，SSTable 术语来源 |
| RocksDB | Meta | LevelDB 强化版，被 Ceph / TiKV 等内嵌 |
| Badger | Dgraph | 纯 Go 的 LSM KV（与 Pebble 并列的 Go 代表） |

### 第三档 · 分布式 NoSQL / 时序（旁证）

| 项目 | 说明 |
| --- | --- |
| Cassandra / HBase | memtable + SSTable + compaction，经典 LSM 架构 |
| TiKV（PingCAP） | 用 RocksDB，JuiceFS 可直接拿它当分布式元数据引擎 |

---

## 五、论文表述（收口句）

> LSM 树不是凭空选择——Ceph（分布式对象存储）用 RocksDB 存对象元数据与 bucket 索引，
> CockroachDB 用纯 Go 的 Pebble 作为存储引擎，说明在「写密集 + 需要有序范围查询」的
> 对象存储/分布式系统元数据场景里，LSM 树是经大规模生产验证的主流选择；
> 本项目基于同一理由，实现了一个退化 LSM（memtable + WAL + 单层快照）。

---