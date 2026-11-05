# 对象存储元数据查询特性 · MinIO 付费版解读 与 文件元数据参考

> 目标：为 gos3 的 Catalog（docs/metadata-catalog.md）补充两块背景——① MinIO 付费版
> 元数据查询特性到底做了什么；② 不同文件类型各自携带哪些元数据，作为"要不要提取内容元数据"
> 的决策参考。
>
> 现状一句话：gos3 的 Catalog 设计只索引 S3 系统/用户元数据，明确不做"对象内容索引"。
> 本文第二部分（文件内部元数据）属于内容解析范畴，先记录清楚，是否纳入后续里程碑另议。

---

## 第一部分：MinIO 付费版的"元数据查询"是什么

### 1.1 名称与定位

MinIO 的这个能力叫 Catalog，只出现在商业版里：

| 时间 | 产品名 | 说明 |
| --- | --- | --- |
| 2024-03 前 | MinIO（社区版 AGPLv3） | 无 Catalog，只有按 key 排序的 ListObjects |
| 2024-03 | MinIO Enterprise Object Store | 首批六个商用特性之一：Catalog / Data Firewall / KMS / Cache / Observability / Console |
| 后来 | AIStor（独立二进制，商业许可） | Catalog 延续，作为 Enterprise 层特性，不可单独购买，按容量计费 |

社区版 MinIO（以及 gos3 对齐的免费 S3 语义）只有 ListObjects/ListObjectsV2，
它只支持 prefix + delimiter + 按 key 字典序排序。任何"按大小、时间、类型、tag 查对象"的
需求，客户端都得自己跑全量 LIST，再逐个 HEAD 拿元数据。这就是 Catalog 要消灭的痛点。

### 1.2 解决的问题

MinIO 官方给出的量化：十亿个对象，LIST 要跑 100 万次才能扫完，而且扫完得到的只是
key 列表，元数据还得再 HEAD 一次；拿到元数据后依然没法查询——除非自己搭一套元数据数据库
（官方原话：这在 EB 级规模下"必然失败"）。连 AWS 的 S3 Inventory 都是
"跑命令 → 导出 CSV → 上传 → 用 Presto 查"的拼凑。

Catalog 的卖点：

- 实时自动索引：对象一写入元数据即入索引，无需任何手工导出/建库步骤，永远"最新"。
- 无需外部服务：独立于 Elasticsearch / 数据库，不引入额外运维负担。
- EB 级可查：针对"数千亿对象"规模设计，查询秒级返回（而非小时级全遍历）。
- 熟悉接口：GraphQL，管理员在 Global Console 里点选即可查。

### 1.3 能查什么（查询面）

Catalog 本质是对对象命名空间 + 对象元数据建二级索引，支持按下列属性过滤/聚合/排序：

| 维度 | 字段示例 | 说明 |
| --- | --- | --- |
| 命名空间 | key / prefix / bucket | 按前缀过滤，仍保持字典序 |
| 时间 | 创建时间 / LastModified | "某日期之后新增了多少对象" |
| 大小 | size | "大于 1 MiB 的对象有哪些"（区间比较） |
| 类型 | Content-Type | 按 MIME 过滤（image/png 等） |
| 用户标签 | Tag 的 key/value | tag.env=prod |
| 用户元数据 | x-amz-meta-* | 自定义元数据键值 |
| 其他系统属性 | ETag / VersionID / StorageClass / 复制状态 / 对象锁（保留期、合法持有） | 治理、合规、审计 |

典型 GraphQL 查询意图（官方博客示例）：

- "哪些对象的 key 带某前缀？"
- "这个日期之后新增了多少对象？"
- "有多少对象大于某个大小？"
- 按 content-type / storage-class / size 分桶做聚合统计（用于容量分摊、成本核算）

当前实现细节（写进 gos3 决策时的对照）：

- 索引驻内存服务查询，备份到对象存储；磁盘化索引是规划方向（意味着官方也受内存上限约束）。
- 响应格式为 JSON。
- 一致性为近实时（索引与存储存在极短 lag，靠自动重建/自愈补齐）。

### 1.4 应用场景

| 场景 | 例子 |
| --- | --- |
| 数据治理 / 合规 | 找出所有带敏感 tag 的对象、审计某 bucket 近 30 天写入 |
| 成本核算 | 按 storage-class / content-type 聚合容量与对象数 |
| 运营自动化 | 批量定位并清理过期/超大对象、按属性触发生命周期 |
| 审计 | 查询"谁的对象""何时写入""多大""什么类型" |

---

## 第二部分：对象/文件元数据分层

元数据分两层，务必区分，因为它们的获取成本和索引策略完全不同：

- L1 · S3 层元数据：上传时由客户端/网关带上的系统 + 用户元数据，不读文件内容，免费可得。
- L2 · 文件内部元数据：藏在文件字节里的 EXIF/ID3/XMP/文档属性，必须解析文件内容才能提取，
  这正是 gos3 Catalog 当前"明确不做"的部分。

### 2.1 L1 · S3 层（任何对象都有，gos3 Catalog 已覆盖）

| 字段 | 来源 | 索引建议 |
| --- | --- | --- |
| bucket / key | 请求路径 | 主键，天然有序 |
| size | PUT 头/内容长度 | 分档索引（见设计文档） |
| LastModified | 服务端生成 | TIME 索引 |
| ETag | 服务端计算 | 高基数，不建索引，仅过滤 |
| Content-Type | 客户端提供（可猜） | 低基数，倒排 |
| Storage-Class | 客户端指定 | 低基数，倒排 |
| VersionID | 版本化开启时生成 | 主键一部分 |
| DeleteMarker | 删除操作 | 文档字段 |
| Tags | x-amz-tagging | 低基数 key 倒排 |
| UserMeta | x-amz-meta-* | 仅白名单 key 建索引 |
| 对象锁 / 复制状态 | 系统 | 布尔/枚举字段 |

### 2.2 L2 · 文件内部元数据（内容解析，按类型）

> 提取成本说明：图片/音频/视频/PDF/Office 的元数据集中在文件头部的 tag 结构里，
> 通常只需读前几 KB 就能拿到，不必读全文件；但实现每种解析器都是独立工作量。

#### 图片

| 格式 | 元数据标准 | 关键字段 |
| --- | --- | --- |
| JPEG / TIFF | EXIF | 相机厂商/型号、拍摄时间、光圈、快门、ISO、焦距、曝光补偿、白平衡、方向、GPS（经纬度/海拔）、序列号 |
| 各类图片 | IPTC | 版权、作者/摄影师、关键词、标题、描述、拍摄地点 |
| JPEG / PNG / TIFF | XMP | 标题、描述、权利、评级、创建工具、修改历史 |
| 全部 | 技术属性 | 宽×高、位深、色彩空间（sRGB/AdobeRGB/ProPhoto）、DPI、压缩方式、是否含透明通道 |

#### 音频

| 格式 | 元数据标准 | 关键字段 |
| --- | --- | --- |
| MP3 | ID3 (v2.x) | 标题、艺术家、专辑、年份、曲目号、流派、封面（内嵌图）、歌词、注释、编码软件、版权 |
| FLAC / OGG / Opus | Vorbis Comments | 同上 + 专辑艺术家、碟号、发行日期、ISRC |
| WAV | RIFF INFO chunk | 标题、艺术家、版权 |
| M4A / AAC | MP4 (moov/udta) | 标题、艺术家、专辑、封面 |
| 全部 | 技术属性 | 时长、比特率、采样率、声道数、编解码器 |

#### 视频

| 层 | 关键字段 |
| --- | --- |
| 容器（MP4/MOV/MKV/WebM/AVI） | 标题、作者、版权、创建日期、拍摄位置（GPS）、旋转角度、编码软件 |
| 视频流 | 编解码器（H.264/H.265/VP9/AV1）、分辨率、帧率、比特率、位深、色域（HDR/HLG/Dolby Vision） |
| 音频流 | 编解码器、采样率、声道数、比特率 |
| 字幕流 | 语言、格式（SRT/ASS） |
| 全局 | 时长 |

#### 文档

| 格式 | 关键字段 |
| --- | --- |
| PDF | 标题、作者、主题、关键词、创建工具（Creator）、生成器（Producer）、创建/修改时间、页数、页面尺寸、是否加密、PDF 版本 |
| DOCX / ODT | 作者、标题、主题、关键词、分类、公司、经理、最后修改者、修订号、总编辑时间、创建/修改时间、模板、页数/字数 |
| XLSX / ODS | 作者、标题、主题、关键词、分类、最后修改者、工作表数量 |
| PPTX / ODP | 作者、标题、主题、关键词、分类、幻灯片数量 |

> Office 2007+ 内部是 OOXML（docProps/core.xml + app.xml），字段即 OOXML Core Properties：
> dc:creator / dc:title / dc:subject / dc:keywords / dc:description、cp:revision 等。

#### 电子书

| 格式 | 关键字段 |
| --- | --- |
| EPUB | 标题、作者、出版者、语言、ISBN、出版日期、描述、封面、版权（OPF metadata） |
| MOBI / AZW | 同上 + ASIN |
| PDF（作书） | 复用 PDF 属性 |

#### 邮件 / 压缩包 / 字体 / 地理（补充）

| 类型 | 关键字段 |
| --- | --- |
| EML / MSG | From、To、Cc、Bcc、Subject、Date、Message-ID、附件列表 |
| ZIP / RAR / 7z / TAR | 内含文件清单、压缩比、是否加密、注释 |
| TTF / OTF / WOFF | 字体名、设计师、版权、版本 |
| GeoTIFF / Shapefile | 坐标系（EPSG）、地理范围（bounding box） |
| CAD（DWG/DXF） | 作者、软件版本、单位 |

---

## 第三部分：对 gos3 Catalog 的落点建议

| 决策项 | 建议 | 理由 |
| --- | --- | --- |
| L1 层 | 全做，已覆盖在 metadata-catalog.md 设计里 | 零解析成本，是 MinIO Catalog 的主体 |
| L2 层（文件内部元数据） | 先不做，本文仅作参考 | 每种格式一个解析器，工作量大；且与"不索引对象内容"的既定边界冲突 |
| 折中方案（若将来要做） | 只提取 Content-Type 前缀可判定的低成本字段：图片宽高、音视频时长、PDF/Office 文档属性 | 集中在文件头，读前几 KB 即可，命中 90% 价值 |
| 提取时机 | 写路径异步提取（与 Catalog worker 同构），失败静默 | 不阻塞 PUT，与现有 write-behind 哲学一致 |

---
