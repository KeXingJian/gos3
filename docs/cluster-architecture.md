# gos3 分布式集群网络架构

本文档描述 `../docker-compose.yml` 中 2 节点 gos3 集群（node1、node2）加验证容器（verify）的网络与存储架构。

## 架构图

```mermaid
graph TB
    subgraph Verify["verify 容器"]
        VS["verify-cluster.sh<br/>PHASE=write<br/>S3_ENDPOINT=http://node1:9000"]
    end

    subgraph N1["node1 容器"]
        N1H["S3 HTTP :9000<br/>(Health/ListBuckets/...)"]
        N1G["internode gRPC :9001<br/>advertise=node1:9001"]
        N1D1["local disk /data/d1"]
        N1D2["local disk /data/d2"]
        N1H --- N1G
        N1G --- N1D1
        N1G --- N1D2
    end

    subgraph N2["node2 容器"]
        N2H["S3 HTTP :9000"]
        N2G["internode gRPC :9001<br/>advertise=node2:9001"]
        N2D1["local disk /data/d1"]
        N2D2["local disk /data/d2"]
        N2H --- N2G
        N2G --- N2D1
        N2G --- N2D2
    end

    VOL1[("volume: node1-data")]
    VOL2[("volume: node2-data")]

    VS -->|"S3 API (S3_ENDPOINT)<br/>Basic/root 凭据"| N1H
    VS -. "depends_on: service_healthy" .-> N1H
    VS -. "depends_on: service_healthy" .-> N2H

    N1G <-->|"gRPC DiskService<br/>-peers node2:9001"| N2G

    N1D1 --- VOL1
    N1D2 --- VOL1
    N2D1 --- VOL2
    N2D2 --- VOL2

    classDef store fill:#eef,stroke:#557;
    classDef disk fill:#efe,stroke:#575;
    classDef verify fill:#fee,stroke:#755;
    class N1H,N2H,N1G,N2G store;
    class N1D1,N1D2,N2D1,N2D2 disk;
    class VS verify;
```

## 关键点

- **验证流量**：`verify` 容器通过 `S3_ENDPOINT=http://node1:9000` 访问 node1 的 S3 HTTP 接口。
- **集群内部通信**：node1 与 node2 的 `:9001` 之间通过 gRPC `DiskService` 互访，用于跨节点的磁盘读写（分片写入/读取、元数据）。
- **节点发现**：
  - `-advertise node1:9001` / `-advertise node2:9001`：本节点对外广播的 gRPC 地址，即对端拨号使用的地址。
  - `-peers node2:9001` / `-peers node1:9001`：要连接的对端 gRPC 地址，两节点互为 peer。
- **存储布局**：每节点 2 个本地目录，共 4 块盘；未指定分片参数时自动推导为 `data=2, parity=2`（见 `../internal/cli/cli.go` 的 `layout`）。
- **持久化**：`node1-data`、`node2-data` 两个命名卷分别挂载到各节点的 `/data`。
- **启动依赖**：`verify` 通过 `depends_on: condition: service_healthy` 等待两节点 `/healthz` 健康（S3 前先就绪）后再执行验证脚本。
- **环境变量**：`GOS3_ROOT_USER`/`GOS3_ROOT_PASSWORD` 为 root 凭据（默认 `minioadmin`）；`GOS3_OTEL_EXPORTER=stdout` 将链路追踪输出到标准输出。
