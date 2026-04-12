# DataImport

`DataImport` 是一个围绕 PostgreSQL、Kafka、Elasticsearch 的数据导入项目。

当前主要入口：

- `go run ./cmd/batchinsert`：向 PostgreSQL 写入测试数据
- `go run ./cmd/statistics`：统计待处理数据量
- `go run ./cmd/syncES`：执行全量导入链路，将 PostgreSQL 中的文章数据写入 Elasticsearch

配置文件位于 `config/config.yaml`，索引 mapping 位于 `config/article.json`。

## 项目结构

```text
.
├── cmd
│   ├── batchinsert
│   ├── statistics
│   └── syncES
├── config
│   ├── article.json
│   └── config.yaml
├── internal
│   ├── db
│   │   ├── postgresql
│   │   └── redis
│   ├── pkg
│   │   ├── config
│   │   ├── es
│   │   ├── kafka
│   │   └── utils
│   └── service
│       └── syncES
└── README.md
```

## syncES 概览

`syncES` 负责把 PostgreSQL 中 `article` 表的快照数据，全量同步到 Elasticsearch。

这条链路是：

`PostgreSQL -> Producer -> Kafka topic(full_load) -> Consumer work pool -> Elasticsearch`

同时还带一个 DLQ topic，用于记录写 ES 失败的 batch。

核心目标：

- 保证全量任务有明确的快照边界，不把运行中新增数据混进当前任务
- 使用 Kafka 解耦读取和写入，削峰填谷
- 使用 consumer 内部 work pool 提高单个 consumer 实例的处理吞吐
- 手动管理 offset，保证异步处理下仍然只提交每个 partition 已连续完成的 offset
- 在任务中断时保留 checkpoint，支持从上次已发布位置继续执行

## syncES 运行流程

`go run ./cmd/syncES` 后，主流程如下：

1. 初始化 PostgreSQL、Kafka、Elasticsearch 客户端
2. 读取本地 checkpoint
3. 如果存在 checkpoint，则恢复上次任务；否则查询当前 `article` 表的 `MAX(id)` 作为本次任务的 `snapshot_max_id`
4. 根据配置重建 Kafka 主 topic 和 DLQ topic，并等待 topic ready
5. 根据配置重建 Elasticsearch 索引
6. 启动 producer，从 PostgreSQL 按 `id` 递增分页读取快照范围内的数据并写入 Kafka
7. 启动多个 consumer 实例，同属一个 consumer group
8. 每个 consumer 内部再启动一个 work pool，异步处理消息并写入 Elasticsearch
9. 处理成功后按 partition 连续推进 offset，只提交已连续完成的最大 offset
10. 如果写 ES 失败，则把失败 batch 投递到 DLQ
11. producer 结束后等待 consumer 排空
12. 正常完成则删除 checkpoint；异常退出则保存 checkpoint

## 设计点

### 1. 快照边界

全量导入不是无限追平，而是先固定一个快照上界：

- 新任务启动时先查询 `SELECT COALESCE(MAX(id), 0) FROM article`
- 这个值记为 `snapshot_max_id`
- producer 只读取 `id > start_id AND id <= snapshot_max_id` 的数据

这样可以保证本次任务处理的是一个确定范围，避免运行期间新写入的数据影响任务闭环。

### 2. Producer 设计

producer 的职责只有两件事：

- 按 `id` 顺序分页读取 PostgreSQL
- 把每一页组装成一个 `FullLoadBatch` 后发布到 Kafka

batch 中会记录：

- `batch_id`
- `start_id`
- `end_id`
- `source`
- `created_at`
- `records`

这样 consumer 侧不用再回查数据库，拿到消息即可直接写 ES。

### 3. Kafka 作为缓冲层

Kafka 在这条链路里承担的是削峰和解耦：

- PostgreSQL 读取速度和 Elasticsearch 写入速度可以解耦
- consumer 挂掉后可以重新消费
- 异步处理时可以通过 partition 做横向分摊
- 写 ES 失败时可以转入 DLQ，而不是阻塞整条主链路

当前主 topic 默认是 `full_load`，DLQ topic 默认是 `full_load_dlq`。

### 4. 单 consumer 内部 work pool

consumer 数量受 Kafka partition 数限制，但单个 consumer 内部的处理并发不一定受这个限制。

因此当前实现采用两层并发：

- 第一层：同一个 consumer group 中多个 consumer 实例并行消费不同 partition
- 第二层：每个 consumer 实例内部再启动多个 goroutine 异步处理消息

这样做的目的不是突破 Kafka 的拉取并行度上限，而是提升单 consumer 拿到消息后的处理吞吐，尤其适合下游是 ES bulk 写入这种 I/O 型操作。

### 5. 手动 offset 提交

由于 consumer 内部是异步处理，不能在拿到消息后立即提交 offset，否则 worker 还没真正写完 ES。

当前 offset 策略是：

- 每条消息进入 worker 前，先登记为 partition 维度的 in-flight
- worker 完成后，把对应 offset 标记为 completed
- 只有当某个 partition 上从 `nextOffset` 开始的一段 offset 连续完成时，才推进可提交 offset
- 按批次或定时提交这些已连续完成的 offset

这套设计的目的是在异步处理下仍然保持 at-least-once 语义，并尽量减少重复消费窗口。

### 6. Elasticsearch 写入

consumer 收到的是一个 `FullLoadBatch`，会直接转换成 bulk 文档列表并调用 ES bulk API。

当前特性：

- 每个文档都带固定 `_id`
- 同一条记录重复消费时会走覆盖写，而不是生成重复文档
- 失败 batch 会进入 DLQ
- 写 ES 带有限次重试和重试间隔

这意味着系统默认接受 Kafka 常见的 at-least-once 语义，并利用 ES `_id` 降低重复消费带来的副作用。

### 7. DLQ 设计

当 batch 写 ES 失败时，不会直接让整条主链路永远卡死，而是：

- 记录失败 batch 的基本信息
- 收集失败文档 ID
- 把失败原因和 batch 元数据投递到 `full_load_dlq`

这样主链路可以继续向前推进，失败数据则进入单独的补偿路径。

### 8. Checkpoint 恢复

checkpoint 文件为项目根目录下的 `full_load_checkpoint.json`。

保存时机：

- 收到取消信号
- 任务异常退出但 producer 已经成功发布过部分 batch

checkpoint 中保存：

- `job_id`
- `snapshot_max_id`
- `last_published_end_id`
- `updated_at`

恢复时会继续沿用原来的 `job_id` 和 `snapshot_max_id`，并从 `last_published_end_id` 之后继续生产，避免把已经发布到 Kafka 的数据重复扫描一遍。

### 9. Topic 和索引重建

如果配置开启：

- `RecreateTopics = true`
- `DeleteIndexFirst = true`
- `CreateIndex = true`

则每次任务启动时会：

- 删除并重建 Kafka topic
- 等待 topic 进入 ready 状态
- 删除并重建 Elasticsearch 索引

这里重建 topic 使用的是“删除后轮询重建成功”策略，而不是单纯依赖 metadata 消失，以适配 Kafka 删除 topic 的异步传播行为。

### 10. 应用层背压

当前 `syncES` 里额外实现了一层应用层背压，避免 producer 无限快于 consumer。

控制方式：

- 统计 `publishedBatches`
- 统计 `consumedBatches`
- 当两者差值超过 `MaxPipelineBatches` 时，producer 暂停继续读库和发 Kafka
- 每隔 `ProducerThrottleWait` 再检查一次

这个背压不是 Kafka broker 级限流，而是任务内的管道水位控制，目的是把 backlog 限制在一个可控范围内。

## 关键配置

`syncES.DefaultConfig()` 中当前比较关键的参数有：

| 配置项 | 作用 |
| --- | --- |
| `PageSize` | PostgreSQL 单次读取的记录数 |
| `ConsumerWorkers` | 同一个 consumer group 中启动多少个 consumer 实例 |
| `TopicPartitions` | Kafka 主 topic 分区数，决定 group 内最大有效消费并行度 |
| `CommitBatchSize` | 手动提交 offset 的批次阈值 |
| `CommitInterval` | 手动提交 offset 的时间阈值 |
| `ConsumerBatchSize` | 单次 poll 最多拉取多少条消息 |
| `ConsumerPoolSize` | 单个 consumer 内部 work pool 的 worker 数 |
| `ConsumerQueueSize` | 单个 consumer 内部待处理队列长度 |
| `ProducerMaxPending` | Kafka producer 本地待确认消息上限 |
| `MaxPipelineBatches` | 允许 producer 超前 consumer 的最大 batch 数 |
| `ProducerThrottleWait` | 触发应用层背压后的等待间隔 |
| `ESRetryWait` | ES 写入失败后的重试间隔 |

调参时建议按下面顺序进行：

1. 先确认 ES bulk 写入是不是瓶颈
2. 再调整 `ConsumerPoolSize`
3. 再调整 `TopicPartitions` 和 `ConsumerWorkers`
4. 最后再看 `PageSize`、`ConsumerBatchSize`、`CommitBatchSize`

## 适用语义

当前实现更接近：

- Kafka 消费语义：at-least-once
- Elasticsearch 写入语义：基于固定 `_id` 的幂等覆盖

这意味着在 rebalance、崩溃恢复、手动重试等场景下，单条消息可能被重复处理，但最终写入结果应该保持可接受的一致性。

## 当前边界

当前方案适合“全量导入 + 批量写 ES”这类任务，但也有明确边界：

- consumer 实例数仍然受 topic partition 数限制
- 单 consumer 内部并发提高的是处理吞吐，不保证同 partition 的严格业务顺序
- 如果 ES 明显慢于 producer，最终瓶颈仍然在 ES
- 应用层背压只对当前进程内的 producer 生效，不是通用的跨进程分布式背压

如果后续要继续增强，优先级通常是：

- 为 consumer 增加基于队列水位的 pause/resume
- 增强 ES 限流和失败分类
- 接入 Kafka lag 监控
- 引入自动扩容和更明确的运行指标

## 相关代码

- `internal/service/syncES/coordinator.go`：主流程编排
- `internal/service/syncES/reader.go`：分页读取 PostgreSQL
- `internal/service/syncES/handler.go`：消费消息并写入 ES / DLQ
- `internal/service/syncES/checkpoint.go`：checkpoint 持久化与恢复
- `internal/service/syncES/topic.go`：topic 与索引初始化
- `internal/service/syncES/types.go`：配置、消息结构、统计定义
- `internal/pkg/kafka`：Kafka producer、consumer、admin 封装
- `internal/pkg/es`：Elasticsearch 写入封装
