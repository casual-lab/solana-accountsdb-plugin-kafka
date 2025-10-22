# Geyser-Kafka 插件改造实施检查清单

## 📋 总览

本清单用于跟踪改造工作的进度，确保所有任务按计划完成。

**项目目标**: 使 Geyser-Kafka 插件支持 5 张数据表的完整数据采集

**预计工期**: 7.5 天

**当前状态**: 🔴 未开始

---

## 阶段 1: Protobuf 定义和代码生成 (1 天)

**目标**: 定义新的消息类型并生成 Rust 代码

### 任务列表

- [ ] 1.1 在 `proto/event.proto` 中添加 `BlockMetadataEvent`
  ```protobuf
  message BlockMetadataEvent {
    uint64 slot = 1;
    string block_hash = 2;
    uint64 parent_slot = 3;
    string parent_block_hash = 4;
    string leader = 5;
    uint32 executed_transaction_count = 6;
    optional int64 block_time = 7;
    optional uint64 block_height = 8;
    repeated Reward rewards = 9;
    uint32 total_compute_units = 10;
    uint64 total_fees = 11;
  }
  ```

- [ ] 1.2 添加 `ProgramLogEvent`
  ```protobuf
  message ProgramLogEvent {
    uint64 slot = 1;
    bytes tx_signature = 2;
    uint32 tx_index = 3;
    string program_id = 4;
    string message = 5;
    uint32 log_index = 6;
    uint32 depth = 7;
    LogType log_type = 8;
  }
  ```

- [ ] 1.3 添加 `LogType` 枚举
  ```protobuf
  enum LogType {
    UNKNOWN = 0;
    INVOKE = 1;
    LOG = 2;
    DATA = 3;
    SUCCESS = 4;
    FAILED = 5;
    CONSUMED = 6;
  }
  ```

- [ ] 1.4 添加 `ChainEvent`
  ```protobuf
  message ChainEvent {
    uint64 slot = 1;
    bytes tx_signature = 2;
    uint32 tx_index = 3;
    string program_id = 4;
    bytes event_discriminator = 5;
    string event_type = 6;
    bytes raw_data = 7;
    optional string json_payload = 8;
    uint32 log_index = 9;
  }
  ```

- [ ] 1.5 更新 `MessageWrapper`
  ```protobuf
  message MessageWrapper {
    oneof event_message {
      UpdateAccountEvent account = 1;
      SlotStatusEvent slot = 2;
      TransactionEvent transaction = 3;
      BlockMetadataEvent block = 4;         // 新增
      ProgramLogEvent program_log = 5;      // 新增
      ChainEvent chain_event = 6;           // 新增
    }
  }
  ```

- [ ] 1.6 运行 `cargo build` 生成 Rust 代码

- [ ] 1.7 验证生成的代码
  - [ ] 检查 `target/debug/build/.../blockdaemon.solana.accountsdb_plugin_kafka.types.rs`
  - [ ] 确认所有新类型存在
  - [ ] 确认编译无错误

### 验收标准

- [x] 所有 Protobuf 定义正确
- [x] Rust 代码成功生成
- [x] `cargo build` 无错误

---

## 阶段 2: 配置系统扩展 (0.5 天)

**目标**: 添加新的配置选项

### 任务列表

- [ ] 2.1 扩展 `src/config.rs::ConfigFilter`
  ```rust
  pub struct ConfigFilter {
      // 现有字段...
      
      // 新增 topic 配置
      pub block_metadata_topic: String,
      pub program_log_topic: String,
      pub chain_event_topic: String,
      
      // 新增功能开关
      pub extract_program_logs: bool,
      pub extract_anchor_events: bool,
      
      // 新增过滤器
      pub event_discriminators: Vec<String>,
  }
  ```

- [ ] 2.2 更新 `ConfigFilter::default()`
  ```rust
  impl Default for ConfigFilter {
      fn default() -> Self {
          Self {
              // 现有字段...
              block_metadata_topic: "".to_owned(),
              program_log_topic: "".to_owned(),
              chain_event_topic: "".to_owned(),
              extract_program_logs: false,
              extract_anchor_events: false,
              event_discriminators: Vec::new(),
          }
      }
  }
  ```

- [ ] 2.3 扩展 `src/filter.rs::Filter`
  ```rust
  pub struct Filter {
      // 现有字段...
      
      pub block_metadata_topic: String,
      pub program_log_topic: String,
      pub chain_event_topic: String,
      pub extract_program_logs: bool,
      pub extract_anchor_events: bool,
      pub event_discriminators: HashSet<[u8; 8]>,
  }
  ```

- [ ] 2.4 更新 `Filter::new()` 构造函数

- [ ] 2.5 创建测试配置文件 `config.test.json`
  ```json
  {
    "libpath": "target/release/libsolana_accountsdb_plugin_kafka.so",
    "kafka": {
      "bootstrap.servers": "localhost:9092"
    },
    "filters": [
      {
        "update_account_topic": "solana.accounts",
        "transaction_topic": "solana.transactions",
        "block_metadata_topic": "solana.blocks",
        "program_log_topic": "solana.program_logs",
        "chain_event_topic": "solana.events",
        "extract_program_logs": true,
        "extract_anchor_events": true
      }
    ]
  }
  ```

- [ ] 2.6 测试配置加载
  ```bash
  cargo test test_config_loading
  ```

### 验收标准

- [x] 配置结构正确扩展
- [x] 向后兼容（旧配置仍能工作）
- [x] 测试配置成功加载

---

## 阶段 3: 实现 notify_block_metadata (1 天)

**目标**: 实现区块元数据通知功能

### 任务列表

- [ ] 3.1 在 `src/plugin.rs` 中实现 `notify_block_metadata()`
  ```rust
  fn notify_block_metadata(
      &self,
      blockinfo: ReplicaBlockInfoVersions,
  ) -> PluginResult<()> {
      // 实现
  }
  ```

- [ ] 3.2 添加 `unwrap_block_metadata()` 辅助方法
  ```rust
  fn unwrap_block_metadata(
      blockinfo: ReplicaBlockInfoVersions,
  ) -> &ReplicaBlockInfoV3 {
      match blockinfo {
          ReplicaBlockInfoVersions::V0_0_1(_) => panic!("..."),
          ReplicaBlockInfoVersions::V0_0_2(_) => panic!("..."),
          ReplicaBlockInfoVersions::V0_0_3(info) => info,
      }
  }
  ```

- [ ] 3.3 添加 `build_reward()` 方法转换奖励数据

- [ ] 3.4 在 `src/event.rs` 中确保导出 `BlockMetadataEvent`

- [ ] 3.5 在 `src/publisher.rs` 中实现 `publish_block_metadata()`
  ```rust
  pub fn publish_block_metadata(
      &self,
      ev: BlockMetadataEvent,
      wrap_messages: bool,
      topic: &str,
  ) -> Result<(), KafkaError> {
      // 实现
  }
  ```

- [ ] 3.6 在 `src/prom.rs` 中添加指标
  ```rust
  pub static ref UPLOAD_BLOCKS_TOTAL: IntCounterVec = 
      register_int_counter_vec!(
          "geyser_block_uploads_total",
          "Total number of block metadata published",
          &["status"]
      ).unwrap();
  ```

- [ ] 3.7 更新 Publisher 消息包装逻辑
  - [ ] 添加 `Block(Box<BlockMetadataEvent>)` 变体
  - [ ] 使用 `b'B'` 作为键前缀

- [ ] 3.8 编译测试
  ```bash
  cargo build --release
  ```

- [ ] 3.9 单元测试
  ```bash
  cargo test notify_block_metadata
  ```

### 验收标准

- [x] `notify_block_metadata()` 正确实现
- [x] Kafka 消息正确发送
- [x] Prometheus 指标正常记录
- [x] 单元测试通过

---

## 阶段 4: 实现日志解析器 (2 天)

**目标**: 创建高效的日志解析模块

### 任务列表

- [ ] 4.1 创建 `src/log_parser.rs` 文件

- [ ] 4.2 定义 `LogParser` 结构
  ```rust
  pub struct LogParser {
      program_stack: Vec<String>,
      depth: u32,
  }
  ```

- [ ] 4.3 实现 `parse_program_logs()` 方法
  - [ ] 遍历日志消息
  - [ ] 识别日志类型
  - [ ] 维护程序栈
  - [ ] 生成 `ProgramLogEvent`

- [ ] 4.4 实现 `parse_chain_events()` 方法
  - [ ] 查找 "Program data:" 日志
  - [ ] 提取 base64 数据
  - [ ] 解码为字节
  - [ ] 提取判别器（前 8 字节）
  - [ ] 生成 `ChainEvent`

- [ ] 4.5 实现 `identify_log_type()` 方法
  ```rust
  fn identify_log_type(&self, message: &str) -> LogType {
      if message.contains(" invoke [") {
          LogType::Invoke
      } else if message.starts_with("Program log: ") {
          LogType::Log
      } else if message.starts_with("Program data: ") {
          LogType::Data
      }
      // ...
  }
  ```

- [ ] 4.6 实现 `extract_program_id()` 方法
  ```rust
  fn extract_program_id(&self, message: &str) -> Option<String> {
      // "Program 11111111111111111111111111111111 invoke [1]"
      //          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
      let parts: Vec<&str> = message.split_whitespace().collect();
      if parts.len() >= 3 && parts[0] == "Program" {
          Some(parts[1].to_string())
      } else {
          None
      }
  }
  ```

- [ ] 4.7 实现 `extract_program_data()` 方法
  ```rust
  fn extract_program_data(message: &str) -> Option<Vec<u8>> {
      // "Program data: SGVsbG8gV29ybGQ="
      //               ^^^^^^^^^^^^^^^^^
      if let Some(data_str) = message.strip_prefix("Program data: ") {
          base64::decode(data_str.trim()).ok()
      } else {
          None
      }
  }
  ```

- [ ] 4.8 实现 `parse_anchor_event()` 方法
  ```rust
  fn parse_anchor_event(data: &[u8]) -> Option<([u8; 8], Vec<u8>)> {
      if data.len() >= 8 {
          let mut discriminator = [0u8; 8];
          discriminator.copy_from_slice(&data[0..8]);
          let event_data = data[8..].to_vec();
          Some((discriminator, event_data))
      } else {
          None
      }
  }
  ```

- [ ] 4.9 在 `src/lib.rs` 中添加模块声明
  ```rust
  mod log_parser;
  pub use log_parser::LogParser;
  ```

- [ ] 4.10 编写单元测试
  - [ ] `test_identify_invoke_log`
  - [ ] `test_identify_log_message`
  - [ ] `test_identify_program_data`
  - [ ] `test_extract_program_id`
  - [ ] `test_extract_program_data`
  - [ ] `test_parse_anchor_event`
  - [ ] `test_program_stack_management`
  - [ ] `test_parse_program_logs_integration`
  - [ ] `test_parse_chain_events_integration`

- [ ] 4.11 运行测试
  ```bash
  cargo test log_parser
  ```

- [ ] 4.12 性能测试
  - [ ] 测试解析 100 条日志的时间
  - [ ] 确保 < 1 ms

### 验收标准

- [x] 所有日志类型正确识别
- [x] 程序栈正确维护
- [x] Anchor 事件正确解析
- [x] 单元测试覆盖率 > 80%
- [x] 性能满足要求

---

## 阶段 5: 扩展 notify_transaction (1 天)

**目标**: 集成日志解析器到交易处理流程

### 任务列表

- [ ] 5.1 修改 `src/plugin.rs::notify_transaction()`
  ```rust
  fn notify_transaction(...) -> PluginResult<()> {
      // 现有交易发布逻辑
      
      // 新增：提取程序日志
      if filter.extract_program_logs && !filter.program_log_topic.is_empty() {
          // 实现
      }
      
      // 新增：解析链上事件
      if filter.extract_anchor_events && !filter.chain_event_topic.is_empty() {
          // 实现
      }
  }
  ```

- [ ] 5.2 添加错误处理
  ```rust
  if let Err(e) = publisher.publish_program_logs(...) {
      error!("Failed to publish program logs: {}", e);
      UPLOAD_ERRORS.inc();
      // 不中断主流程
  }
  ```

- [ ] 5.3 在 `src/publisher.rs` 中实现批量发布
  - [ ] `publish_program_logs(events: Vec<ProgramLogEvent>)`
  - [ ] `publish_chain_events(events: Vec<ChainEvent>)`

- [ ] 5.4 优化：避免不必要的日志解析
  ```rust
  // 只在配置开启时才解析
  if filter.extract_program_logs || filter.extract_anchor_events {
      let log_messages = &info.transaction_status_meta
          .log_messages
          .clone()
          .unwrap_or_default();
      
      let mut parser = LogParser::new();
      
      if filter.extract_program_logs {
          let program_logs = parser.parse_program_logs(...);
          // 发布
      }
      
      if filter.extract_anchor_events {
          let chain_events = parser.parse_chain_events(...);
          // 发布
      }
  }
  ```

- [ ] 5.5 添加 Prometheus 指标
  ```rust
  UPLOAD_PROGRAM_LOGS_TOTAL.with_label_values(&["success"]).inc();
  UPLOAD_EVENTS_TOTAL.with_label_values(&["success"]).inc();
  ```

- [ ] 5.6 编译测试
  ```bash
  cargo build --release
  ```

- [ ] 5.7 单元测试
  ```bash
  cargo test notify_transaction
  ```

### 验收标准

- [x] 日志解析正确集成
- [x] 错误不影响主流程
- [x] 批量发送正常工作
- [x] 测试通过

---

## 阶段 6: Publisher 和 Prometheus 集成 (0.5 天)

**目标**: 完善发布和监控功能

### 任务列表

- [ ] 6.1 在 `src/publisher.rs` 中实现新方法
  ```rust
  pub fn publish_block_metadata(...) -> Result<(), KafkaError> { }
  pub fn publish_program_logs(...) -> Result<(), KafkaError> { }
  pub fn publish_chain_events(...) -> Result<(), KafkaError> { }
  ```

- [ ] 6.2 实现消息包装逻辑
  ```rust
  let buf = if wrap_messages {
      Self::encode_with_wrapper(Block(Box::new(ev)))
  } else {
      ev.encode_to_vec()
  };
  ```

- [ ] 6.3 在 `src/prom.rs` 中添加所有新指标
  ```rust
  UPLOAD_BLOCKS_TOTAL
  UPLOAD_PROGRAM_LOGS_TOTAL
  UPLOAD_EVENTS_TOTAL
  ```

- [ ] 6.4 更新 `StatsThreadedProducerContext` 如果需要

- [ ] 6.5 在 `src/event.rs` 中确保类型导出
  ```rust
  pub use {
      BlockMetadataEvent,
      ProgramLogEvent,
      ChainEvent,
      LogType,
  };
  ```

- [ ] 6.6 测试 Prometheus 端点
  ```bash
  curl http://localhost:9091/metrics | grep geyser
  ```

### 验收标准

- [x] 所有 Publisher 方法正常工作
- [x] Prometheus 指标正确记录
- [x] 消息包装选项正常

---

## 阶段 7: 集成测试和验证 (1 天)

**目标**: 端到端测试所有功能

### 任务列表

- [ ] 7.1 启动 Kafka 集群
  ```bash
  cd redpanda-quickstart
  docker-compose up -d
  ```

- [ ] 7.2 创建所有 Kafka topics
  ```bash
  rpk topic create solana.accounts
  rpk topic create solana.transactions
  rpk topic create solana.blocks
  rpk topic create solana.program_logs
  rpk topic create solana.events
  ```

- [ ] 7.3 编译 release 版本
  ```bash
  cargo build --release
  ```

- [ ] 7.4 使用完整配置启动测试节点
  ```bash
  solana-test-validator \
    --geyser-plugin-config config.test.json \
    --log
  ```

- [ ] 7.5 启动 Kafka 消费者监控
  ```bash
  # Terminal 1
  rpk topic consume solana.blocks
  
  # Terminal 2
  rpk topic consume solana.program_logs
  
  # Terminal 3
  rpk topic consume solana.events
  ```

- [ ] 7.6 执行测试交易
  ```bash
  # 简单转账
  solana transfer <ADDRESS> 0.1 --url http://localhost:8899
  
  # Token 操作（会产生更多日志）
  spl-token create-token
  spl-token create-account <TOKEN>
  spl-token mint <TOKEN> 100 <ACCOUNT>
  ```

- [ ] 7.7 验证数据接收
  - [ ] `solana.blocks` 收到区块数据
  - [ ] `solana.program_logs` 收到日志数据
  - [ ] `solana.events` 收到事件数据（如果有 Anchor 程序）

- [ ] 7.8 验证数据字段
  - [ ] 区块：slot, block_hash, leader, tx_count
  - [ ] 日志：program_id, message, depth, log_type
  - [ ] 事件：program_id, discriminator, raw_data

- [ ] 7.9 性能测试
  ```bash
  # 使用 bench-tps 压力测试
  solana-bench-tps \
    --entrypoint 127.0.0.1:8001 \
    --faucet 127.0.0.1:9900 \
    --duration 60 \
    --tx_count 1000
  ```
  - [ ] 确认无消息丢失
  - [ ] 确认延迟可接受（< 100ms p99）
  - [ ] 确认 CPU 使用合理（< 50%）

- [ ] 7.10 检查 Prometheus 指标
  ```bash
  curl http://localhost:9091/metrics
  ```
  - [ ] `geyser_block_uploads_total`
  - [ ] `geyser_program_logs_uploads_total`
  - [ ] `geyser_events_uploads_total`
  - [ ] 所有计数器递增

- [ ] 7.11 错误场景测试
  - [ ] Kafka 断开连接
  - [ ] 无效配置
  - [ ] 解析失败场景

### 验收标准

- [x] 所有 topics 正常接收数据
- [x] 数据格式正确完整
- [x] 性能满足要求
- [x] 监控指标正常
- [x] 错误处理正确

---

## 阶段 8: 文档和示例 (0.5 天)

**目标**: 完善文档和示例

### 任务列表

- [ ] 8.1 更新 `README.md`
  - [ ] 添加新功能说明
  - [ ] 更新配置示例
  - [ ] 添加使用场景

- [ ] 8.2 创建 `CONFIGURATION.md`
  - [ ] 完整配置参考
  - [ ] 所有字段说明
  - [ ] 最佳实践

- [ ] 8.3 创建 `EXAMPLES.md`
  - [ ] 基础配置示例
  - [ ] 高级过滤示例
  - [ ] 消费者代码示例（Python/Rust/Node.js）

- [ ] 8.4 更新 `ARCHITECTURE_ANALYSIS.md`
  - [ ] 补充新组件说明
  - [ ] 更新架构图

- [ ] 8.5 创建消费者示例代码
  - [ ] Python: `examples/consumer.py`
  - [ ] Rust: `examples/consumer.rs`
  - [ ] Node.js: `examples/consumer.js`

- [ ] 8.6 添加 API 文档
  ```bash
  cargo doc --no-deps --open
  ```

- [ ] 8.7 创建快速开始指南 `QUICKSTART.md`

- [ ] 8.8 添加故障排查指南 `TROUBLESHOOTING.md`

### 验收标准

- [x] README 完整准确
- [x] 配置文档详细
- [x] 示例代码可运行
- [x] API 文档完整

---

## 🎉 最终验收

### 功能完整性

- [ ] ✅ 支持 `chain_blocks` 数据采集
- [ ] ✅ 支持 `chain_txs` 数据采集
- [ ] ✅ 支持 `chain_accounts` 数据采集
- [ ] ✅ 支持 `chain_program_logs` 数据采集
- [ ] ✅ 支持 `chain_events` 数据采集

### 质量标准

- [ ] ✅ 所有单元测试通过
- [ ] ✅ 集成测试通过
- [ ] ✅ 性能测试通过（1000 TPS）
- [ ] ✅ 代码覆盖率 > 70%
- [ ] ✅ 无 clippy 警告
- [ ] ✅ 文档完整

### 部署就绪

- [ ] ✅ Release 版本编译成功
- [ ] ✅ 配置模板齐全
- [ ] ✅ 部署文档完整
- [ ] ✅ 监控指标完善

---

## 📊 进度跟踪

| 阶段 | 任务数 | 已完成 | 进度 | 状态 |
|------|--------|--------|------|------|
| 阶段 1 | 7 | 0 | 0% | 🔴 未开始 |
| 阶段 2 | 6 | 0 | 0% | 🔴 未开始 |
| 阶段 3 | 9 | 0 | 0% | 🔴 未开始 |
| 阶段 4 | 12 | 0 | 0% | 🔴 未开始 |
| 阶段 5 | 7 | 0 | 0% | 🔴 未开始 |
| 阶段 6 | 6 | 0 | 0% | 🔴 未开始 |
| 阶段 7 | 11 | 0 | 0% | 🔴 未开始 |
| 阶段 8 | 8 | 0 | 0% | 🔴 未开始 |
| **总计** | **66** | **0** | **0%** | 🔴 未开始 |

---

## 🐛 问题跟踪

### 已知问题

_暂无_

### 待解决问题

_暂无_

---

## 📝 变更日志

### [未发布]

- 创建初始检查清单

---

**最后更新**: 2025-01-XX  
**维护者**: GitHub Copilot
