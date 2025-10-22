# Solana Geyser-Kafka 插件改造计划

## 📋 文档概述

本文档详细说明如何改造现有的 Solana Geyser-Kafka 插件，使其能够完整支持以下 5 张数据表的数据采集：

| 表名 | 描述 | 核心字段 | 数据来源 | 状态 |
|------|------|---------|---------|------|
| **chain_blocks** | 区块基础信息 | slot, block_hash, parent_slot, leader, num_txs, timestamp | `notify_block_metadata()` | ⚠️ 需实现 |
| **chain_txs** | 交易明细 | tx_signature, slot, status, fee, compute_units, timestamp | `notify_transaction()` | ✅ 已支持 |
| **chain_accounts** | 账户快照 | pubkey, owner_program, lamports, slot, write_version | `update_account()` | ✅ 已支持 |
| **chain_program_logs** | 合约日志事件 | tx_signature, program_id, message, slot, timestamp | `log_messages` | ⚠️ 需提取 |
| **chain_events** | Anchor/自定义事件 | program, event_type, payload(JSON), slot, timestamp | `log_messages` | ⚠️ 需解析 |

---

## 🏗️ 现有架构分析

### 1. 核心组件架构

```
┌─────────────────────────────────────────────────────────────┐
│                   Solana Validator                          │
│                  (Agave Geyser Interface)                   │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                  KafkaPlugin (plugin.rs)                    │
│  ┌──────────────────────────────────────────────────────┐   │
│  │ GeyserPlugin 接口实现                                │   │
│  │  - on_load()                                         │   │
│  │  - update_account()          ✅ 已实现               │   │
│  │  - update_slot_status()      ✅ 已实现               │   │
│  │  - notify_transaction()      ✅ 已实现               │   │
│  │  - notify_block_metadata()   ❌ 未实现               │   │
│  └──────────────────────────────────────────────────────┘   │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                  Filter (filter.rs)                         │
│  - 程序过滤 (program_filters/program_ignores)               │
│  - 账户过滤 (account_filters)                               │
│  - 交易类型过滤 (include_vote_transactions, etc)            │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                Publisher (publisher.rs)                     │
│  - update_account()      → Kafka Topic                      │
│  - update_slot_status()  → Kafka Topic                      │
│  - update_transaction()  → Kafka Topic                      │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                   Kafka Cluster                             │
│  - account_updates topic                                    │
│  - slot_status topic                                        │
│  - transactions topic                                       │
└─────────────────────────────────────────────────────────────┘
```

### 2. 数据流向图

```
Validator Events
    │
    ├─► update_account() ────────► chain_accounts ✅
    │
    ├─► update_slot_status() ────► (用于同步，不直接对应表)
    │
    ├─► notify_transaction() ────► chain_txs ✅
    │                          └──► log_messages (需要提取)
    │                              ├─► chain_program_logs ⚠️
    │                              └─► chain_events ⚠️
    │
    └─► notify_block_metadata() ─► chain_blocks ❌ (未实现)
```

---

## 🔍 现有实现详细分析

### 1. 已实现功能

#### ✅ update_account() - 支持 chain_accounts 表

**文件位置**: `src/plugin.rs:96-135`

**功能**: 
- 捕获账户状态变更
- 包含完整的账户数据（pubkey, lamports, owner, data, write_version 等）
- 支持启动时批量发布（publish_all_accounts）

**数据结构** (`proto/event.proto:7-42`):
```protobuf
message UpdateAccountEvent {
  uint64 slot = 1;
  bytes pubkey = 2;
  uint64 lamports = 3;
  bytes owner = 4;
  bool executable = 5;
  uint64 rent_epoch = 6;
  bytes data = 7;
  uint64 write_version = 8;
  optional bytes txn_signature = 9;
  uint32 data_version = 10;
  bool is_startup = 11;
  uint64 account_age = 12;
}
```

**Kafka 发布流程**:
```rust
publisher.update_account(event, filter.wrap_messages, &filter.update_account_topic)
```

#### ✅ notify_transaction() - 支持 chain_txs 表

**文件位置**: `src/plugin.rs:166-206`

**功能**:
- 捕获所有交易信息
- 包含交易签名、状态、费用、计算单元消耗
- 包含完整的 log_messages（但未单独提取）
- 支持过滤投票交易和失败交易

**数据结构** (`proto/event.proto:203-227`):
```protobuf
message TransactionEvent {
  bytes signature = 1;
  bool is_vote = 2;
  SanitizedTransaction transaction = 3;
  TransactionStatusMeta transaction_status_meta = 4;
  uint64 slot = 5;
  uint64 index = 6;
  uint32 compute_units_consumed = 7;
  uint64 compute_units_price = 8;
  uint64 total_cost = 9;
  uint32 instruction_count = 10;
  uint32 account_count = 11;
  uint64 execution_time_ns = 12;
  bool is_successful = 13;
  repeated string execution_logs = 14;  // 包含 log_messages！
  repeated string error_details = 15;
  uint32 confirmation_count = 16;
}

message TransactionStatusMeta {
  // ...
  repeated string log_messages = 7;  // 这里包含所有程序日志
  // ...
}
```

**重要发现**: 
- ✅ `log_messages` 已经在 `TransactionStatusMeta.log_messages` 中
- ✅ 同时也复制到了 `TransactionEvent.execution_logs` 中
- ⚠️ 但没有单独提取为 `chain_program_logs` 和 `chain_events`

#### ✅ update_slot_status() - 用于同步状态

**文件位置**: `src/plugin.rs:137-164`

**功能**:
- 跟踪 slot 状态变化（Processed, Confirmed, Rooted 等）
- 用于判断数据确认状态
- 不直接对应数据表，但对同步至关重要

### 2. 缺失功能

#### ❌ notify_block_metadata() - 需要实现 chain_blocks 表

**目标数据表字段**:
```sql
CREATE TABLE chain_blocks (
    slot BIGINT PRIMARY KEY,
    block_hash VARCHAR(88),
    parent_slot BIGINT,
    leader VARCHAR(44),
    num_txs INTEGER,
    timestamp TIMESTAMP,
    block_time BIGINT,
    block_height BIGINT
);
```

**需要实现的接口**:
```rust
fn notify_block_metadata(&self, blockinfo: ReplicaBlockInfoVersions) -> PluginResult<()>
```

**可用数据** (来自 Geyser 接口):
- `block_info.slot` - slot 编号
- `block_info.blockhash` - 区块哈希
- `block_info.parent_slot` - 父区块 slot
- `block_info.parent_blockhash` - 父区块哈希
- `block_info.rewards` - 区块奖励信息
- `block_info.block_time` - 区块时间戳
- `block_info.block_height` - 区块高度
- `block_info.executed_transaction_count` - 交易数量

**当前状态**: ❌ 该方法完全未实现

#### ⚠️ chain_program_logs - 需要从 log_messages 提取

**目标数据表字段**:
```sql
CREATE TABLE chain_program_logs (
    id BIGSERIAL PRIMARY KEY,
    tx_signature VARCHAR(88),
    slot BIGINT,
    program_id VARCHAR(44),
    message TEXT,
    log_index INTEGER,
    timestamp TIMESTAMP
);
```

**数据来源**: `TransactionStatusMeta.log_messages`

**日志格式示例**:
```
"Program 11111111111111111111111111111111 invoke [1]"
"Program log: Instruction: Transfer"
"Program 11111111111111111111111111111111 success"
"Program TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA invoke [1]"
"Program log: Instruction: MintTo"
"Program data: <base64_encoded_data>"
"Program TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA consumed 4645 of 200000 compute units"
"Program TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA success"
```

**需要提取的信息**:
- Program ID (从 "Program XXX invoke" 解析)
- Log message (原始日志内容)
- Log index (日志在数组中的索引)
- 与交易和 slot 的关联

**当前状态**: ⚠️ 数据在 `execution_logs` 中，但未单独提取和发布

#### ⚠️ chain_events - 需要解析 Anchor 事件

**目标数据表字段**:
```sql
CREATE TABLE chain_events (
    id BIGSERIAL PRIMARY KEY,
    slot BIGINT,
    tx_signature VARCHAR(88),
    program_id VARCHAR(44),
    event_type VARCHAR(100),
    payload JSONB,
    timestamp TIMESTAMP
);
```

**数据来源**: `log_messages` 中的特定模式

**Anchor 事件日志格式**:
```
"Program data: <base64_encoded_data>"
```

**解析流程**:
1. 查找包含 "Program data:" 的日志行
2. 提取 base64 数据
3. 解码为字节数组
4. 前 8 字节是事件判别器 (discriminator)
5. 剩余部分是 Borsh 序列化的事件数据

**当前状态**: ⚠️ 数据在 `execution_logs` 中，但未解析和发布

---

## 🎯 改造目标

### 整体目标

使 Geyser-Kafka 插件能够向 Kafka 发送 5 种类型的事件消息：

1. ✅ `UpdateAccountEvent` → `chain_accounts` (已支持)
2. ✅ `TransactionEvent` → `chain_txs` (已支持)
3. ⚠️ `BlockMetadataEvent` → `chain_blocks` (需新增)
4. ⚠️ `ProgramLogEvent` → `chain_program_logs` (需新增)
5. ⚠️ `ChainEvent` → `chain_events` (需新增)

### 设计原则

1. **最小侵入**: 保持现有代码结构，只添加新功能
2. **向后兼容**: 不破坏现有的 topic 和数据格式
3. **可配置性**: 通过配置文件控制新功能的开启
4. **性能优先**: 日志解析在内存中高效完成
5. **错误处理**: 解析失败不影响主流程

---

## 📐 软件设计框架

### 1. 新增 Protobuf 消息定义

**文件**: `proto/event.proto`

```protobuf
// 1. 区块元数据事件
message BlockMetadataEvent {
  uint64 slot = 1;
  string block_hash = 2;
  uint64 parent_slot = 3;
  string parent_block_hash = 4;
  string leader = 5;                      // 区块领导者 pubkey
  uint32 executed_transaction_count = 6;  // 交易数量
  optional int64 block_time = 7;          // Unix 时间戳
  optional uint64 block_height = 8;       // 区块高度
  repeated Reward rewards = 9;            // 区块奖励
  uint32 total_compute_units = 10;        // 总计算单元
  uint64 total_fees = 11;                 // 总费用
}

// 2. 程序日志事件
message ProgramLogEvent {
  uint64 slot = 1;
  bytes tx_signature = 2;                 // 关联的交易签名
  uint32 tx_index = 3;                    // 交易在区块中的索引
  string program_id = 4;                  // 程序 ID
  string message = 5;                     // 日志消息
  uint32 log_index = 6;                   // 日志在交易中的索引
  uint32 depth = 7;                       // 调用深度 [1,2,3,...]
  LogType log_type = 8;                   // 日志类型
}

enum LogType {
  UNKNOWN = 0;
  INVOKE = 1;      // "Program XXX invoke [N]"
  LOG = 2;         // "Program log: XXX"
  DATA = 3;        // "Program data: XXX"
  SUCCESS = 4;     // "Program XXX success"
  FAILED = 5;      // "Program XXX failed: XXX"
  CONSUMED = 6;    // "Program XXX consumed N of M compute units"
}

// 3. 链上事件（Anchor 和自定义事件）
message ChainEvent {
  uint64 slot = 1;
  bytes tx_signature = 2;                 // 关联的交易签名
  uint32 tx_index = 3;                    // 交易在区块中的索引
  string program_id = 4;                  // 发出事件的程序
  bytes event_discriminator = 5;          // 事件判别器（8 字节）
  string event_type = 6;                  // 事件类型名称（可选）
  bytes raw_data = 7;                     // 原始事件数据（Borsh 编码）
  optional string json_payload = 8;       // JSON 格式的事件数据（如果能解析）
  uint32 log_index = 9;                   // 对应的日志索引
}

// 4. 更新 MessageWrapper 包含新事件
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

### 2. 配置文件扩展

**文件**: `src/config.rs`

```rust
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigFilter {
    // 现有配置
    pub update_account_topic: String,
    pub slot_status_topic: String,
    pub transaction_topic: String,
    
    // 新增配置
    pub block_metadata_topic: String,        // 区块元数据 topic
    pub program_log_topic: String,           // 程序日志 topic
    pub chain_event_topic: String,           // 链上事件 topic
    
    // 现有过滤器
    pub program_ignores: Vec<String>,
    pub program_filters: Vec<String>,
    pub account_filters: Vec<String>,
    
    // 新增过滤器
    pub extract_program_logs: bool,          // 是否提取程序日志
    pub extract_anchor_events: bool,         // 是否解析 Anchor 事件
    pub event_discriminators: Vec<String>,   // 要捕获的事件判别器列表
    
    // 现有标志
    pub publish_all_accounts: bool,
    pub include_vote_transactions: bool,
    pub include_failed_transactions: bool,
    pub wrap_messages: bool,
}

impl Default for ConfigFilter {
    fn default() -> Self {
        Self {
            // 现有字段...
            
            // 新增字段默认值
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

### 3. 日志解析器模块

**新文件**: `src/log_parser.rs`

```rust
/// 日志解析器 - 从交易日志中提取程序日志和事件
pub struct LogParser {
    /// 当前程序调用栈
    program_stack: Vec<String>,
    /// 当前调用深度
    depth: u32,
}

impl LogParser {
    pub fn new() -> Self {
        Self {
            program_stack: Vec::new(),
            depth: 0,
        }
    }
    
    /// 解析所有日志消息，生成程序日志事件
    pub fn parse_program_logs(
        &mut self,
        log_messages: &[String],
        signature: &[u8],
        slot: u64,
        tx_index: u32,
    ) -> Vec<ProgramLogEvent> {
        // 实现日志解析逻辑
    }
    
    /// 解析链上事件（从 "Program data:" 日志）
    pub fn parse_chain_events(
        &mut self,
        log_messages: &[String],
        signature: &[u8],
        slot: u64,
        tx_index: u32,
    ) -> Vec<ChainEvent> {
        // 实现事件解析逻辑
    }
    
    /// 识别日志类型
    fn identify_log_type(message: &str) -> LogType {
        // INVOKE: "Program XXX invoke [N]"
        // LOG: "Program log: XXX"
        // DATA: "Program data: XXX"
        // SUCCESS: "Program XXX success"
        // FAILED: "Program XXX failed"
        // CONSUMED: "Program XXX consumed N of M compute units"
    }
    
    /// 从 "Program XXX invoke [N]" 提取程序 ID
    fn extract_program_id(message: &str) -> Option<String> {
        // 使用正则或字符串解析
    }
    
    /// 从 "Program data: XXX" 提取 base64 数据
    fn extract_program_data(message: &str) -> Option<Vec<u8>> {
        // base64 解码
    }
    
    /// 解析 Anchor 事件格式
    fn parse_anchor_event(data: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
        // 返回 (discriminator, borsh_data)
    }
}
```

### 4. Publisher 扩展

**文件**: `src/publisher.rs`

```rust
impl Publisher {
    // 现有方法...
    
    /// 发布区块元数据事件
    pub fn publish_block_metadata(
        &self,
        ev: BlockMetadataEvent,
        wrap_messages: bool,
        topic: &str,
    ) -> Result<(), KafkaError> {
        let temp_key;
        let (key, buf) = if wrap_messages {
            temp_key = self.copy_and_prepend(&ev.slot.to_le_bytes(), b'B');
            (&temp_key, Self::encode_with_wrapper(Block(Box::new(ev))))
        } else {
            temp_key = ev.slot.to_le_bytes().to_vec();
            (&temp_key, ev.encode_to_vec())
        };
        let record = BaseRecord::<Vec<u8>, _>::to(topic).key(key).payload(&buf);
        self.producer.send(record).map(|_| ()).map_err(|(e, _)| e)
    }
    
    /// 发布程序日志事件（批量）
    pub fn publish_program_logs(
        &self,
        events: Vec<ProgramLogEvent>,
        wrap_messages: bool,
        topic: &str,
    ) -> Result<(), KafkaError> {
        for ev in events {
            let key = ev.tx_signature.clone();
            let buf = if wrap_messages {
                Self::encode_with_wrapper(ProgramLog(Box::new(ev)))
            } else {
                ev.encode_to_vec()
            };
            let record = BaseRecord::<Vec<u8>, _>::to(topic).key(&key).payload(&buf);
            self.producer.send(record).map_err(|(e, _)| e)?;
        }
        Ok(())
    }
    
    /// 发布链上事件（批量）
    pub fn publish_chain_events(
        &self,
        events: Vec<ChainEvent>,
        wrap_messages: bool,
        topic: &str,
    ) -> Result<(), KafkaError> {
        for ev in events {
            let key = ev.tx_signature.clone();
            let buf = if wrap_messages {
                Self::encode_with_wrapper(ChainEventMsg(Box::new(ev)))
            } else {
                ev.encode_to_vec()
            };
            let record = BaseRecord::<Vec<u8>, _>::to(topic).key(&key).payload(&buf);
            self.producer.send(record).map_err(|(e, _)| e)?;
        }
        Ok(())
    }
}
```

### 5. Plugin 核心逻辑扩展

**文件**: `src/plugin.rs`

```rust
impl GeyserPlugin for KafkaPlugin {
    // 现有方法...
    
    /// 新增：实现区块元数据通知
    fn notify_block_metadata(
        &self,
        blockinfo: ReplicaBlockInfoVersions,
    ) -> PluginResult<()> {
        let info = Self::unwrap_block_metadata(blockinfo);
        let publisher = self.unwrap_publisher();
        
        for filter in self.unwrap_filters() {
            if !filter.block_metadata_topic.is_empty() {
                let event = BlockMetadataEvent {
                    slot: info.slot,
                    block_hash: bs58::encode(&info.blockhash).into_string(),
                    parent_slot: info.parent_slot,
                    parent_block_hash: bs58::encode(&info.parent_blockhash).into_string(),
                    leader: "".to_string(), // 需要从其他地方获取
                    executed_transaction_count: info.executed_transaction_count as u32,
                    block_time: info.block_time,
                    block_height: info.block_height,
                    rewards: info.rewards.iter().map(Self::build_reward).collect(),
                    total_compute_units: 0, // 需要累加
                    total_fees: 0, // 需要累加
                };
                
                publisher.publish_block_metadata(
                    event,
                    filter.wrap_messages,
                    &filter.block_metadata_topic,
                )?;
            }
        }
        
        Ok(())
    }
    
    /// 修改：扩展 notify_transaction 以提取日志和事件
    fn notify_transaction(
        &self,
        transaction: ReplicaTransactionInfoVersions,
        slot: u64,
    ) -> PluginResult<()> {
        let info = Self::unwrap_transaction(transaction);
        let publisher = self.unwrap_publisher();
        
        for filter in self.unwrap_filters() {
            // 现有的交易发布逻辑...
            if !filter.transaction_topic.is_empty() {
                // 现有代码
            }
            
            // 新增：提取程序日志
            if filter.extract_program_logs && !filter.program_log_topic.is_empty() {
                let log_messages = &info.transaction_status_meta
                    .log_messages
                    .clone()
                    .unwrap_or_default();
                
                let mut parser = LogParser::new();
                let program_logs = parser.parse_program_logs(
                    log_messages,
                    info.signature.as_ref(),
                    slot,
                    info.index as u32,
                );
                
                if !program_logs.is_empty() {
                    publisher.publish_program_logs(
                        program_logs,
                        filter.wrap_messages,
                        &filter.program_log_topic,
                    )?;
                }
            }
            
            // 新增：解析链上事件
            if filter.extract_anchor_events && !filter.chain_event_topic.is_empty() {
                let log_messages = &info.transaction_status_meta
                    .log_messages
                    .clone()
                    .unwrap_or_default();
                
                let mut parser = LogParser::new();
                let chain_events = parser.parse_chain_events(
                    log_messages,
                    info.signature.as_ref(),
                    slot,
                    info.index as u32,
                );
                
                if !chain_events.is_empty() {
                    publisher.publish_chain_events(
                        chain_events,
                        filter.wrap_messages,
                        &filter.chain_event_topic,
                    )?;
                }
            }
        }
        
        Ok(())
    }
}
```

---

## 📝 详细改造计划

### 阶段 1: Protobuf 定义和代码生成 (1 天)

**任务**:
1. 在 `proto/event.proto` 中添加新的消息定义
2. 运行 `cargo build` 触发 protobuf 代码生成
3. 验证生成的 Rust 代码

**文件**:
- `proto/event.proto` - 添加 BlockMetadataEvent, ProgramLogEvent, ChainEvent

**验证**:
```bash
cargo build
# 检查 target/debug/build/.../out/blockdaemon.solana.accountsdb_plugin_kafka.types.rs
```

### 阶段 2: 配置系统扩展 (0.5 天)

**任务**:
1. 扩展 `ConfigFilter` 结构
2. 扩展 `Filter` 结构
3. 添加默认值

**文件**:
- `src/config.rs` - 添加新配置字段
- `src/filter.rs` - 添加新过滤逻辑

**验证**:
- 创建测试配置文件并加载

### 阶段 3: 实现 notify_block_metadata (1 天)

**任务**:
1. 实现 `notify_block_metadata()` 方法
2. 构建 `BlockMetadataEvent` 消息
3. 添加 Publisher 发布方法

**文件**:
- `src/plugin.rs` - 实现 notify_block_metadata
- `src/publisher.rs` - 实现 publish_block_metadata
- `src/prom.rs` - 添加 Prometheus 指标

**测试**:
```bash
# 1. 编译插件
cargo build --release

# 2. 配置启用 block_metadata_topic
# 3. 启动测试节点
# 4. 验证 Kafka topic 收到区块数据
```

### 阶段 4: 实现日志解析器 (2 天)

**任务**:
1. 创建 `src/log_parser.rs`
2. 实现程序日志解析
3. 实现 Anchor 事件解析
4. 添加单元测试

**文件**:
- `src/log_parser.rs` - 新建文件
- `src/lib.rs` - 添加模块声明

**单元测试示例**:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_parse_invoke_log() {
        let log = "Program 11111111111111111111111111111111 invoke [1]";
        let result = LogParser::extract_program_id(log);
        assert_eq!(result, Some("11111111111111111111111111111111".to_string()));
    }
    
    #[test]
    fn test_parse_program_data() {
        let log = "Program data: SGVsbG8gV29ybGQ=";  // "Hello World" in base64
        let result = LogParser::extract_program_data(log);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), b"Hello World");
    }
}
```

### 阶段 5: 扩展 notify_transaction (1 天)

**任务**:
1. 修改 `notify_transaction` 方法
2. 集成 LogParser
3. 添加错误处理

**文件**:
- `src/plugin.rs` - 修改 notify_transaction
- `src/publisher.rs` - 添加批量发布方法

**注意事项**:
- 日志解析失败不应影响交易发布
- 使用 `if let` 和 `unwrap_or_default` 处理 Option

### 阶段 6: Publisher 和 Prometheus 集成 (0.5 天)

**任务**:
1. 添加新的 Publisher 方法
2. 添加 Prometheus 指标
3. 更新消息包装逻辑

**文件**:
- `src/publisher.rs` - 新增发布方法
- `src/prom.rs` - 新增指标
- `src/event.rs` - 确保类型导出

**Prometheus 指标**:
```rust
lazy_static! {
    pub static ref UPLOAD_BLOCKS_TOTAL: IntCounterVec = register_int_counter_vec!(
        "geyser_block_uploads_total",
        "Total number of block metadata published",
        &["status"]
    ).unwrap();
    
    pub static ref UPLOAD_PROGRAM_LOGS_TOTAL: IntCounterVec = register_int_counter_vec!(
        "geyser_program_logs_uploads_total",
        "Total number of program logs published",
        &["status"]
    ).unwrap();
    
    pub static ref UPLOAD_EVENTS_TOTAL: IntCounterVec = register_int_counter_vec!(
        "geyser_events_uploads_total",
        "Total number of chain events published",
        &["status"]
    ).unwrap();
}
```

### 阶段 7: 集成测试和验证 (1 天)

**任务**:
1. 创建完整的测试配置
2. 启动测试环境
3. 验证所有数据流
4. 性能测试

**测试配置** (`config.enhanced.json`):
```json
{
  "libpath": "target/release/libsolana_accountsdb_plugin_kafka.so",
  "kafka": {
    "bootstrap.servers": "localhost:9092"
  },
  "filters": [
    {
      "update_account_topic": "solana.accounts",
      "slot_status_topic": "solana.slots",
      "transaction_topic": "solana.transactions",
      "block_metadata_topic": "solana.blocks",
      "program_log_topic": "solana.program_logs",
      "chain_event_topic": "solana.events",
      "extract_program_logs": true,
      "extract_anchor_events": true,
      "include_vote_transactions": false,
      "include_failed_transactions": true
    }
  ]
}
```

**验证脚本**:
```bash
#!/bin/bash

# 1. 启动 Kafka
docker-compose up -d

# 2. 创建 topics
kafka-topics --create --topic solana.blocks --bootstrap-server localhost:9092
kafka-topics --create --topic solana.program_logs --bootstrap-server localhost:9092
kafka-topics --create --topic solana.events --bootstrap-server localhost:9092

# 3. 启动消费者查看数据
kafka-console-consumer --topic solana.blocks --bootstrap-server localhost:9092 &
kafka-console-consumer --topic solana.program_logs --bootstrap-server localhost:9092 &
kafka-console-consumer --topic solana.events --bootstrap-server localhost:9092 &

# 4. 启动测试节点
solana-test-validator --geyser-plugin-config config.enhanced.json

# 5. 执行测试交易
solana transfer <ADDRESS> 0.1 --url http://localhost:8899

# 6. 检查 Kafka 数据
```

### 阶段 8: 文档和示例 (0.5 天)

**任务**:
1. 更新 README.md
2. 创建配置示例
3. 添加消费者示例代码
4. 更新 API 文档

**文档文件**:
- `README.md` - 主文档
- `EXAMPLES.md` - 使用示例
- `CONFIG_REFERENCE.md` - 配置参考
- `ARCHITECTURE.md` - 架构说明

---

## 🔄 数据流程图（改造后）

```
┌─────────────────────────────────────────────────────────────┐
│                   Solana Validator                          │
└──────────────────┬──────────────────────────────────────────┘
                   │
    ┌──────────────┼──────────────┬──────────────┬─────────────┐
    │              │              │              │             │
    ▼              ▼              ▼              ▼             ▼
┌─────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐
│ Account │  │   Slot   │  │   Tx     │  │  Block   │  │  Block   │
│ Update  │  │  Status  │  │  Notify  │  │ Metadata │  │ Metadata │
└────┬────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘
     │            │              │              │              │
     ▼            ▼              ▼              ▼              │
┌─────────────────────────────────────────────────────────────┤
│                    KafkaPlugin                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │                   Filter Layer                       │   │
│  └──────────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────────┐   │
│  │                  LogParser (NEW)                     │   │
│  │  - parse_program_logs()                              │   │
│  │  - parse_chain_events()                              │   │
│  └──────────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────────┐   │
│  │                    Publisher                         │   │
│  └──────────────────────────────────────────────────────┘   │
└────┬────┬────┬────┬────┬────────────────────────────────────┘
     │    │    │    │    │
     ▼    ▼    ▼    ▼    ▼
┌─────────────────────────────────────────────────────────────┐
│                      Kafka Topics                           │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐    │
│  │ accounts │  │   txs    │  │  blocks  │  │   logs   │    │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘    │
│  ┌──────────┐                                               │
│  │  events  │                                               │
│  └──────────┘                                               │
└─────────────────────────────────────────────────────────────┘
     │            │            │            │            │
     ▼            ▼            ▼            ▼            ▼
┌─────────────────────────────────────────────────────────────┐
│                    PostgreSQL Tables                        │
│  ┌────────────┐  ┌──────────┐  ┌─────────────┐             │
│  │   chain_   │  │  chain_  │  │   chain_    │             │
│  │  accounts  │  │   txs    │  │   blocks    │             │
│  └────────────┘  └──────────┘  └─────────────┘             │
│  ┌────────────┐  ┌──────────┐                              │
│  │   chain_   │  │  chain_  │                              │
│  │program_logs│  │  events  │                              │
│  └────────────┘  └──────────┘                              │
└─────────────────────────────────────────────────────────────┘
```

---

## ⚠️ 注意事项和风险

### 1. 性能考虑

**日志解析性能**:
- 每笔交易可能有几十到上百条日志
- 正则表达式解析可能成为瓶颈
- **缓解策略**: 使用高效的字符串解析，避免正则表达式

**Kafka 吞吐量**:
- 新增 3 个 topic 会增加 Kafka 负载
- **缓解策略**: 
  - 批量发送消息
  - 配置合理的 Kafka producer 参数
  - 使用压缩（lz4）

### 2. 数据一致性

**交易和日志的关联**:
- 必须确保日志和交易正确关联
- **解决方案**: 使用交易签名作为关联键

**区块元数据时机**:
- notify_block_metadata 可能在交易之前或之后调用
- **解决方案**: 使用 slot 作为关联，消费者需要处理乱序

### 3. 向后兼容性

**现有配置**:
- 新字段都有默认值（空字符串或 false）
- 不影响现有部署

**现有 topic 格式**:
- 不修改现有消息格式
- 新消息使用新 topic

### 4. 错误处理

**日志解析失败**:
- 不应导致插件崩溃
- 不应阻止交易发布
- **策略**: 记录警告日志，继续处理

**Kafka 发送失败**:
- 当前实现会返回错误
- **改进**: 考虑添加重试机制或失败队列

---

## 📊 工作量估算

| 阶段 | 任务 | 预计时间 | 难度 |
|-----|------|---------|------|
| 1 | Protobuf 定义 | 1 天 | ⭐⭐ |
| 2 | 配置系统扩展 | 0.5 天 | ⭐ |
| 3 | 实现 notify_block_metadata | 1 天 | ⭐⭐⭐ |
| 4 | 实现日志解析器 | 2 天 | ⭐⭐⭐⭐ |
| 5 | 扩展 notify_transaction | 1 天 | ⭐⭐⭐ |
| 6 | Publisher 和 Prometheus | 0.5 天 | ⭐⭐ |
| 7 | 集成测试和验证 | 1 天 | ⭐⭐⭐ |
| 8 | 文档和示例 | 0.5 天 | ⭐ |
| **总计** | | **7.5 天** | |

---

## ✅ 验收标准

### 功能验收

1. **区块元数据**:
   - ✅ 每个区块生成一条 BlockMetadataEvent
   - ✅ 包含所有必需字段（slot, hash, leader, tx_count 等）
   - ✅ 发布到配置的 Kafka topic

2. **程序日志**:
   - ✅ 正确解析所有日志类型（INVOKE, LOG, DATA, SUCCESS, FAILED）
   - ✅ 保持调用层级（depth）
   - ✅ 正确关联到交易和 slot

3. **链上事件**:
   - ✅ 正确识别 "Program data:" 日志
   - ✅ 正确解码 base64 数据
   - ✅ 提取事件判别器（8 字节）
   - ✅ 保存原始数据供下游解析

### 性能验收

1. **延迟**:
   - ✅ 日志解析不应显著增加延迟（< 10ms per tx）
   - ✅ Kafka 发送延迟合理（< 100ms p99）

2. **吞吐量**:
   - ✅ 处理 1000 TPS 无压力
   - ✅ Kafka producer 无积压

### 质量验收

1. **测试覆盖**:
   - ✅ 日志解析器单元测试覆盖率 > 80%
   - ✅ 端到端集成测试通过

2. **错误处理**:
   - ✅ 解析错误不影响主流程
   - ✅ 所有错误都有适当日志

3. **文档**:
   - ✅ README 包含新功能说明
   - ✅ 配置参考文档完整
   - ✅ 至少一个完整示例

---

## 🚀 后续优化方向

### 短期优化（1-2 周）

1. **日志解析优化**:
   - 使用更高效的字符串解析算法
   - 缓存程序 ID 和常见模式

2. **批量处理**:
   - 累积多条日志/事件后批量发送
   - 减少 Kafka 调用次数

3. **Schema Registry 集成**:
   - 使用 Confluent Schema Registry
   - 支持 Avro 格式

### 中期优化（1-2 月）

1. **Anchor IDL 集成**:
   - 加载 Anchor IDL 文件
   - 自动解析事件为 JSON
   - 提供事件类型名称

2. **过滤增强**:
   - 基于事件判别器过滤
   - 基于程序 ID 过滤日志
   - 正则表达式匹配日志内容

3. **压缩和优化**:
   - 使用更紧凑的数据格式
   - 可选的字段去重
   - 增量账户数据

### 长期优化（3+ 月）

1. **实时 IDL 发现**:
   - 自动从链上拉取 Anchor IDL
   - 动态更新事件解析器

2. **多链支持**:
   - 支持不同的 Solana 分叉
   - 支持不同的程序框架（不只是 Anchor）

3. **高级分析**:
   - 内置 MEV 检测
   - 交易流程追踪
   - 账户关系图谱

---

## 📚 参考资源

### Geyser 接口文档
- [Geyser Plugin Interface](https://docs.solana.com/developing/plugins/geyser-plugins)
- [Agave Geyser源码](https://github.com/anza-xyz/agave/tree/master/geyser-plugin-interface)

### Anchor 框架
- [Anchor Event 文档](https://www.anchor-lang.com/docs/events)
- [Borsh 序列化](https://borsh.io/)

### 现有实现参考
- [Blockdaemon Kafka Plugin](https://github.com/Blockdaemon/solana-accountsdb-plugin-kafka)
- [Yellowstone gRPC Plugin](https://github.com/rpcpool/yellowstone-grpc)

---

## 📞 支持和联系

如有疑问或需要支持，请：
1. 查看项目 README 和文档
2. 搜索 GitHub Issues
3. 创建新的 Issue 并详细描述问题

---

**版本**: 1.0  
**最后更新**: 2025-01-XX  
**作者**: GitHub Copilot  
**状态**: 待实施
