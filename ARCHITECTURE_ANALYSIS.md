# Geyser-Kafka 插件架构分析

## 📐 系统架构概览

### 核心架构图

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Solana Validator (Agave)                     │
│                                                                       │
│  ┌────────────────────────────────────────────────────────────┐     │
│  │              Geyser Plugin Interface                       │     │
│  │  ┌──────────────────────────────────────────────────────┐  │     │
│  │  │ trait GeyserPlugin                                   │  │     │
│  │  │  - on_load()                                         │  │     │
│  │  │  - update_account()                                  │  │     │
│  │  │  - update_slot_status()                              │  │     │
│  │  │  - notify_transaction()                              │  │     │
│  │  │  - notify_block_metadata()                           │  │     │
│  │  │  - account_data_notifications_enabled()              │  │     │
│  │  │  - transaction_notifications_enabled()               │  │     │
│  │  └──────────────────────────────────────────────────────┘  │     │
│  └────────────────────────────────────────────────────────────┘     │
└───────────────────────────────┬─────────────────────────────────────┘
                                │ Dynamic Loading (.so)
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│              Solana AccountsDB Kafka Plugin (libsolana_...so)       │
│                                                                       │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  KafkaPlugin (src/plugin.rs)                                │    │
│  │  ┌────────────────┐  ┌────────────────┐  ┌──────────────┐  │    │
│  │  │   Publisher    │  │     Filter     │  │ Prometheus   │  │    │
│  │  │   (Kafka)      │  │   (Filters)    │  │   Service    │  │    │
│  │  └────────────────┘  └────────────────┘  └──────────────┘  │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                       │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  Config (src/config.rs)                                     │    │
│  │  - Kafka 配置                                                │    │
│  │  - Filter 配置                                               │    │
│  │  - Topic 映射                                                │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                       │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  Event Types (proto/event.proto → Rust types)              │    │
│  │  - UpdateAccountEvent                                       │    │
│  │  - TransactionEvent                                         │    │
│  │  - SlotStatusEvent                                          │    │
│  │  - MessageWrapper                                           │    │
│  └─────────────────────────────────────────────────────────────┘    │
└───────────────────────────────┬─────────────────────────────────────┘
                                │ rdkafka
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                          Apache Kafka                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │
│  │   accounts   │  │ transactions │  │ slot_status  │              │
│  │    topic     │  │    topic     │  │    topic     │              │
│  └──────────────┘  └──────────────┘  └──────────────┘              │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 🔧 组件详细分析

### 1. Plugin Core (src/plugin.rs)

#### 1.1 KafkaPlugin 结构

```rust
pub struct KafkaPlugin {
    publisher: Option<Publisher>,      // Kafka 发布者
    filter: Option<Vec<Filter>>,       // 过滤器链
    prometheus: Option<PrometheusService>, // 监控服务
}
```

**职责**:
- 实现 GeyserPlugin trait
- 协调各组件交互
- 处理 Geyser 回调

#### 1.2 关键方法实现状态

| 方法 | 实现状态 | 代码位置 | 说明 |
|------|---------|---------|------|
| `on_load()` | ✅ 已实现 | L50-87 | 加载配置，初始化组件 |
| `on_unload()` | ✅ 已实现 | L89-96 | 清理资源 |
| `update_account()` | ✅ 已实现 | L98-135 | 账户更新回调 |
| `update_slot_status()` | ✅ 已实现 | L137-164 | Slot 状态更新 |
| `notify_transaction()` | ✅ 已实现 | L166-206 | 交易通知回调 |
| `notify_block_metadata()` | ❌ 未实现 | - | **需要添加** |
| `account_data_notifications_enabled()` | ✅ 已实现 | L208-214 | 控制账户通知 |
| `transaction_notifications_enabled()` | ✅ 已实现 | L216-222 | 控制交易通知 |

#### 1.3 数据转换流程

```rust
// Geyser 接口 → 内部事件 → Protobuf → Kafka

ReplicaAccountInfoVersions  // Geyser 类型
    ↓ unwrap_update_account()
ReplicaAccountInfoV3        // 提取最新版本
    ↓ build UpdateAccountEvent
UpdateAccountEvent          // Protobuf 类型
    ↓ encode_to_vec() / encode_with_wrapper()
Vec<u8>                     // 序列化字节
    ↓ Publisher::update_account()
Kafka Topic                 // 发送到 Kafka
```

---

### 2. Filter System (src/filter.rs)

#### 2.1 Filter 结构

```rust
pub struct Filter {
    // 过滤器标志
    pub publish_all_accounts: bool,
    pub include_vote_transactions: bool,
    pub include_failed_transactions: bool,
    
    // 程序和账户过滤器
    pub program_ignores: HashSet<[u8; 32]>,
    pub program_filters: HashSet<[u8; 32]>,
    pub account_filters: HashSet<[u8; 32]>,
    
    // Topic 配置
    pub update_account_topic: String,
    pub slot_status_topic: String,
    pub transaction_topic: String,
    
    // 包装选项
    pub wrap_messages: bool,
}
```

#### 2.2 过滤逻辑

```rust
// 程序过滤
pub fn wants_program(&self, program: &[u8]) -> bool {
    // 1. 不在 ignore 列表中
    // 2. program_filters 为空（接受所有）OR 在 filters 列表中
    !self.program_ignores.contains(key) &&
    (self.program_filters.is_empty() || self.program_filters.contains(key))
}

// 账户过滤
pub fn wants_account(&self, account: &[u8]) -> bool {
    // 只有在 account_filters 中的账户才被接受
    self.account_filters.contains(key)
}
```

**过滤决策树**:
```
账户更新
  ├─ is_startup && !publish_all_accounts → 忽略
  ├─ !wants_program(owner) → 忽略
  ├─ !wants_account(pubkey) → 忽略
  └─ 通过 → 发布

交易
  ├─ is_vote && !include_vote_transactions → 忽略
  ├─ is_failed && !include_failed_transactions → 忽略
  ├─ 无账户匹配 program_filters/account_filters → 忽略
  └─ 通过 → 发布
```

---

### 3. Publisher (src/publisher.rs)

#### 3.1 Publisher 结构

```rust
pub struct Publisher {
    producer: ThreadedProducer<StatsThreadedProducerContext>,
    shutdown_timeout: Duration,
}
```

#### 3.2 发布方法

```rust
// 账户更新发布
pub fn update_account(
    &self,
    ev: UpdateAccountEvent,
    wrap_messages: bool,
    topic: &str,
) -> Result<(), KafkaError>

// Slot 状态发布
pub fn update_slot_status(
    &self,
    ev: SlotStatusEvent,
    wrap_messages: bool,
    topic: &str,
) -> Result<(), KafkaError>

// 交易发布
pub fn update_transaction(
    &self,
    ev: TransactionEvent,
    wrap_messages: bool,
    topic: &str,
) -> Result<(), KafkaError>
```

#### 3.3 消息键策略

| 事件类型 | 键类型 | 键内容 | 包装前缀 |
|---------|--------|--------|----------|
| Account | bytes | pubkey (32 bytes) | `A` |
| Slot | u64 | slot (8 bytes LE) | `S` |
| Transaction | bytes | signature | `T` |

```rust
// 消息键构造
let (key, buf) = if wrap_messages {
    temp_key = self.copy_and_prepend(ev.pubkey.as_slice(), b'A');
    (&temp_key, Self::encode_with_wrapper(Account(Box::new(ev))))
} else {
    (&ev.pubkey, ev.encode_to_vec())
};
```

#### 3.4 包装消息格式

```protobuf
message MessageWrapper {
  oneof event_message {
    UpdateAccountEvent account = 1;     // 标签: 'A'
    SlotStatusEvent slot = 2;           // 标签: 'S'
    TransactionEvent transaction = 3;   // 标签: 'T'
  }
}
```

**好处**:
- 单一 topic 可以接收多种事件
- 消费者可以通过类型标签区分事件
- 减少 topic 数量

---

### 4. Configuration (src/config.rs)

#### 4.1 配置结构

```rust
pub struct Config {
    libpath: String,                      // 插件库路径
    kafka: HashMap<String, String>,       // Kafka 配置
    shutdown_timeout_ms: u64,             // 关闭超时
    filters: Vec<ConfigFilter>,           // 过滤器配置
    prometheus: Option<SocketAddr>,       // Prometheus 端口
}

pub struct ConfigFilter {
    // Topic 配置
    update_account_topic: String,
    slot_status_topic: String,
    transaction_topic: String,
    
    // 过滤器
    program_ignores: Vec<String>,         // 要忽略的程序
    program_filters: Vec<String>,         // 要包含的程序
    account_filters: Vec<String>,         // 要包含的账户
    
    // 标志
    publish_all_accounts: bool,           // 启动时发布所有账户
    include_vote_transactions: bool,      // 包含投票交易
    include_failed_transactions: bool,    // 包含失败交易
    wrap_messages: bool,                  // 使用 MessageWrapper
}
```

#### 4.2 默认 Kafka 配置

```rust
fn fill_defaults(&mut self) {
    self.set_default("request.required.acks", "1");       // 至少一个 broker 确认
    self.set_default("message.timeout.ms", "30000");      // 30 秒超时
    self.set_default("compression.type", "lz4");          // LZ4 压缩
    self.set_default("partitioner", "murmur2_random");    // MurmurHash2 分区
}
```

**性能调优建议**:
```rust
// 高吞吐量配置
"batch.size": "1000000",              // 1MB 批量大小
"linger.ms": "10",                    // 10ms 等待批量
"buffer.memory": "67108864",          // 64MB 缓冲区
"compression.type": "lz4",            // 快速压缩

// 低延迟配置
"batch.size": "16384",                // 16KB 批量
"linger.ms": "0",                     // 立即发送
"acks": "1",                          // 单个确认
```

---

### 5. Event Types (proto/event.proto)

#### 5.1 消息层次结构

```
MessageWrapper (可选包装)
  ├─ UpdateAccountEvent
  │   ├─ 基础字段 (slot, pubkey, lamports, owner, ...)
  │   └─ 分析字段 (data_version, is_startup, account_age)
  │
  ├─ SlotStatusEvent
  │   ├─ 基础字段 (slot, parent, status)
  │   └─ 分析字段 (is_confirmed, confirmation_count, status_description)
  │
  └─ TransactionEvent
      ├─ 基础字段 (signature, slot, is_vote)
      ├─ SanitizedTransaction (完整交易数据)
      ├─ TransactionStatusMeta (执行结果)
      └─ 分析字段 (compute_units, cost, logs, ...)
```

#### 5.2 重要嵌套类型

**SanitizedTransaction**:
```protobuf
message SanitizedTransaction {
  SanitizedMessage message_ = 1;      // 消息内容
  bytes message_hash = 2;             // 消息哈希
  bool is_simple_vote_transaction = 3;
  repeated bytes signatures = 4;      // 签名列表
}
```

**TransactionStatusMeta**:
```protobuf
message TransactionStatusMeta {
  bool is_status_err = 1;             // 是否失败
  string error_info = 2;              // 错误信息
  uint64 fee = 3;                     // 手续费
  repeated uint64 pre_balances = 4;   // 执行前余额
  repeated uint64 post_balances = 5;  // 执行后余额
  repeated InnerInstructions inner_instructions = 6; // CPI
  repeated string log_messages = 7;   // **程序日志** ⭐
  repeated TransactionTokenBalance pre_token_balances = 8;
  repeated TransactionTokenBalance post_token_balances = 9;
  repeated Reward rewards = 10;
  uint32 compute_units_consumed = 11; // 计算单元
  // ...
}
```

**log_messages 是关键**:
- 包含所有程序输出
- 格式化为字符串数组
- 包含 Anchor 事件数据（base64 编码）

---

### 6. Prometheus Metrics (src/prom.rs)

#### 6.1 现有指标

```rust
lazy_static! {
    // 上传计数器
    pub static ref UPLOAD_ACCOUNTS_TOTAL: IntCounterVec = 
        register_int_counter_vec!(
            "geyser_account_uploads_total",
            "Total number of account updates published",
            &["status"]  // success/failed
        ).unwrap();
    
    pub static ref UPLOAD_SLOTS_TOTAL: IntCounterVec = 
        register_int_counter_vec!(
            "geyser_slot_uploads_total",
            "Total number of slot status published",
            &["status"]
        ).unwrap();
    
    pub static ref UPLOAD_TRANSACTIONS_TOTAL: IntCounterVec = 
        register_int_counter_vec!(
            "geyser_transaction_uploads_total",
            "Total number of transactions published",
            &["status"]
        ).unwrap();
    
    // Kafka 生产者统计
    pub static ref KAFKA_PRODUCER_QUEUE_SIZE: IntGauge = 
        register_int_gauge!(
            "kafka_producer_queue_size",
            "Number of messages in Kafka producer queue"
        ).unwrap();
    
    pub static ref KAFKA_PRODUCER_MSG_COUNT: IntCounter = 
        register_int_counter!(
            "kafka_producer_msg_count",
            "Total messages sent to Kafka"
        ).unwrap();
}
```

#### 6.2 PrometheusService

```rust
pub struct PrometheusService {
    runtime: Runtime,    // Tokio 运行时
    addr: SocketAddr,    // 监听地址
}

// HTTP 端点: GET /metrics
// 返回: Prometheus 文本格式
```

---

## 🔄 数据流详细分析

### 流程 1: 账户更新

```
1. Validator 账户状态变更
   ↓
2. Geyser Interface 调用 update_account()
   ↓ (plugin.rs:98)
3. KafkaPlugin::update_account() 接收
   ├─ 检查 is_startup 和 publish_all_accounts
   ├─ 获取 ReplicaAccountInfoV3
   ├─ 应用 Filter::wants_program() / wants_account()
   └─ 构建 UpdateAccountEvent
   ↓
4. Publisher::update_account()
   ├─ 选择键: pubkey
   ├─ 序列化: Protobuf
   ├─ 可选包装: MessageWrapper
   └─ Kafka BaseRecord
   ↓
5. ThreadedProducer::send()
   ├─ 加入发送队列
   ├─ 批量打包
   ├─ 压缩（LZ4）
   └─ 发送到 Kafka
   ↓
6. Kafka Topic: update_account_topic
   └─ 持久化到日志
```

### 流程 2: 交易处理

```
1. Validator 执行交易
   ↓
2. Geyser Interface 调用 notify_transaction()
   ↓ (plugin.rs:166)
3. KafkaPlugin::notify_transaction() 接收
   ├─ 获取 ReplicaTransactionInfoV3
   ├─ 检查 is_vote / is_failed
   ├─ 应用账户/程序过滤器
   └─ 调用 build_transaction_event()
       ├─ 构建 SanitizedTransaction
       │   ├─ Legacy / V0 消息
       │   ├─ 指令列表
       │   └─ 地址表查询
       ├─ 构建 TransactionStatusMeta
       │   ├─ 状态和错误
       │   ├─ 余额变化
       │   ├─ **log_messages** ⭐
       │   ├─ Token 余额
       │   └─ 内部指令
       ├─ 计算分析字段
       │   ├─ compute_units_consumed
       │   ├─ compute_units_price
       │   ├─ total_cost
       │   ├─ instruction_count
       │   └─ account_count
       └─ 生成 TransactionEvent
   ↓
4. Publisher::update_transaction()
   └─ 发送到 Kafka
```

**关键观察**: 
- ✅ `log_messages` 已经在 TransactionStatusMeta 中
- ✅ 也复制到 TransactionEvent.execution_logs
- ❌ 但未单独提取和发布

### 流程 3: Slot 状态更新

```
1. Validator Slot 状态变化
   ↓
2. Geyser Interface 调用 update_slot_status()
   ↓ (plugin.rs:137)
3. KafkaPlugin::update_slot_status() 接收
   ├─ 转换 PluginSlotStatus → SlotStatus
   ├─ 计算派生字段
   │   ├─ is_confirmed (Confirmed/Rooted)
   │   ├─ confirmation_count (0/1/2)
   │   └─ status_description (描述文本)
   └─ 构建 SlotStatusEvent
   ↓
4. Publisher::update_slot_status()
   └─ 发送到 Kafka
```

---

## 🧩 待实现组件设计

### 1. BlockMetadata 处理

**接口签名**:
```rust
fn notify_block_metadata(
    &self,
    blockinfo: ReplicaBlockInfoVersions,
) -> PluginResult<()>
```

**可用数据** (来自 Geyser):
```rust
pub struct ReplicaBlockInfo {
    pub slot: u64,
    pub blockhash: String,
    pub parent_slot: u64,
    pub parent_blockhash: String,
    pub rewards: Vec<Reward>,
    pub block_time: Option<i64>,
    pub block_height: Option<u64>,
    pub executed_transaction_count: u64,
}
```

**实现伪代码**:
```rust
fn notify_block_metadata(&self, blockinfo: ReplicaBlockInfoVersions) -> PluginResult<()> {
    let info = Self::unwrap_block_metadata(blockinfo);
    let publisher = self.unwrap_publisher();
    
    for filter in self.unwrap_filters() {
        if !filter.block_metadata_topic.is_empty() {
            let event = BlockMetadataEvent {
                slot: info.slot,
                block_hash: info.blockhash.clone(),
                parent_slot: info.parent_slot,
                parent_block_hash: info.parent_blockhash.clone(),
                leader: /* 需要从其他地方获取 */,
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
```

### 2. LogParser 模块

**设计目标**:
- 高性能字符串解析
- 维护程序调用栈
- 识别多种日志格式

**核心结构**:
```rust
pub struct LogParser {
    program_stack: Vec<String>,  // ["Program1", "Program2", ...]
    depth: u32,                  // 当前深度
}

pub enum LogType {
    Invoke,      // "Program XXX invoke [N]"
    Log,         // "Program log: XXX"
    Data,        // "Program data: XXX"
    Success,     // "Program XXX success"
    Failed,      // "Program XXX failed: XXX"
    Consumed,    // "Program XXX consumed N of M compute units"
    Unknown,
}
```

**解析状态机**:
```
┌──────────────────────────────────────┐
│           开始解析日志                │
└─────────────┬────────────────────────┘
              │
              ▼
┌──────────────────────────────────────┐
│      检查日志前缀                     │
├──────────────────────────────────────┤
│ "Program XXX invoke [N]" → INVOKE    │
│ "Program log: " → LOG                │
│ "Program data: " → DATA              │
│ "Program XXX success" → SUCCESS      │
│ "Program XXX failed" → FAILED        │
│ "Program XXX consumed" → CONSUMED    │
└─────────────┬────────────────────────┘
              │
              ▼
    ┌─────────┴──────────┐
    │                    │
    ▼                    ▼
┌────────┐          ┌────────┐
│ INVOKE │          │  DATA  │
└───┬────┘          └───┬────┘
    │                   │
    │ Push to stack     │ Extract base64
    │                   │ Decode
    │                   │ Parse discriminator
    │                   │
    ▼                   ▼
┌────────┐          ┌────────────┐
│SUCCESS │          │ ChainEvent │
│/FAILED │          └────────────┘
└───┬────┘
    │
    │ Pop from stack
    │
    ▼
```

**关键算法**:
```rust
impl LogParser {
    pub fn parse_program_logs(
        &mut self,
        log_messages: &[String],
        signature: &[u8],
        slot: u64,
        tx_index: u32,
    ) -> Vec<ProgramLogEvent> {
        let mut events = Vec::new();
        
        for (index, message) in log_messages.iter().enumerate() {
            let log_type = self.identify_log_type(message);
            
            match log_type {
                LogType::Invoke => {
                    if let Some(program_id) = self.extract_program_id(message) {
                        self.program_stack.push(program_id.clone());
                        self.depth += 1;
                        
                        events.push(ProgramLogEvent {
                            slot,
                            tx_signature: signature.to_vec(),
                            tx_index,
                            program_id,
                            message: message.clone(),
                            log_index: index as u32,
                            depth: self.depth,
                            log_type: LogType::Invoke.into(),
                        });
                    }
                }
                LogType::Success | LogType::Failed => {
                    if !self.program_stack.is_empty() {
                        let program_id = self.program_stack.pop().unwrap();
                        self.depth = self.depth.saturating_sub(1);
                        
                        events.push(ProgramLogEvent {
                            slot,
                            tx_signature: signature.to_vec(),
                            tx_index,
                            program_id,
                            message: message.clone(),
                            log_index: index as u32,
                            depth: self.depth + 1,
                            log_type: log_type.into(),
                        });
                    }
                }
                _ => {
                    // 其他日志类型
                    if let Some(program_id) = self.program_stack.last() {
                        events.push(ProgramLogEvent {
                            slot,
                            tx_signature: signature.to_vec(),
                            tx_index,
                            program_id: program_id.clone(),
                            message: message.clone(),
                            log_index: index as u32,
                            depth: self.depth,
                            log_type: log_type.into(),
                        });
                    }
                }
            }
        }
        
        events
    }
    
    fn identify_log_type(&self, message: &str) -> LogType {
        if message.contains(" invoke [") {
            LogType::Invoke
        } else if message.starts_with("Program log: ") {
            LogType::Log
        } else if message.starts_with("Program data: ") {
            LogType::Data
        } else if message.ends_with(" success") {
            LogType::Success
        } else if message.contains(" failed: ") {
            LogType::Failed
        } else if message.contains(" consumed ") {
            LogType::Consumed
        } else {
            LogType::Unknown
        }
    }
    
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
}
```

---

## 📊 性能分析

### 当前性能特征

**CPU 使用**:
- Protobuf 序列化: ~10-50 μs per message
- Kafka 发送准备: ~5-10 μs
- 过滤检查: ~1-2 μs

**内存使用**:
- 每个事件: ~200 bytes - 10 KB (取决于数据大小)
- Kafka 缓冲区: 默认 64 MB
- 程序栈: 可忽略

**延迟**:
- 端到端延迟: < 1 ms (不含网络)
- Kafka 发送延迟: 10-100 ms (取决于配置)

### 潜在瓶颈

1. **日志解析**:
   - 字符串操作密集
   - 每笔交易 50-200 条日志
   - **优化**: 使用字节切片而非 String::split

2. **Protobuf 序列化**:
   - 大交易数据（> 1 KB）
   - **优化**: 考虑增量序列化

3. **Kafka 发送**:
   - 单条发送开销大
   - **优化**: 批量发送

---

## 🔐 安全和可靠性

### 错误处理策略

```rust
// 当前策略
fn notify_transaction(...) -> PluginResult<()> {
    // 失败会返回错误，可能中断 validator
    publisher.update_transaction(...)?;
    Ok(())
}

// 改进策略
fn notify_transaction(...) -> PluginResult<()> {
    if let Err(e) = publisher.update_transaction(...) {
        error!("Failed to publish transaction: {}", e);
        // 记录指标但不中断
        UPLOAD_ERRORS.inc();
    }
    Ok(()) // 始终返回 Ok，避免影响 validator
}
```

### 数据一致性保证

**Kafka 配置**:
```
acks=1                // 至少一个 broker 确认
retries=3             // 重试 3 次
max.in.flight=1       // 保证顺序（但降低吞吐量）
```

**At-Least-Once 语义**:
- Kafka 重试可能导致重复消息
- 消费者需要幂等处理
- 使用 slot + signature 去重

---

## 🎯 总结和建议

### 架构优势

1. ✅ **清晰的分层**: Plugin → Filter → Publisher
2. ✅ **灵活配置**: 支持多 filter、多 topic
3. ✅ **性能监控**: 集成 Prometheus
4. ✅ **类型安全**: Protobuf + Rust

### 主要改进点

1. ⚠️ **实现 notify_block_metadata**: 补全区块数据
2. ⚠️ **添加 LogParser**: 提取程序日志和事件
3. ⚠️ **优化错误处理**: 避免影响 validator
4. ⚠️ **批量发送**: 提高 Kafka 吞吐量

### 下一步行动

参考 `REFACTORING_PLAN.md` 中的详细实施计划。

---

**文档版本**: 1.0  
**最后更新**: 2025-01-XX
