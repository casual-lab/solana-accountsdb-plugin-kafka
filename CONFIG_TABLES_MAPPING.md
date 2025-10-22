# Solana Kafka Plugin - 数据表配置映射指南

## 概述

本文档说明如何配置 Solana Kafka Plugin 以输出支持 7 个数据表所需的信息。

## 数据表与配置映射

### 1. chain_blocks (区块基础信息)

**配置项**: `slot_status_topic`

**输出字段**:
- `slot` - 区块槽位号
- `block_hash` - 区块哈希（需从插件获取）
- `parent_slot` - 父区块槽位
- `leader` - 区块领导者（需从插件获取）
- `num_txs` - 交易数量（需计数）
- `timestamp` - 时间戳

**配置示例**:
```json
{
  "slot_status_topic": "solana.chain_blocks",
  "extract_block_metadata": true,
  "track_block_leaders": true
}
```

**当前支持状态**: ✅ 部分支持
- ✅ slot, parent_slot, status - 已支持
- ⚠️ block_hash, leader, num_txs - 需要插件增强

---

### 2. chain_txs (交易明细)

**配置项**: `transaction_topic`

**输出字段**:
- `tx_signature` - 交易签名
- `slot` - 所属槽位
- `status` - 交易状态 (success/failed)
- `fee` - 交易费用
- `compute_units` - 计算单元消耗
- `timestamp` - 时间戳

**配置示例**:
```json
{
  "transaction_topic": "solana.chain_txs",
  "include_vote_transactions": true,
  "include_failed_transactions": true,
  "extract_transaction_metadata": true,
  "track_compute_units": true
}
```

**当前支持状态**: ✅ 完全支持
- ✅ tx_signature, slot, status - 已支持
- ✅ fee - 从 meta 中获取
- ✅ compute_units - 从 meta 中获取
- ✅ timestamp - 可从 slot 计算

---

### 3. chain_accounts (账户快照)

**配置项**: `update_account_topic`

**输出字段**:
- `pubkey` - 账户公钥
- `owner_program` - 所有者程序
- `lamports` - 账户余额
- `slot` - 更新时的槽位
- `write_version` - 写入版本

**配置示例**:
```json
{
  "update_account_topic": "solana.chain_accounts",
  "publish_all_accounts": true,
  "extract_account_metadata": true,
  "program_filters": [],
  "account_filters": []
}
```

**当前支持状态**: ✅ 完全支持
- ✅ 所有字段已在当前插件中支持

---

### 4. chain_program_logs (合约日志事件)

**配置项**: `program_log_topic` (新增)

**输出字段**:
- `tx_signature` - 交易签名
- `program_id` - 程序 ID
- `message` - 日志消息
- `slot` - 槽位
- `timestamp` - 时间戳

**配置示例**:
```json
{
  "program_log_topic": "solana.chain_program_logs",
  "include_program_logs": true,
  "log_level": "all",
  "program_filters": ["YOUR_PROGRAM_ID"]
}
```

**当前支持状态**: ❌ 需要实现
- 需要解析 `ReplicaTransactionInfo` 中的 log messages
- 需要添加新的 protobuf 消息类型
- 需要在插件中添加日志提取和发送逻辑

**实现建议**:
```rust
// 在 notify_transaction 中解析日志
if let Some(logs) = &info.transaction_status_meta.log_messages {
    for log in logs {
        if log.starts_with("Program log:") {
            // 提取并发送到 program_log_topic
        }
    }
}
```

---

### 5. chain_events (Anchor/自定义事件)

**配置项**: `event_topic` (新增)

**输出字段**:
- `program` - 程序地址
- `event_type` - 事件类型
- `payload` - 事件数据 (JSON)
- `slot` - 槽位
- `timestamp` - 时间戳

**配置示例**:
```json
{
  "event_topic": "solana.chain_events",
  "include_events": true,
  "parse_anchor_events": true,
  "parse_custom_events": true,
  "program_filters": ["YOUR_ANCHOR_PROGRAM_ID"]
}
```

**当前支持状态**: ❌ 需要实现
- 需要解析 Anchor 事件日志 (以 "Program data:" 开头的 base64 数据)
- 需要添加事件解码逻辑
- 需要支持自定义事件格式

**Anchor 事件格式**:
```
Program data: <base64_encoded_event>
```
- 前 8 字节: 事件判别器 (discriminator)
- 后续字节: Borsh 序列化的事件数据

**实现建议**:
```rust
// 解析 Anchor 事件
if log.starts_with("Program data:") {
    let data = base64::decode(&log[13..])?;
    let discriminator = &data[0..8];
    let event_data = &data[8..];
    // 根据 discriminator 解析对应的事件结构
}
```

---

### 6. lp_positions (用户 LP 持仓表)

**配置项**: 使用 `domain_specific_filters`

**输出字段**:
- `owner` - 持仓用户
- `lp_token_amount` - LP Token 数量
- `staked_amount` - 质押数量
- `timestamp` - 时间戳

**配置示例**:
```json
{
  "domain_specific_filters": [
    {
      "update_account_topic": "solana.lp_positions",
      "program_filters": [
        "RAYDIUM_PROGRAM_ID",
        "ORCA_PROGRAM_ID",
        "YOUR_LP_PROGRAM_ID"
      ],
      "publish_all_accounts": false,
      "extract_account_metadata": true
    }
  ]
}
```

**数据获取方式**: ✅ 可通过现有功能实现
1. 监听特定 LP 程序的账户更新
2. 从账户 data 中解析 LP 持仓信息
3. 需要了解具体 LP 程序的账户结构

**解析示例** (Raydium):
```rust
// Raydium LP 账户结构 (简化)
struct LpAccount {
    owner: Pubkey,           // offset 0
    pool: Pubkey,            // offset 32
    lp_amount: u64,          // offset 64
    staked_amount: u64,      // offset 72
    // ...
}
```

---

### 7. reward_history (奖励发放记录表)

**配置项**: 使用 `domain_specific_filters`

**输出字段**:
- `owner` - 奖励接收者
- `reward_token` - 奖励代币
- `amount` - 奖励数量
- `block_slot` - 区块槽位
- `timestamp` - 时间戳

**配置示例**:
```json
{
  "domain_specific_filters": [
    {
      "transaction_topic": "solana.reward_history",
      "event_topic": "solana.reward_events",
      "program_filters": [
        "YOUR_STAKING_PROGRAM_ID",
        "YOUR_REWARD_PROGRAM_ID"
      ],
      "include_failed_transactions": false,
      "parse_anchor_events": true
    }
  ]
}
```

**数据获取方式**: ✅ 可通过现有功能实现
1. 监听奖励发放程序的交易
2. 解析交易中的 token 转账指令
3. 或解析 Anchor 事件中的奖励发放事件

**实现方式**:
- **方式1**: 解析交易指令 - 查找 Token Program 的 Transfer 指令
- **方式2**: 解析 Anchor 事件 - 如果程序发出 `RewardClaimed` 事件
- **方式3**: 监听特定账户的余额变化

---

## 配置文件结构

### 基础配置
```json
{
  "libpath": "target/release/libsolana_accountsdb_plugin_kafka.so",
  "kafka": {
    "bootstrap.servers": "localhost:19092",
    "compression.type": "lz4"
  },
  "shutdown_timeout_ms": 30000,
  "filters": [...],
  "domain_specific_filters": [...]
}
```

### 全局过滤器 (filters)
用于捕获所有链上数据，对应表：
- chain_blocks
- chain_txs
- chain_accounts
- chain_program_logs (新增)
- chain_events (新增)

### 领域特定过滤器 (domain_specific_filters)
用于捕获特定业务数据，对应表：
- lp_positions
- reward_history
- 其他自定义业务表

---

## 实施步骤

### 阶段 1: 使用现有功能 ✅
1. 配置 `chain_blocks` (slot_status_topic)
2. 配置 `chain_txs` (transaction_topic)
3. 配置 `chain_accounts` (update_account_topic)
4. 配置 `lp_positions` (通过 program_filters)
5. 配置 `reward_history` (通过 program_filters)

### 阶段 2: 插件增强 ⚠️
需要修改插件代码以支持：
1. **chain_program_logs**: 添加日志提取和发送
2. **chain_events**: 添加 Anchor 事件解析
3. **chain_blocks**: 增强元数据提取（block_hash, leader, num_txs）

### 阶段 3: 数据处理 📊
在 Kafka 消费端或数据库层：
1. 从 LP 账户数据中解析持仓信息
2. 从交易/事件中提取奖励记录
3. 关联 slot 和 timestamp
4. 数据验证和清洗

---

## 配置优化建议

### 1. 性能优化
```json
{
  "kafka": {
    "batch.size": "1000000",
    "linger.ms": "100",
    "compression.type": "lz4",
    "queue.buffering.max.messages": "1000000"
  }
}
```

### 2. 过滤优化
```json
{
  "program_ignores": [
    "Vote111111111111111111111111111111111111111"
  ],
  "publish_all_accounts": false,
  "include_vote_transactions": false
}
```

### 3. 主题命名规范
```
solana.{environment}.{table_name}
例如:
- solana.mainnet.chain_blocks
- solana.mainnet.chain_txs
- solana.devnet.lp_positions
```

---

## 数据流示意图

```
Solana Validator
    ↓
Geyser Plugin Interface
    ↓
Kafka Plugin (本插件)
    ├─→ chain_blocks → Kafka Topic → Database
    ├─→ chain_txs → Kafka Topic → Database
    ├─→ chain_accounts → Kafka Topic → Database
    ├─→ chain_program_logs → Kafka Topic → Database (需实现)
    ├─→ chain_events → Kafka Topic → Database (需实现)
    ├─→ lp_positions → Kafka Topic → Database (需数据解析)
    └─→ reward_history → Kafka Topic → Database (需数据解析)
```

---

## 示例配置文件

完整配置示例请参考:
- `config.json` - 基础配置
- `config.enhanced.json` - 增强配置（包含所有表的映射）

---

## 故障排查

### 1. 数据未发送到 Kafka
- 检查 topic 配置是否为空
- 检查 program_filters 是否正确
- 查看插件日志

### 2. 数据量过大
- 使用 program_filters 过滤
- 设置 publish_all_accounts = false
- 排除 vote 交易

### 3. 缺少某些字段
- 检查插件版本是否支持该字段
- 查看是否需要插件增强
- 考虑在数据处理层补充

---

## 下一步

1. **立即可用**: 使用 `config.enhanced.json` 配置前 3 个表和业务表
2. **短期目标**: 实现 program_logs 和 events 支持
3. **长期目标**: 添加更多元数据和业务逻辑解析

---

## 参考资源

- [Solana Geyser Plugin Interface](https://docs.solana.com/developing/plugins/geyser-plugins)
- [Anchor Events](https://www.anchor-lang.com/docs/events)
- [Kafka Configuration](https://kafka.apache.org/documentation/#producerconfigs)
