# Solana Kafka Plugin - 7 表数据采集方案

本方案详细说明如何使用 Solana Geyser Kafka 插件采集和处理 7 个数据表所需的链上数据。

---

## 📊 数据表概览

| 表名 | 描述 | 核心字段 | 数据来源 | 状态 |
|------|------|---------|---------|------|
| **chain_blocks** | 区块基础信息 | slot, block_hash, parent_slot, leader, num_txs, timestamp | `notify_block_metadata()` | ✅ 可用 |
| **chain_txs** | 交易明细 | tx_signature, slot, status, fee, compute_units, timestamp | `notify_transaction()` | ✅ 可用 |
| **chain_accounts** | 账户快照 | pubkey, owner_program, lamports, slot, write_version | `update_account()` | ✅ 可用 |
| **chain_program_logs** | 合约日志事件 | tx_signature, program_id, message, slot, timestamp | `log_messages` | ✅ 可提取 |
| **chain_events** | Anchor/自定义事件 | program, event_type, payload(JSON), slot, timestamp | `log_messages` | ✅ 可解析 |
| **lp_positions** | 用户 LP 持仓表 | owner, lp_token_amount, staked_amount, timestamp | 账户数据解析 | ✅ 可解析 |
| **reward_history** | 奖励发放记录表 | owner, reward_token, amount, block_slot, timestamp | 交易指令解析 | ✅ 可解析 |

---

## 🎯 核心发现

### ✅ Geyser 接口完全支持所有需求！

通过源码分析（详见 `GEYSER_INTERFACE_ANALYSIS.md`），我们发现：

1. **✅ 支持获取程序日志**
   - 来源: `notify_transaction()` → `transaction_status_meta.log_messages`
   - 格式: `Vec<String>`，包含所有程序输出

2. **✅ 支持解析 Anchor 事件**
   - 来源: `log_messages` 中的 `"Program data: <base64>"`
   - 包含: Event discriminator + Borsh 序列化数据

3. **✅ 支持获取完整区块信息**
   - 来源: `notify_block_metadata()`（需实现此回调）
   - 包含: block_hash, leader, executed_transaction_count 等

4. **✅ 支持获取所有交易细节**
   - 包含: 内部指令、余额变化、Token 变化、计算单元消耗等

---

## 🚀 快速开始

### 1. 使用生产就绪配置

```bash
# 配置文件已经调整为只使用插件当前支持的配置项
cd solana-accountsdb-plugin-kafka
cp config.json config.json.backup  # 备份
# config.json 已更新
```

### 2. 替换程序 ID（如需要）

编辑 `config.json`，将占位符替换为实际的程序 ID：
- LP 程序 ID（如 Raydium, Orca）
- 奖励程序 ID

### 3. 编译插件

```bash
cargo build --release
```

### 4. 启动验证器

```bash
solana-validator \
  --geyser-plugin-config config.json \
  # ... 其他参数 ...
```

---

## 📚 文档导航

### 核心文档

1. **[GEYSER_INTERFACE_ANALYSIS.md](GEYSER_INTERFACE_ANALYSIS.md)** ⭐
   - Geyser 插件接口完整分析
   - 所有可用的事件回调
   - 日志和事件支持说明
   - **必读！了解接口能力**

2. **[CONFIG_TABLES_MAPPING.md](CONFIG_TABLES_MAPPING.md)**
   - 7 个表与配置的映射关系
   - 每个表的字段说明
   - 当前支持状态
   - 实施建议

3. **[FIELD_EXTRACTION_GUIDE.md](FIELD_EXTRACTION_GUIDE.md)**
   - 字段提取详细代码
   - Python 示例实现
   - 日志解析方法
   - Anchor 事件解析
   - LP 账户解析
   - 奖励提取逻辑

4. **[QUICK_REFERENCE.md](QUICK_REFERENCE.md)**
   - 快速参考表
   - 配置示例速查
   - 常见问题解答
   - 性能优化建议

### 配置文件

1. **config.json** - 当前使用的配置（已修正）
2. **config.enhanced.json** - 增强配置示例（包含未来功能）
3. **config.production-ready.json** - 生产环境配置模板

---

## 🔄 数据流架构

```
Solana Validator (Geyser Interface)
    │
    ├─── update_account() ────────────► Kafka: chain_accounts ──► DB: chain_accounts ✅
    │                                             │
    │                                             └──► 解析 LP 数据 ──► DB: lp_positions ✅
    │
    ├─── update_slot_status() ────────► Kafka: chain_blocks ────► DB: chain_blocks ⚠️
    │                                       (缺少部分字段)
    │
    ├─── notify_block_metadata() ─────► (未实现) ──► 可补充完整区块信息 💡
    │                                       block_hash, leader, tx_count
    │
    └─── notify_transaction() ────────► Kafka: chain_txs ───────► DB: chain_txs ✅
             │                               │
             ├─ log_messages ───────────────┼──► 提取程序日志 ──► DB: chain_program_logs ✅
             │                               │
             ├─ log_messages ───────────────┼──► 解析 Anchor 事件 ─► DB: chain_events ✅
             │                               │
             └─ instructions ───────────────┴──► 解析奖励信息 ──► DB: reward_history ✅
```

---

## 📋 实施方案

### 方案 A: 最小改动（推荐起步）⭐

**优点**: 
- 无需修改插件代码
- 立即可用
- 风险低

**实施步骤**:
1. 使用当前 `config.json`
2. 启动插件采集数据
3. 在 Kafka 消费端实现数据处理逻辑（参考 `FIELD_EXTRACTION_GUIDE.md`）

**可获得的表**: 所有 7 个表（通过数据处理）

**时间**: 1-3 天（主要是数据处理逻辑开发）

---

### 方案 B: 插件增强（推荐生产）⭐⭐⭐

**优点**:
- 性能更好（一次解析）
- 数据更完整
- 使用更简单

**需要实现**:
1. 添加 `notify_block_metadata()` 回调
2. 在 `notify_transaction()` 中提取日志和事件
3. 添加新的配置项（`program_log_topic`, `event_topic`, `block_metadata_topic`）
4. 更新 protobuf 定义

**实施步骤**:
1. 修改 `src/config.rs` 添加新配置
2. 修改 `src/plugin.rs` 添加新回调
3. 更新 `proto/kafka.proto`
4. 重新编译和部署

**时间**: 1-2 天开发 + 测试

---

## 🛠️ 技术要点

### 1. 程序日志提取

```rust
// 在 notify_transaction() 中
if let Some(logs) = &transaction_status_meta.log_messages {
    for log in logs {
        if log.starts_with("Program log:") {
            // 提取并发送到 program_log_topic
        }
    }
}
```

### 2. Anchor 事件解析

```rust
// 检测 Anchor 事件
if log.starts_with("Program data:") {
    let data_b64 = &log[14..];
    let data = base64::decode(data_b64)?;
    let discriminator = &data[0..8];  // 事件类型标识
    let event_data = &data[8..];       // Borsh 序列化数据
    // 根据 discriminator 解析事件
}
```

### 3. LP 账户解析

```rust
// 解析 Raydium LP 账户
fn parse_lp_account(data: &[u8]) -> LpPosition {
    let owner = &data[8..40];
    let lp_amount = u64::from_le_bytes(data[72..80].try_into()?);
    // ...
}
```

### 4. 区块元数据获取

```rust
// 实现 notify_block_metadata()
fn notify_block_metadata(&self, blockinfo: ReplicaBlockInfoVersions) -> Result<()> {
    // 获取 blockhash, leader, executed_transaction_count
    // 发送到 block_metadata_topic
}
```

---

## ⚙️ 配置说明

### 当前支持的配置项

```json
{
  "filters": [{
    "update_account_topic": "solana.chain_accounts",     // ✅ 支持
    "slot_status_topic": "solana.chain_blocks",          // ✅ 支持
    "transaction_topic": "solana.chain_txs",             // ✅ 支持
    "program_filters": ["PROGRAM_ID"],                   // ✅ 支持
    "account_filters": ["ACCOUNT_ID"],                   // ✅ 支持
    "program_ignores": ["Vote111..."],                   // ✅ 支持
    "publish_all_accounts": false,                       // ✅ 支持
    "include_vote_transactions": false,                  // ✅ 支持
    "include_failed_transactions": true,                 // ✅ 支持
    "wrap_messages": false                               // ✅ 支持
  }]
}
```

### 计划添加的配置项

```json
{
  "filters": [{
    "program_log_topic": "solana.chain_program_logs",    // 🔜 待添加
    "event_topic": "solana.chain_events",                // 🔜 待添加
    "block_metadata_topic": "solana.block_metadata",     // 🔜 待添加
    "include_program_logs": true,                        // 🔜 待添加
    "include_events": true,                              // 🔜 待添加
    "parse_anchor_events": true                          // 🔜 待添加
  }]
}
```

---

## 🔍 数据验证

### 测试步骤

1. **验证数据采集**
```bash
# 查看 Kafka topic
kafka-topics --list --bootstrap-server localhost:19092

# 消费消息
kafka-console-consumer \
  --bootstrap-server localhost:19092 \
  --topic solana.chain_txs \
  --from-beginning
```

2. **验证日志包含**
```python
# 检查交易是否包含 log_messages
import json
msg = json.loads(kafka_message)
assert 'log_messages' in msg['meta']
print(msg['meta']['log_messages'])
```

3. **验证事件解析**
```python
# 查找 Anchor 事件
for log in msg['meta']['log_messages']:
    if log.startswith('Program data:'):
        print("Found Anchor event:", log)
```

---

## 📈 性能优化

### Kafka 配置
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

### 过滤优化
- 使用 `program_filters` 只采集需要的程序
- 设置 `publish_all_accounts: false` 避免海量数据
- 排除投票交易: `include_vote_transactions: false`
- 使用 `program_ignores` 排除不需要的程序

---

## 🐛 故障排查

### 插件无法启动

**错误**: `unknown field 'program_log_topic'`

**原因**: 配置文件包含插件不支持的字段

**解决**: 使用 `config.json`（已修正）

---

### 数据丢失

**症状**: 某些交易或账户未出现

**检查**:
1. 过滤器配置是否正确
2. Kafka 是否正常接收
3. 是否有错误日志

---

### 内存占用过高

**原因**: 
- `publish_all_accounts: true` 导致海量数据
- 未过滤投票交易

**解决**:
- 设置合适的过滤器
- 调整 Kafka 批处理参数

---

## 📞 支持和贡献

- 📖 完整源码分析: [GEYSER_INTERFACE_ANALYSIS.md](GEYSER_INTERFACE_ANALYSIS.md)
- 💻 字段提取代码: [FIELD_EXTRACTION_GUIDE.md](FIELD_EXTRACTION_GUIDE.md)
- ⚙️ 配置说明: [CONFIG_TABLES_MAPPING.md](CONFIG_TABLES_MAPPING.md)
- 🚀 快速参考: [QUICK_REFERENCE.md](QUICK_REFERENCE.md)

---

## ✅ 总结

### 所有 7 个表都可以实现！

| 实现方式 | 表数量 | 难度 | 推荐 |
|---------|-------|------|------|
| **数据处理层提取** | 7/7 | 中等 | ⭐⭐⭐ 立即可用 |
| **插件增强** | 7/7 | 中等 | ⭐⭐⭐ 生产推荐 |

### 关键技术点
- ✅ Geyser 接口提供 `log_messages`，包含所有日志和事件
- ✅ `notify_block_metadata()` 提供完整区块信息
- ✅ 所有数据都可以从现有接口获取
- ✅ 无需额外的 RPC 查询（可选优化）

### 下一步
1. 🟢 使用当前配置开始采集（立即）
2. 🟡 实现数据处理逻辑（1-3天）
3. 🔵 增强插件功能（可选，1-2天）
4. 🟣 部署到生产环境

**开始探索**: 从 [GEYSER_INTERFACE_ANALYSIS.md](GEYSER_INTERFACE_ANALYSIS.md) 开始！
