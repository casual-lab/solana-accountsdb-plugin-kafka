# 快速参考：7 个表的配置与实现状态

| 表名 | Kafka Topic | 配置项 | 状态 | 主要字段 | 实现方式 |
|------|------------|--------|------|---------|---------|
| **chain_blocks** | `solana.chain_blocks` | `slot_status_topic` | ⚠️ 部分支持 | slot, block_hash, parent_slot, leader, num_txs, timestamp | 插件直接输出 + 数据处理 |
| **chain_txs** | `solana.chain_txs` | `transaction_topic` | ✅ 完全支持 | tx_signature, slot, status, fee, compute_units, timestamp | 插件直接输出 |
| **chain_accounts** | `solana.chain_accounts` | `update_account_topic` | ✅ 完全支持 | pubkey, owner_program, lamports, slot, write_version | 插件直接输出 |
| **chain_program_logs** | `solana.chain_program_logs` | 需新增 `program_log_topic` | 🔄 需数据处理 | tx_signature, program_id, message, slot, timestamp | 从交易日志中提取 |
| **chain_events** | `solana.chain_events` | 需新增 `event_topic` | 🔄 需数据处理 | program, event_type, payload(JSON), slot, timestamp | 解析 Anchor 事件 |
| **lp_positions** | `solana.lp_positions` | 使用 `program_filters` | 🔄 需数据处理 | owner, lp_token_amount, staked_amount, timestamp | 解析账户数据结构 |
| **reward_history** | `solana.reward_history` | 使用 `program_filters` | 🔄 需数据处理 | owner, reward_token, amount, block_slot, timestamp | 解析交易指令或事件 |

---

## 配置文件选择指南

| 配置文件 | 用途 | 适用场景 |
|---------|------|---------|
| `config.json` | 基础配置 | 更新后包含主要 topic 配置 |
| `config.enhanced.json` | 增强配置 | 包含所有 7 个表的详细配置和说明 |
| `config.production-ready.json` | 生产就绪配置 | 立即可用，包含实用的过滤器设置 |

**推荐**: 使用 `config.production-ready.json` 作为起点。

---

## 实施路线图

### 🟢 Phase 1: 立即可用 (0-1 天)
使用现有插件功能：
```bash
# 1. 使用 config.production-ready.json
cp config.production-ready.json config.json

# 2. 替换程序 ID
# 编辑 config.json，将 LP 和 Reward 程序 ID 替换为实际值

# 3. 编译和启动
cargo build --release
# 启动验证器时加载配置
```

**可获得的表**:
- ✅ chain_txs (完整)
- ✅ chain_accounts (完整)
- ✅ chain_blocks (基础字段)

---

### 🟡 Phase 2: 数据处理层 (1-3 天)
实现 Kafka 消费者和数据提取逻辑：

```python
# 参考 FIELD_EXTRACTION_GUIDE.md 中的代码

# 1. 实现日志提取
logs = extract_program_logs(transaction)
# 保存到 chain_program_logs 表

# 2. 实现事件解析
events = parse_anchor_events(transaction)
# 保存到 chain_events 表

# 3. 实现 LP 持仓解析
lp_data = parse_lp_account(account_data)
# 保存到 lp_positions 表

# 4. 实现奖励提取
rewards = extract_rewards_from_transaction(transaction)
# 保存到 reward_history 表
```

**可获得的表**:
- ✅ chain_program_logs
- ✅ chain_events
- ✅ lp_positions
- ✅ reward_history

---

### 🔵 Phase 3: 插件增强 (可选, 3-7 天)
为了更好的性能和更完整的数据，增强插件功能：

**需要修改的文件**:
1. `src/config.rs` - 添加新配置项
2. `src/filter.rs` - 添加新过滤逻辑
3. `src/plugin.rs` - 添加日志和事件处理
4. `proto/kafka.proto` - 添加新消息类型

**改进**:
- 直接输出 program_logs
- 直接输出 parsed events
- 增强 block 元数据（hash, leader）
- 优化性能

---

## 配置示例速查

### 1. 采集所有链数据
```json
{
  "filters": [{
    "update_account_topic": "solana.chain_accounts",
    "slot_status_topic": "solana.chain_blocks",
    "transaction_topic": "solana.chain_txs",
    "publish_all_accounts": false,
    "include_vote_transactions": false,
    "include_failed_transactions": true
  }]
}
```

### 2. 只监控特定程序
```json
{
  "filters": [{
    "transaction_topic": "solana.my_program_txs",
    "program_filters": [
      "YOUR_PROGRAM_ID_HERE"
    ],
    "include_vote_transactions": false
  }]
}
```

### 3. 监控 LP 持仓
```json
{
  "filters": [{
    "update_account_topic": "solana.lp_positions",
    "program_filters": [
      "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
    ],
    "publish_all_accounts": false
  }]
}
```

### 4. 监控奖励发放
```json
{
  "filters": [{
    "transaction_topic": "solana.reward_history",
    "program_filters": [
      "REWARD_PROGRAM_ID"
    ],
    "include_failed_transactions": false
  }]
}
```

---

## 数据处理关键代码

### 从交易日志提取程序日志
```python
def extract_program_logs(tx_message):
    logs = tx_message['meta']['log_messages']
    program_logs = []
    current_program = None
    
    for log in logs:
        if 'invoke' in log:
            current_program = log.split()[1]
        elif log.startswith('Program log:'):
            program_logs.append({
                'program_id': current_program,
                'message': log[13:],
                'tx_signature': tx_message['signature']
            })
    
    return program_logs
```

### 解析 Anchor 事件
```python
def parse_anchor_event(log_line):
    if not log_line.startswith('Program data:'):
        return None
    
    data = base64.b64decode(log_line[14:])
    discriminator = struct.unpack('<Q', data[0:8])[0]
    event_data = data[8:]
    
    # 根据 discriminator 解析事件类型
    return {
        'event_type': get_event_name(discriminator),
        'payload': parse_event_data(event_data)
    }
```

### 解析 LP 账户
```python
def parse_lp_account(account_data):
    owner = account_data[8:40]
    lp_amount = struct.unpack('<Q', account_data[72:80])[0]
    
    return {
        'owner': base58.encode(owner),
        'lp_token_amount': lp_amount / 1e9
    }
```

---

## 常见问题

### Q1: 插件输出的数据格式是什么？
A: Protobuf 格式，需要反序列化。参考 `proto/kafka.proto`。

### Q2: 如何获取 block_hash?
A: 方法1: 通过 RPC 查询 `getBlock(slot)`  
方法2: 等待插件增强直接输出

### Q3: 如何识别 Anchor 事件类型?
A: 通过事件的 discriminator（前 8 字节），需要查看程序的 IDL 文件。

### Q4: LP 账户结构在哪里找?
A: 查看对应 DEX 的 SDK 或合约代码：
- Raydium: https://github.com/raydium-io/raydium-sdk
- Orca: https://github.com/orca-so/whirlpools

### Q5: 如何减少数据量?
A: 
- 使用 `program_filters` 过滤特定程序
- 设置 `publish_all_accounts: false`
- 设置 `include_vote_transactions: false`
- 排除不需要的程序（`program_ignores`）

---

## 性能优化建议

### Kafka 配置
```json
{
  "kafka": {
    "batch.size": "1000000",
    "linger.ms": "100",
    "compression.type": "lz4",
    "queue.buffering.max.messages": "1000000",
    "acks": "1"
  }
}
```

### 过滤器配置
```json
{
  "program_ignores": [
    "Vote111111111111111111111111111111111111111"
  ],
  "publish_all_accounts": false,
  "include_vote_transactions": false
}
```

---

## 监控和调试

### 查看 Kafka 数据
```bash
# 查看 topic 列表
kafka-topics --list --bootstrap-server localhost:19092

# 消费消息
kafka-console-consumer \
  --bootstrap-server localhost:19092 \
  --topic solana.chain_txs \
  --from-beginning
```

### 查看插件日志
```bash
# 验证器日志
tail -f validator.log | grep -i kafka
```

---

## 相关文档

- 📘 [CONFIG_TABLES_MAPPING.md](CONFIG_TABLES_MAPPING.md) - 详细配置说明
- 📗 [FIELD_EXTRACTION_GUIDE.md](FIELD_EXTRACTION_GUIDE.md) - 字段提取代码
- 📕 配置文件:
  - [config.json](config.json) - 基础配置
  - [config.enhanced.json](config.enhanced.json) - 增强配置
  - [config.production-ready.json](config.production-ready.json) - 生产配置

---

## 联系和支持

- GitHub Issues: [项目仓库]
- 文档: [Solana Geyser Plugin Docs](https://docs.solana.com/developing/plugins/geyser-plugins)
