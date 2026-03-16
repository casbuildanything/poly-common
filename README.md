# poly-common

Polymarket 微服务公共类型库，包含：

- **proto/**: Protobuf 消息定义
- **pb/**: 生成的 Go 代码
- **kafka/**: Kafka 生产者/消费者封装

## 安装

```bash
go get github.com/poly-common
```

## 消息类型

### TradeEvent (poly.trades)

交易事件，包含用户交易的详细信息。

### MarketCreatedEvent (poly.markets)

新市场创建事件，当链上 ConditionPreparation 事件触发时发送。

## 生成 Protobuf

```bash
# 安装工具
make install-tools

# 生成代码
make proto
```

## Kafka Topics

| Topic | 消息类型 | 说明 |
|-------|---------|------|
| `poly.trades` | TradeEvent | 交易数据 |
| `poly.markets` | MarketCreatedEvent | 新市场创建 |

