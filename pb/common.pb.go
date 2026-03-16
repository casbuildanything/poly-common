// Package pb contains protobuf message definitions for Polymarket events.
package pb

// MessageType 消息类型枚举
type MessageType int32

const (
	MessageType_MESSAGE_TYPE_UNSPECIFIED      MessageType = 0
	MessageType_MESSAGE_TYPE_TRADE            MessageType = 1
	MessageType_MESSAGE_TYPE_MARKET_CREATED   MessageType = 2
	MessageType_MESSAGE_TYPE_MARKET_RESOLVED  MessageType = 3
	MessageType_MESSAGE_TYPE_POSITION_CHANGED MessageType = 4
)

func (x MessageType) String() string {
	switch x {
	case MessageType_MESSAGE_TYPE_TRADE:
		return "TRADE"
	case MessageType_MESSAGE_TYPE_MARKET_CREATED:
		return "MARKET_CREATED"
	case MessageType_MESSAGE_TYPE_MARKET_RESOLVED:
		return "MARKET_RESOLVED"
	case MessageType_MESSAGE_TYPE_POSITION_CHANGED:
		return "POSITION_CHANGED"
	default:
		return "UNSPECIFIED"
	}
}

// TradeDirection 交易方向
type TradeDirection int32

const (
	TradeDirection_TRADE_DIRECTION_UNSPECIFIED TradeDirection = 0
	TradeDirection_TRADE_DIRECTION_BUY         TradeDirection = 1
	TradeDirection_TRADE_DIRECTION_SELL        TradeDirection = 2
)

func (x TradeDirection) String() string {
	switch x {
	case TradeDirection_TRADE_DIRECTION_BUY:
		return "BUY"
	case TradeDirection_TRADE_DIRECTION_SELL:
		return "SELL"
	default:
		return "UNSPECIFIED"
	}
}

// IsBuy 是否买入
func (d TradeDirection) IsBuy() bool {
	return d == TradeDirection_TRADE_DIRECTION_BUY
}

// IsSell 是否卖出
func (d TradeDirection) IsSell() bool {
	return d == TradeDirection_TRADE_DIRECTION_SELL
}

// TradeDirectionFromString 从字符串转换
func TradeDirectionFromString(s string) TradeDirection {
	switch s {
	case "BUY":
		return TradeDirection_TRADE_DIRECTION_BUY
	case "SELL":
		return TradeDirection_TRADE_DIRECTION_SELL
	default:
		return TradeDirection_TRADE_DIRECTION_UNSPECIFIED
	}
}

// EventType CTF 事件类型
type EventType int32

const (
	EventType_EVENT_TYPE_UNSPECIFIED       EventType = 0
	EventType_EVENT_TYPE_TRANSFER_SINGLE   EventType = 1
	EventType_EVENT_TYPE_POSITION_SPLIT    EventType = 2
	EventType_EVENT_TYPE_POSITIONS_MERGE   EventType = 3
	EventType_EVENT_TYPE_PAYOUT_REDEMPTION EventType = 4
)

func (x EventType) String() string {
	switch x {
	case EventType_EVENT_TYPE_TRANSFER_SINGLE:
		return "TRANSFER_SINGLE"
	case EventType_EVENT_TYPE_POSITION_SPLIT:
		return "POSITION_SPLIT"
	case EventType_EVENT_TYPE_POSITIONS_MERGE:
		return "POSITIONS_MERGE"
	case EventType_EVENT_TYPE_PAYOUT_REDEMPTION:
		return "PAYOUT_REDEMPTION"
	default:
		return "UNSPECIFIED"
	}
}

// Envelope 消息信封
type Envelope struct {
	Type      MessageType `json:"type"`
	Timestamp int64       `json:"timestamp"`
	Source    string      `json:"source"`
	Payload   []byte      `json:"payload"`
}

func (x *Envelope) GetType() MessageType {
	if x != nil {
		return x.Type
	}
	return MessageType_MESSAGE_TYPE_UNSPECIFIED
}

func (x *Envelope) GetTimestamp() int64 {
	if x != nil {
		return x.Timestamp
	}
	return 0
}

func (x *Envelope) GetSource() string {
	if x != nil {
		return x.Source
	}
	return ""
}

func (x *Envelope) GetPayload() []byte {
	if x != nil {
		return x.Payload
	}
	return nil
}

func (x *Envelope) Reset() {
	*x = Envelope{}
}

func (*Envelope) ProtoMessage() {}
