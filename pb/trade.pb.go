package pb

// TradeEvent 交易事件
type TradeEvent struct {
	// 链上信息
	TxHash         string `json:"tx_hash"`
	BlockNumber    uint64 `json:"block_number"`
	LogIndex       uint32 `json:"log_index"`
	BlockTimestamp int64  `json:"block_timestamp"`

	// 交易信息
	User        string         `json:"user"`
	Direction   TradeDirection `json:"direction"`
	TokenId     string         `json:"token_id"`
	TokenAmount string         `json:"token_amount"`
	UsdcAmount  string         `json:"usdc_amount"`
	Price       string         `json:"price"`

	// 市场信息
	ConditionId string `json:"condition_id"`
	MarketId    string `json:"market_id"`
	Outcome     string `json:"outcome"`

	// 事件类型
	EventType EventType `json:"event_type"`

	// 订单哈希
	OrderHash string `json:"order_hash,omitempty"`
}

func (x *TradeEvent) GetTxHash() string {
	if x != nil {
		return x.TxHash
	}
	return ""
}

func (x *TradeEvent) GetBlockNumber() uint64 {
	if x != nil {
		return x.BlockNumber
	}
	return 0
}

func (x *TradeEvent) GetLogIndex() uint32 {
	if x != nil {
		return x.LogIndex
	}
	return 0
}

func (x *TradeEvent) GetBlockTimestamp() int64 {
	if x != nil {
		return x.BlockTimestamp
	}
	return 0
}

func (x *TradeEvent) GetUser() string {
	if x != nil {
		return x.User
	}
	return ""
}

func (x *TradeEvent) GetDirection() TradeDirection {
	if x != nil {
		return x.Direction
	}
	return TradeDirection_TRADE_DIRECTION_UNSPECIFIED
}

func (x *TradeEvent) GetTokenId() string {
	if x != nil {
		return x.TokenId
	}
	return ""
}

func (x *TradeEvent) GetTokenAmount() string {
	if x != nil {
		return x.TokenAmount
	}
	return ""
}

func (x *TradeEvent) GetUsdcAmount() string {
	if x != nil {
		return x.UsdcAmount
	}
	return ""
}

func (x *TradeEvent) GetPrice() string {
	if x != nil {
		return x.Price
	}
	return ""
}

func (x *TradeEvent) GetConditionId() string {
	if x != nil {
		return x.ConditionId
	}
	return ""
}

func (x *TradeEvent) GetMarketId() string {
	if x != nil {
		return x.MarketId
	}
	return ""
}

func (x *TradeEvent) GetOutcome() string {
	if x != nil {
		return x.Outcome
	}
	return ""
}

func (x *TradeEvent) GetEventType() EventType {
	if x != nil {
		return x.EventType
	}
	return EventType_EVENT_TYPE_UNSPECIFIED
}

func (x *TradeEvent) GetOrderHash() string {
	if x != nil {
		return x.OrderHash
	}
	return ""
}

func (x *TradeEvent) Reset() {
	*x = TradeEvent{}
}

func (*TradeEvent) ProtoMessage() {}

// TradeBatch 批量交易事件
type TradeBatch struct {
	Trades    []*TradeEvent `json:"trades"`
	Timestamp int64         `json:"timestamp"`
}

func (x *TradeBatch) GetTrades() []*TradeEvent {
	if x != nil {
		return x.Trades
	}
	return nil
}

func (x *TradeBatch) GetTimestamp() int64 {
	if x != nil {
		return x.Timestamp
	}
	return 0
}

func (x *TradeBatch) Reset() {
	*x = TradeBatch{}
}

func (*TradeBatch) ProtoMessage() {}
