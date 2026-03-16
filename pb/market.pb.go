package pb

// MarketCreatedEvent 市场创建事件
type MarketCreatedEvent struct {
	// 链上信息
	TxHash         string `json:"tx_hash"`
	BlockNumber    uint64 `json:"block_number"`
	LogIndex       uint32 `json:"log_index"`
	BlockTimestamp int64  `json:"block_timestamp"`

	// 市场信息
	ConditionId      string `json:"condition_id"`
	Oracle           string `json:"oracle"`
	QuestionId       string `json:"question_id"`
	OutcomeSlotCount uint32 `json:"outcome_slot_count"`
}

func (x *MarketCreatedEvent) GetTxHash() string {
	if x != nil {
		return x.TxHash
	}
	return ""
}

func (x *MarketCreatedEvent) GetBlockNumber() uint64 {
	if x != nil {
		return x.BlockNumber
	}
	return 0
}

func (x *MarketCreatedEvent) GetLogIndex() uint32 {
	if x != nil {
		return x.LogIndex
	}
	return 0
}

func (x *MarketCreatedEvent) GetBlockTimestamp() int64 {
	if x != nil {
		return x.BlockTimestamp
	}
	return 0
}

func (x *MarketCreatedEvent) GetConditionId() string {
	if x != nil {
		return x.ConditionId
	}
	return ""
}

func (x *MarketCreatedEvent) GetOracle() string {
	if x != nil {
		return x.Oracle
	}
	return ""
}

func (x *MarketCreatedEvent) GetQuestionId() string {
	if x != nil {
		return x.QuestionId
	}
	return ""
}

func (x *MarketCreatedEvent) GetOutcomeSlotCount() uint32 {
	if x != nil {
		return x.OutcomeSlotCount
	}
	return 0
}

func (x *MarketCreatedEvent) Reset() {
	*x = MarketCreatedEvent{}
}

func (*MarketCreatedEvent) ProtoMessage() {}

// MarketResolvedEvent 市场解决事件
type MarketResolvedEvent struct {
	TxHash           string   `json:"tx_hash"`
	BlockNumber      uint64   `json:"block_number"`
	BlockTimestamp   int64    `json:"block_timestamp"`
	ConditionId      string   `json:"condition_id"`
	Oracle           string   `json:"oracle"`
	QuestionId       string   `json:"question_id"`
	PayoutNumerators []uint32 `json:"payout_numerators"`
}

func (x *MarketResolvedEvent) GetTxHash() string {
	if x != nil {
		return x.TxHash
	}
	return ""
}

func (x *MarketResolvedEvent) GetBlockNumber() uint64 {
	if x != nil {
		return x.BlockNumber
	}
	return 0
}

func (x *MarketResolvedEvent) GetBlockTimestamp() int64 {
	if x != nil {
		return x.BlockTimestamp
	}
	return 0
}

func (x *MarketResolvedEvent) GetConditionId() string {
	if x != nil {
		return x.ConditionId
	}
	return ""
}

func (x *MarketResolvedEvent) GetOracle() string {
	if x != nil {
		return x.Oracle
	}
	return ""
}

func (x *MarketResolvedEvent) GetQuestionId() string {
	if x != nil {
		return x.QuestionId
	}
	return ""
}

func (x *MarketResolvedEvent) GetPayoutNumerators() []uint32 {
	if x != nil {
		return x.PayoutNumerators
	}
	return nil
}

func (x *MarketResolvedEvent) Reset() {
	*x = MarketResolvedEvent{}
}

func (*MarketResolvedEvent) ProtoMessage() {}

// MarketBatch 批量市场事件
type MarketBatch struct {
	Created   []*MarketCreatedEvent  `json:"created"`
	Resolved  []*MarketResolvedEvent `json:"resolved"`
	Timestamp int64                  `json:"timestamp"`
}

func (x *MarketBatch) GetCreated() []*MarketCreatedEvent {
	if x != nil {
		return x.Created
	}
	return nil
}

func (x *MarketBatch) GetResolved() []*MarketResolvedEvent {
	if x != nil {
		return x.Resolved
	}
	return nil
}

func (x *MarketBatch) GetTimestamp() int64 {
	if x != nil {
		return x.Timestamp
	}
	return 0
}

func (x *MarketBatch) Reset() {
	*x = MarketBatch{}
}

func (*MarketBatch) ProtoMessage() {}
