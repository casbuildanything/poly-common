package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/fuibox/poly-common/pb"
	"github.com/segmentio/kafka-go"
)

const tradeOrderedKey = "poly_trades_ordered"

// topicAwareBalancer routes balancing decisions by topic.
// - trades topic: use a key-based balancer (hash) so same key => same partition
// - other topics: keep existing behavior (least-bytes)
type topicAwareBalancer struct {
	tradesTopic     string
	tradesBalancer  kafka.Balancer
	defaultBalancer kafka.Balancer
}

func (b *topicAwareBalancer) Balance(msg kafka.Message, partitions ...int) int {
	if msg.Topic == b.tradesTopic {
		return b.tradesBalancer.Balance(msg, partitions...)
	}
	return b.defaultBalancer.Balance(msg, partitions...)
}

// Producer Kafka 生产者
type Producer struct {
	writer *kafka.Writer
	config *Config
	closed bool
	mu     sync.RWMutex
}

// NewProducer 创建新的生产者
func NewProducer(cfg *Config) (*Producer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Balancer: &topicAwareBalancer{
			tradesTopic:     cfg.Topics.Trades,
			tradesBalancer:  &kafka.Hash{},
			defaultBalancer: &kafka.LeastBytes{},
		},
		BatchSize:    cfg.Producer.BatchSize,
		BatchTimeout: cfg.Producer.BatchTimeout,
		Async:        cfg.Producer.Async,
		RequiredAcks: kafka.RequiredAcks(cfg.Producer.RequiredAcks),
	}

	return &Producer{
		writer: writer,
		config: cfg,
	}, nil
}

// ProduceTrade 发送交易事件
func (p *Producer) ProduceTrade(ctx context.Context, event *pb.TradeEvent) error {
	return p.produceMessage(ctx, p.config.Topics.Trades, pb.MessageType_MESSAGE_TYPE_TRADE, event, tradeOrderedKey)
}

// ProduceTradeBatch 批量发送交易事件
func (p *Producer) ProduceTradeBatch(ctx context.Context, events []*pb.TradeEvent) error {
	messages := make([]kafka.Message, 0, len(events))
	for _, event := range events {
		payload, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrSerialize, err)
		}

		envelope := &pb.Envelope{
			Type:      pb.MessageType_MESSAGE_TYPE_TRADE,
			Timestamp: time.Now().UnixMilli(),
			Source:    "listener",
			Payload:   payload,
		}

		data, err := json.Marshal(envelope)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrSerialize, err)
		}

		messages = append(messages, kafka.Message{
			Topic: p.config.Topics.Trades,
			Key:   []byte(tradeOrderedKey),
			Value: data,
		})
	}

	return p.writer.WriteMessages(ctx, messages...)
}

// ProduceMarketCreated 发送市场创建事件
func (p *Producer) ProduceMarketCreated(ctx context.Context, event *pb.MarketCreatedEvent) error {
	return p.produceMessage(ctx, p.config.Topics.Markets, pb.MessageType_MESSAGE_TYPE_MARKET_CREATED, event, event.ConditionId)
}

// ProduceMarketResolved 发送市场解决事件
func (p *Producer) ProduceMarketResolved(ctx context.Context, event *pb.MarketResolvedEvent) error {
	return p.produceMessage(ctx, p.config.Topics.Markets, pb.MessageType_MESSAGE_TYPE_MARKET_RESOLVED, event, event.ConditionId)
}

// produceMessage 通用消息发送
func (p *Producer) produceMessage(ctx context.Context, topic string, msgType pb.MessageType, msg interface{}, key string) error {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return ErrProducerClosed
	}
	p.mu.RUnlock()

	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrSerialize, err)
	}

	envelope := &pb.Envelope{
		Type:      msgType,
		Timestamp: time.Now().UnixMilli(),
		Source:    "listener",
		Payload:   payload,
	}

	data, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrSerialize, err)
	}

	return p.writer.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: data,
	})
}

// Close 关闭生产者
func (p *Producer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}

	p.closed = true
	return p.writer.Close()
}

// Stats 返回生产者统计信息
func (p *Producer) Stats() kafka.WriterStats {
	return p.writer.Stats()
}
