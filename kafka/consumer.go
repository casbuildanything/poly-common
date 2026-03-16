package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/fuibox/poly-common/pb"
	"github.com/segmentio/kafka-go"
)

// MessageHandler 消息处理函数
type MessageHandler func(ctx context.Context, envelope *pb.Envelope, raw []byte) error

// TradeHandler 交易消息处理函数
type TradeHandler func(ctx context.Context, event *pb.TradeEvent) error

// MarketCreatedHandler 市场创建消息处理函数
type MarketCreatedHandler func(ctx context.Context, event *pb.MarketCreatedEvent) error

// Consumer Kafka 消费者
type Consumer struct {
	reader *kafka.Reader
	config *Config
	topic  string
	closed bool
	mu     sync.RWMutex

	// 消息处理器
	tradeHandler         TradeHandler
	marketCreatedHandler MarketCreatedHandler
	genericHandler       MessageHandler
}

// ConsumerOption 消费者选项
type ConsumerOption func(*Consumer)

// WithTradeHandler 设置交易处理器
func WithTradeHandler(h TradeHandler) ConsumerOption {
	return func(c *Consumer) {
		c.tradeHandler = h
	}
}

// WithMarketCreatedHandler 设置市场创建处理器
func WithMarketCreatedHandler(h MarketCreatedHandler) ConsumerOption {
	return func(c *Consumer) {
		c.marketCreatedHandler = h
	}
}

// WithGenericHandler 设置通用处理器
func WithGenericHandler(h MessageHandler) ConsumerOption {
	return func(c *Consumer) {
		c.genericHandler = h
	}
}

// NewConsumer 创建新的消费者
func NewConsumer(cfg *Config, topic string, opts ...ConsumerOption) (*Consumer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		GroupID:        cfg.ConsumerGroup,
		Topic:          topic,
		MinBytes:       cfg.Consumer.MinBytes,
		MaxBytes:       cfg.Consumer.MaxBytes,
		MaxWait:        cfg.Consumer.MaxWait,
		CommitInterval: cfg.Consumer.CommitInterval,
		StartOffset:    cfg.Consumer.StartOffset,
	})

	c := &Consumer{
		reader: reader,
		config: cfg,
		topic:  topic,
	}

	for _, opt := range opts {
		opt(c)
	}

	return c, nil
}

// NewTradeConsumer 创建交易消费者
func NewTradeConsumer(cfg *Config, handler TradeHandler) (*Consumer, error) {
	return NewConsumer(cfg, cfg.Topics.Trades, WithTradeHandler(handler))
}

// NewMarketConsumer 创建市场消费者
func NewMarketConsumer(cfg *Config, handler MarketCreatedHandler) (*Consumer, error) {
	return NewConsumer(cfg, cfg.Topics.Markets, WithMarketCreatedHandler(handler))
}

// Start 开始消费消息
func (c *Consumer) Start(ctx context.Context) error {
	log.Printf("[KafkaConsumer] Starting consumer for topic: %s, group: %s", c.topic, c.config.ConsumerGroup)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				log.Printf("[KafkaConsumer] Error fetching message: %v", err)
				continue
			}

			if err := c.handleMessage(ctx, msg); err != nil {
				log.Printf("[KafkaConsumer] Error handling message: %v", err)
			}

			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("[KafkaConsumer] Error committing message: %v", err)
			}
		}
	}
}

// handleMessage 处理单条消息
func (c *Consumer) handleMessage(ctx context.Context, msg kafka.Message) error {
	var envelope pb.Envelope
	if err := json.Unmarshal(msg.Value, &envelope); err != nil {
		return fmt.Errorf("%w: %v", ErrDeserialize, err)
	}

	if c.genericHandler != nil {
		if err := c.genericHandler(ctx, &envelope, msg.Value); err != nil {
			return err
		}
	}

	switch envelope.Type {
	case pb.MessageType_MESSAGE_TYPE_TRADE:
		if c.tradeHandler != nil {
			var event pb.TradeEvent
			if err := json.Unmarshal(envelope.Payload, &event); err != nil {
				return fmt.Errorf("%w: %v", ErrDeserialize, err)
			}
			return c.tradeHandler(ctx, &event)
		}

	case pb.MessageType_MESSAGE_TYPE_MARKET_CREATED:
		if c.marketCreatedHandler != nil {
			var event pb.MarketCreatedEvent
			if err := json.Unmarshal(envelope.Payload, &event); err != nil {
				return fmt.Errorf("%w: %v", ErrDeserialize, err)
			}
			return c.marketCreatedHandler(ctx, &event)
		}

	default:
		log.Printf("[KafkaConsumer] Unknown message type: %v", envelope.Type)
	}

	return nil
}

// Close 关闭消费者
func (c *Consumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true
	return c.reader.Close()
}

// Stats 返回消费者统计信息
func (c *Consumer) Stats() kafka.ReaderStats {
	return c.reader.Stats()
}

// Lag 返回消费延迟
func (c *Consumer) Lag() int64 {
	return c.reader.Lag()
}
