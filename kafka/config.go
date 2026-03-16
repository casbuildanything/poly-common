package kafka

import (
	"time"
)

// Config Kafka 配置
type Config struct {
	Brokers       []string       `yaml:"brokers" json:"brokers"`
	ConsumerGroup string         `yaml:"consumer_group" json:"consumer_group"`
	Topics        TopicsConfig   `yaml:"topics" json:"topics"`
	Producer      ProducerConfig `yaml:"producer" json:"producer"`
	Consumer      ConsumerConfig `yaml:"consumer" json:"consumer"`
}

// TopicsConfig Topic 配置
type TopicsConfig struct {
	Trades  string `yaml:"trades" json:"trades"`
	Markets string `yaml:"markets" json:"markets"`
}

// ProducerConfig 生产者配置
type ProducerConfig struct {
	BatchSize    int           `yaml:"batch_size" json:"batch_size"`
	BatchTimeout time.Duration `yaml:"batch_timeout" json:"batch_timeout"`
	Async        bool          `yaml:"async" json:"async"`
	RequiredAcks int           `yaml:"required_acks" json:"required_acks"` // 0: none, 1: leader, -1: all
}

// ConsumerConfig 消费者配置
type ConsumerConfig struct {
	MinBytes       int           `yaml:"min_bytes" json:"min_bytes"`
	MaxBytes       int           `yaml:"max_bytes" json:"max_bytes"`
	MaxWait        time.Duration `yaml:"max_wait" json:"max_wait"`
	CommitInterval time.Duration `yaml:"commit_interval" json:"commit_interval"`
	StartOffset    int64         `yaml:"start_offset" json:"start_offset"` // -1: latest, -2: earliest
}

// DefaultConfig 返回默认配置
func DefaultConfig() *Config {
	return &Config{
		Brokers:       []string{"localhost:9092"},
		ConsumerGroup: "poly-analytics",
		Topics: TopicsConfig{
			Trades:  "poly.trades",
			Markets: "poly.markets",
		},
		Producer: ProducerConfig{
			BatchSize:    100,
			BatchTimeout: time.Second,
			Async:        true,
			RequiredAcks: 1,
		},
		Consumer: ConsumerConfig{
			MinBytes:       10e3, // 10KB
			MaxBytes:       10e6, // 10MB
			MaxWait:        time.Second,
			CommitInterval: time.Second,
			StartOffset:    -1, // latest
		},
	}
}

// Validate 验证配置
func (c *Config) Validate() error {
	if len(c.Brokers) == 0 {
		return ErrNoBrokers
	}
	if c.Topics.Trades == "" && c.Topics.Markets == "" {
		return ErrNoTopics
	}
	return nil
}
