package kafka

import "errors"

var (
	ErrNoBrokers      = errors.New("kafka: no brokers configured")
	ErrNoTopics       = errors.New("kafka: no topics configured")
	ErrProducerClosed = errors.New("kafka: producer is closed")
	ErrConsumerClosed = errors.New("kafka: consumer is closed")
	ErrSerialize      = errors.New("kafka: failed to serialize message")
	ErrDeserialize    = errors.New("kafka: failed to deserialize message")
)
