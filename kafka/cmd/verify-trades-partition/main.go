package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	polyKafka "github.com/fuibox/poly-common/kafka"
	"github.com/fuibox/poly-common/pb"
	kafkago "github.com/segmentio/kafka-go"
)

func main() {
	var (
		brokersCSV  = flag.String("brokers", "localhost:9092", "comma-separated brokers")
		topic       = flag.String("topic", "poly.trades", "topic name")
		partitions  = flag.Int("partitions", 128, "topic partitions (for local verification)")
		replicas    = flag.Int("replicas", 1, "topic replication factor (for local verification)")
		n           = flag.Int("n", 20, "number of trade messages to produce+verify")
		readTimeout = flag.Duration("read-timeout", 15*time.Second, "how long to wait for reading N produced messages")
	)
	flag.Parse()

	brokers := splitCSV(*brokersCSV)
	if len(brokers) == 0 {
		log.Fatal("no brokers provided")
	}

	runID := fmt.Sprintf("verify-%d-%d", time.Now().Unix(), rand.New(rand.NewSource(time.Now().UnixNano())).Intn(1_000_000))
	tokenPrefix := runID + "-"

	ctx := context.Background()

	// Best-effort topic creation for local verification.
	if err := ensureTopic(ctx, brokers[0], *topic, *partitions, *replicas); err != nil {
		log.Printf("[warn] ensureTopic failed (continuing): %v", err)
	}

	cfg := polyKafka.DefaultConfig()
	cfg.Brokers = brokers
	cfg.Topics.Trades = *topic

	producer, err := polyKafka.NewProducer(cfg)
	if err != nil {
		log.Fatalf("NewProducer error: %v", err)
	}
	defer func() { _ = producer.Close() }()

	log.Printf("run_id=%s topic=%s brokers=%v n=%d", runID, *topic, brokers, *n)
	log.Printf("producing trades (TokenId prefix=%q)...", tokenPrefix)

	for i := 0; i < *n; i++ {
		ev := &pb.TradeEvent{
			TxHash:      fmt.Sprintf("0x%s-%d", runID, i),
			BlockNumber: 12345,
			LogIndex:    uint32(i),
			TokenId:     fmt.Sprintf("%s%d", tokenPrefix, i),
			ConditionId: "cond",
			MarketId:    "mkt",
			Outcome:     "yes",
		}
		if err := producer.ProduceTrade(ctx, ev); err != nil {
			log.Fatalf("ProduceTrade error: %v", err)
		}
	}

	// Read back and print partitions.
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     brokers,
		Topic:       *topic,
		GroupID:     "verify-group-" + runID,
		StartOffset: kafkago.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     500 * time.Millisecond,
	})
	defer func() { _ = reader.Close() }()

	log.Printf("reading back produced trades and printing msg.Partition...")
	deadline := time.Now().Add(*readTimeout)

	seen := 0
	partitionsSeen := map[int]struct{}{}
	for seen < *n && time.Now().Before(deadline) {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Fatalf("ReadMessage error: %v", err)
		}

		var env pb.Envelope
		if err := json.Unmarshal(msg.Value, &env); err != nil {
			continue
		}
		if env.Type != pb.MessageType_MESSAGE_TYPE_TRADE {
			continue
		}
		var ev pb.TradeEvent
		if err := json.Unmarshal(env.Payload, &ev); err != nil {
			continue
		}
		if !strings.HasPrefix(ev.TokenId, tokenPrefix) {
			continue
		}

		partitionsSeen[msg.Partition] = struct{}{}
		seen++
		log.Printf("i=%02d partition=%d offset=%d token_id=%s", seen-1, msg.Partition, msg.Offset, ev.TokenId)
	}

	if seen < *n {
		log.Fatalf("timeout: only saw %d/%d produced messages within %s", seen, *n, *readTimeout)
	}

	log.Printf("unique_partitions=%d partitions=%v", len(partitionsSeen), keys(partitionsSeen))
	if len(partitionsSeen) != 1 {
		log.Fatalf("FAIL: expected all messages in 1 partition, got %d partitions", len(partitionsSeen))
	}
	log.Printf("PASS: all %d messages landed in the same partition", *n)
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func keys(m map[int]struct{}) []int {
	out := make([]int, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func ensureTopic(ctx context.Context, brokerAddr, topic string, partitions, replicas int) error {
	conn, err := kafkago.DialContext(ctx, "tcp", brokerAddr)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	controller, err := conn.Controller()
	if err != nil {
		return err
	}

	ctrlConn, err := kafkago.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return err
	}
	defer func() { _ = ctrlConn.Close() }()

	err = ctrlConn.CreateTopics(kafkago.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: replicas,
	})
	if err != nil {
		// tolerate "already exists" and similar
		if strings.Contains(strings.ToLower(err.Error()), "already exists") {
			return nil
		}
		return err
	}
	return nil
}

