package listener

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"db_listener/internal/config"

	"github.com/segmentio/kafka-go"
)

type emailPayload struct {
	ID int64 `json:"id"`
}

var (
	writer     *kafka.Writer
	writerOnce sync.Once

	seenMu sync.Mutex
	seen   = map[string]time.Time{} // key=id -> until
)

func ExtractIDAsKey(raw []byte) string {
	var p emailPayload
	if err := json.Unmarshal(raw, &p); err == nil && p.ID > 0 {
		return strconv.FormatInt(p.ID, 10)
	}
	return "email_json"
}

// простая защёлка на 10 секунд, чтобы отстрелить мгновенные дубли вызова в один процесс
func RecentlySent(key string) bool {
	now := time.Now()
	seenMu.Lock()
	defer seenMu.Unlock()

	if t, ok := seen[key]; ok && now.Before(t) {
		return true
	}
	seen[key] = now.Add(10 * time.Second)

	// периодическая уборка
	if len(seen) > 10000 {
		for k, t := range seen {
			if now.After(t) {
				delete(seen, k)
			}
		}
	}
	return false
}

func SendToKafka(ctx context.Context, cfg *config.Config, value []byte) error {
	writerOnce.Do(func() {
		writer = kafka.NewWriter(kafka.WriterConfig{
			Brokers:  cfg.Kafka.Brokers,
			Topic:    cfg.Kafka.Topic,
			Balancer: &kafka.Hash{},
		})
	})

	key := ExtractIDAsKey(value)
	if RecentlySent(key) {
		return nil
	}

	if err := writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: value,
	}); err != nil {
		return fmt.Errorf("failed to send data in kafka: %w", err)
	}
	return nil
}
