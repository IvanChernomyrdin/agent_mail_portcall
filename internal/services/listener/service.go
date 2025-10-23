package listener

import (
	"context"
	"db_listener/internal/config"
	"db_listener/internal/domain/notify"
	"log"
	"strings"
	"sync"
	"time"
)

type Notifier interface {
	Listen(ctx context.Context, channels ...string) (<-chan notify.Notification, <-chan error, error)
}

type Service struct {
	notifier Notifier
	cfg      *config.Config

	mu       sync.Mutex
	inFlight map[string]struct{}
}

func New(n Notifier, cfg *config.Config) *Service {
	return &Service{
		notifier: n,
		cfg:      cfg,
		inFlight: make(map[string]struct{})}
}

func (s *Service) LockID(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.inFlight[id]; ok {
		return false
	}
	s.inFlight[id] = struct{}{}
	return true
}

func (s *Service) UnlockID(id string) {
	s.mu.Lock()
	delete(s.inFlight, id)
	s.mu.Unlock()
}

// Run запускает прослушивание и отправляет полезную нагрузку в Kafka.
func (s *Service) Run(ctx context.Context, channels ...string) error {
	msgs, errs, err := s.notifier.Listen(ctx, channels...)
	if err != nil {
		return err
	}
	bufferedMsgs := make(chan notify.Notification, 100)

	// Проксирование сообщений в буферизованный канал
	go func() {
		defer close(bufferedMsgs)
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-msgs:
				if !ok {
					return
				}
				bufferedMsgs <- msg
			}
		}
	}()

	// Обработка ошибок без буфера
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case e, ok := <-errs:
				if !ok {
					return
				}
				if e != nil {
					log.Printf("listener error: %v", e)
				}
			}
		}
	}()

	// Основной цикл только для сообщений
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case m, ok := <-bufferedMsgs:
			if !ok {
				return nil
			}

			id := strings.TrimSpace(m.Payload)
			if id == "" {
				continue
			}

			if !s.LockID(id) {
				continue
			}
			go func(id string) {
				defer s.UnlockID(id)

				payload, err := GetMessage(id, s.cfg, &PgxConnector{})
				if err != nil {
					log.Printf("GetMessage(%s) err: %v", id, err)
					return
				}

				sendCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				_ = SendToKafka(sendCtx, s.cfg, payload)
				cancel()
			}(id)
		}
	}
}
