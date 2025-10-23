// main_test.go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"testing"

	"db_listener/internal/config"
	"db_listener/internal/domain/notify"
	"db_listener/internal/services/listener"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Mock для Notifier (интерфейс из listener)
type MockNotifier struct {
	mock.Mock
}

func (m *MockNotifier) Listen(ctx context.Context, channels ...string) (<-chan notify.Notification, <-chan error, error) {
	args := m.Called(ctx, channels)

	var msgChan <-chan notify.Notification
	if args.Get(0) != nil {
		msgChan = args.Get(0).(<-chan notify.Notification)
	}

	var errChan <-chan error
	if args.Get(1) != nil {
		errChan = args.Get(1).(<-chan error)
	}

	return msgChan, errChan, args.Error(2)
}

// Глобальные переменные для подмены функций
var (
	configMustLoad      = config.MustLoad
	signalNotifyContext = signal.NotifyContext
	logFatalf           = log.Fatalf
)

// Упрощенная testMain
func testMain(notifier listener.Notifier) {
	cfg := configMustLoad()

	ctx, stop := signalNotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Создаем реальный сервис с нашим моком notifier
	svc := listener.New(notifier, cfg)

	if err := svc.Run(ctx, "send_email"); err != nil && err != context.Canceled {
		logFatalf("listener stopped: %v", err)
	}
}

// Проверяет нормальный запуск и завершение приложения
func TestListenerMain_GracefulShutdown(t *testing.T) {
	t.Run("Запуск и остановка listener", func(t *testing.T) {
		// Инициализируем глобальные переменные
		configMustLoad = config.MustLoad
		signalNotifyContext = signal.NotifyContext
		logFatalf = log.Fatalf

		// Сохраняем оригинальные функции
		originalConfig := configMustLoad
		originalSignal := signalNotifyContext
		originalLog := logFatalf

		defer func() {
			configMustLoad = originalConfig
			signalNotifyContext = originalSignal
			logFatalf = originalLog
		}()

		// Создаем мок notifier
		mockNotifier := &MockNotifier{}

		// Создаем валидный тестовый конфиг
		realConfig := &config.Config{
			Db: config.DbConfig{
				Driver:         "postgres",
				Host:           "localhost",
				Port:           5432,
				User:           "test",
				Password:       "test",
				Name:           "testdb",
				TableEmail:     "emails",
				NotifyChannels: []string{"send_email"},
				SslMode:        "disable",
			},
			Kafka: config.KafkaConfig{
				Host:    "localhost",
				Port:    9092,
				Topic:   "test-topic",
				GroupID: "test-group",
				Brokers: []string{"localhost:9092"},
			},
			Logger: config.LoggerConfig{
				GRPCAddress:  "localhost:50051",
				FallbackPath: "/tmp/logs",
				ServiceName:  "test-service",
			},
		}

		// Создаем СРАЗУ ОТМЕНЕННЫЙ контекст
		testCtx, cancel := context.WithCancel(context.Background())
		cancel()

		// Настраиваем ожидания для мока
		// Listen должен вернуть nil каналы и nil ошибку
		mockNotifier.On("Listen", mock.AnythingOfType("*context.cancelCtx"), []string{"send_email"}).Return(
			(<-chan notify.Notification)(nil),
			(<-chan error)(nil),
			nil,
		)

		// Подменяем функции
		configMustLoad = func() *config.Config {
			return realConfig
		}
		signalNotifyContext = func(ctx context.Context, signals ...os.Signal) (context.Context, context.CancelFunc) {
			assert.Equal(t, []os.Signal{syscall.SIGINT, syscall.SIGTERM}, signals)
			return testCtx, cancel
		}

		// log.Fatalf не должен вызываться
		logFatalfCalled := false
		logFatalf = func(format string, v ...interface{}) {
			logFatalfCalled = true
			t.Errorf("log.Fatalf был вызван: %s", format)
		}

		// ВЫПОЛНЯЕМ ТЕСТ с передачей мока notifier
		testMain(mockNotifier)

		// ПРОВЕРЯЕМ РЕЗУЛЬТАТЫ
		mockNotifier.AssertCalled(t, "Listen", mock.AnythingOfType("*context.cancelCtx"), []string{"send_email"})
		assert.False(t, logFatalfCalled, "log.Fatalf не должен вызываться")
	})
}
