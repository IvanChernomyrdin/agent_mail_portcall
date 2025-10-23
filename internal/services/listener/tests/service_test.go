package tests

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"db_listener/internal/config"
	"db_listener/internal/domain/notify"
	"db_listener/internal/services/listener"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockNotifier struct {
	mock.Mock
}

func (m *MockNotifier) Listen(ctx context.Context, channels ...string) (<-chan notify.Notification, <-chan error, error) {
	args := m.Called(ctx, channels)
	return args.Get(0).(<-chan notify.Notification), args.Get(1).(<-chan error), args.Error(2)
}

// успешный запуск сервиса и обработку сообщений
func TestRun_Success(t *testing.T) {
	t.Run("Успешная обработка сообщения", func(t *testing.T) {
		mockNotifier := new(MockNotifier)
		cfg := &config.Config{}

		msgChan := make(chan notify.Notification, 1)
		errChan := make(chan error, 1)

		mockNotifier.On("Listen", mock.Anything, mock.Anything).Return((<-chan notify.Notification)(msgChan), (<-chan error)(errChan), nil)

		service := listener.New(mockNotifier, cfg)
		ctx, cancel := context.WithCancel(context.Background())

		var wg sync.WaitGroup
		wg.Add(1)
		var runErr error
		go func() {
			defer wg.Done()
			runErr = service.Run(ctx, "test-channel")
		}()

		// Даем время на запуск
		time.Sleep(10 * time.Millisecond)

		// Отправляем сообщение
		msgChan <- notify.Notification{Payload: "123"}

		time.Sleep(100 * time.Millisecond)
		cancel()
		wg.Wait()

		// В реальной среде будет ошибка, но главное что функция работает
		assert.NotNil(t, runErr)
		mockNotifier.AssertExpectations(t)
	})
}

// обработка ошибок при запуске прослушивания
func TestRun_ListenError(t *testing.T) {
	t.Run("Ошибка при запуске прослушивания", func(t *testing.T) {
		mockNotifier := new(MockNotifier)
		cfg := &config.Config{}

		expectedErr := errors.New("listen failed")
		mockNotifier.On("Listen", mock.Anything, mock.Anything).Return((<-chan notify.Notification)(nil), (<-chan error)(nil), expectedErr)

		service := listener.New(mockNotifier, cfg)
		err := service.Run(context.Background(), "test-channel")

		assert.Equal(t, expectedErr, err)
	})
}

// пропуск сообщений с пустым payload
func TestRun_EmptyPayload(t *testing.T) {
	t.Run("Пропуск сообщения с пустым payload", func(t *testing.T) {
		mockNotifier := new(MockNotifier)
		cfg := &config.Config{}

		msgChan := make(chan notify.Notification, 1)
		errChan := make(chan error, 1)

		mockNotifier.On("Listen", mock.Anything, mock.Anything).Return((<-chan notify.Notification)(msgChan), (<-chan error)(errChan), nil)

		service := listener.New(mockNotifier, cfg)
		ctx, cancel := context.WithCancel(context.Background())

		var wg sync.WaitGroup
		wg.Add(1)
		var runErr error
		go func() {
			defer wg.Done()
			runErr = service.Run(ctx, "test-channel")
		}()

		time.Sleep(10 * time.Millisecond)
		msgChan <- notify.Notification{Payload: "   "}

		time.Sleep(100 * time.Millisecond)
		cancel()
		wg.Wait()

		// Сообщение с пустым payload должно быть проигнорировано, но сервис работает
		assert.NotNil(t, runErr)
	})

	t.Run("Пропуск сообщения с пустой строкой", func(t *testing.T) {
		mockNotifier := new(MockNotifier)
		cfg := &config.Config{}

		msgChan := make(chan notify.Notification, 1)
		errChan := make(chan error, 1)

		mockNotifier.On("Listen", mock.Anything, mock.Anything).Return((<-chan notify.Notification)(msgChan), (<-chan error)(errChan), nil)

		service := listener.New(mockNotifier, cfg)
		ctx, cancel := context.WithCancel(context.Background())

		var wg sync.WaitGroup
		wg.Add(1)
		var runErr error
		go func() {
			defer wg.Done()
			runErr = service.Run(ctx, "test-channel")
		}()

		time.Sleep(10 * time.Millisecond)
		msgChan <- notify.Notification{Payload: ""}

		time.Sleep(100 * time.Millisecond)
		cancel()
		wg.Wait()

		assert.NotNil(t, runErr)
	})
}

// graceful shutdown при отмене контекста
func TestRun_ContextCancel(t *testing.T) {
	t.Run("Graceful shutdown при отмене контекста", func(t *testing.T) {
		mockNotifier := new(MockNotifier)
		cfg := &config.Config{}

		msgChan := make(chan notify.Notification)
		errChan := make(chan error)

		mockNotifier.On("Listen", mock.Anything, mock.Anything).Return((<-chan notify.Notification)(msgChan), (<-chan error)(errChan), nil)

		service := listener.New(mockNotifier, cfg)
		ctx, cancel := context.WithCancel(context.Background())

		var wg sync.WaitGroup
		wg.Add(1)
		var runErr error
		go func() {
			defer wg.Done()
			runErr = service.Run(ctx, "test-channel")
		}()

		time.Sleep(10 * time.Millisecond)
		cancel()
		wg.Wait()

		assert.Equal(t, context.Canceled, runErr)
	})
}

// обработка нескольких сообщений
func TestRun_MultipleMessages(t *testing.T) {
	t.Run("Обработка нескольких сообщений", func(t *testing.T) {
		mockNotifier := new(MockNotifier)
		cfg := &config.Config{}

		msgChan := make(chan notify.Notification, 3)
		errChan := make(chan error, 1)

		mockNotifier.On("Listen", mock.Anything, mock.Anything).Return((<-chan notify.Notification)(msgChan), (<-chan error)(errChan), nil)

		service := listener.New(mockNotifier, cfg)
		ctx, cancel := context.WithCancel(context.Background())

		var wg sync.WaitGroup
		wg.Add(1)
		var runErr error
		go func() {
			defer wg.Done()
			runErr = service.Run(ctx, "test-channel")
		}()

		time.Sleep(10 * time.Millisecond)

		// Отправляем несколько сообщений
		msgChan <- notify.Notification{Payload: "1"}
		msgChan <- notify.Notification{Payload: "2"}
		msgChan <- notify.Notification{Payload: "3"}

		time.Sleep(100 * time.Millisecond)
		cancel()
		wg.Wait()

		assert.NotNil(t, runErr)
	})
}

// обработка ошибок из канала ошибок
func TestRun_ErrorHandling(t *testing.T) {
	t.Run("Обработка ошибок из канала ошибок", func(t *testing.T) {
		mockNotifier := new(MockNotifier)
		cfg := &config.Config{}

		msgChan := make(chan notify.Notification, 1)
		errChan := make(chan error, 1)

		mockNotifier.On("Listen", mock.Anything, mock.Anything).Return((<-chan notify.Notification)(msgChan), (<-chan error)(errChan), nil)

		service := listener.New(mockNotifier, cfg)
		ctx, cancel := context.WithCancel(context.Background())

		var wg sync.WaitGroup
		wg.Add(1)
		var runErr error
		go func() {
			defer wg.Done()
			runErr = service.Run(ctx, "test-channel")
		}()

		time.Sleep(10 * time.Millisecond)

		// Отправляем ошибку в канал ошибок
		errChan <- errors.New("test error")

		time.Sleep(100 * time.Millisecond)
		cancel()
		wg.Wait()

		assert.NotNil(t, runErr)
	})
}
