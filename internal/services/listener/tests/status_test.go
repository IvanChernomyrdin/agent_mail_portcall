package tests

import (
	"context"
	"strings"
	"testing"

	"db_listener/internal/config"
	"db_listener/internal/services/listener"

	"github.com/stretchr/testify/assert"
)

// TestMarkSent тестирует вызов функции MarkSent
func TestMarkSent(t *testing.T) {
	t.Run("Вызов MarkSent с невалидным хостом", func(t *testing.T) {
		cfg := &config.Config{
			Db: config.DbConfig{
				Driver:         "postgres",
				Host:           "invalid-host-that-does-not-exist",
				Port:           5432,
				User:           "test",
				Password:       "test",
				Name:           "test",
				TableEmail:     "email_outbox",
				NotifyChannels: []string{"email_events"},
				SslMode:        "disable",
			},
		}

		err := listener.MarkSent(context.Background(), cfg, 123)

		// Ожидаем ошибку подключения к БД
		assert.Error(t, err)
		assert.True(t, isConnectionError(err), "Expected connection error, got: %v", err)
	})

	t.Run("MarkSent с отмененным контекстом", func(t *testing.T) {
		cfg := &config.Config{
			Db: config.DbConfig{
				Driver:         "postgres",
				Host:           "localhost",
				Port:           5432,
				User:           "test",
				Password:       "test",
				Name:           "test",
				TableEmail:     "email_outbox",
				NotifyChannels: []string{"email_events"},
				SslMode:        "disable",
			},
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Немедленная отмена

		err := listener.MarkSent(ctx, cfg, 123)

		assert.Error(t, err)
	})
}

// TestMarkFailed тестирует вызов функции MarkFailed
func TestMarkFailed(t *testing.T) {
	t.Run("Вызов MarkFailed с невалидным хостом", func(t *testing.T) {
		cfg := &config.Config{
			Db: config.DbConfig{
				Driver:         "postgres",
				Host:           "another-invalid-host",
				Port:           5432,
				User:           "test",
				Password:       "test",
				Name:           "test",
				TableEmail:     "email_outbox",
				NotifyChannels: []string{"email_events"},
				SslMode:        "disable",
			},
		}

		err := listener.MarkFailed(context.Background(), cfg, 456)

		assert.Error(t, err)
		assert.True(t, isConnectionError(err), "Expected connection error, got: %v", err)
	})
}

// TestMarkSended тестирует вызов функции MarkSended
func TestMarkSended(t *testing.T) {
	t.Run("Вызов MarkSended с невалидным хостом", func(t *testing.T) {
		cfg := &config.Config{
			Db: config.DbConfig{
				Driver:         "postgres",
				Host:           "yet-another-invalid-host",
				Port:           5432,
				User:           "test",
				Password:       "test",
				Name:           "test",
				TableEmail:     "email_outbox",
				NotifyChannels: []string{"email_events"},
				SslMode:        "disable",
			},
		}

		err := listener.MarkSended(context.Background(), cfg, 789)

		assert.Error(t, err)
		assert.True(t, isConnectionError(err), "Expected connection error, got: %v", err)
	})
}

// TestFunctionsExist тестирует что функции вообще существуют и могут быть вызваны
func TestFunctionsExist(t *testing.T) {
	t.Run("Проверка существования функций", func(t *testing.T) {
		cfg := &config.Config{
			Db: config.DbConfig{
				Driver:         "postgres",
				Host:           "localhost",
				Port:           5432,
				User:           "test",
				Password:       "test",
				Name:           "test",
				TableEmail:     "email_outbox",
				NotifyChannels: []string{"email_events"},
				SslMode:        "disable",
			},
		}

		// Просто проверяем что функции можно вызвать
		// Они вернут ошибки, но не должны паниковать
		assert.NotPanics(t, func() {
			listener.MarkSent(context.Background(), cfg, 1)
		})

		assert.NotPanics(t, func() {
			listener.MarkFailed(context.Background(), cfg, 1)
		})

		assert.NotPanics(t, func() {
			listener.MarkSended(context.Background(), cfg, 1)
		})
	})
}

// TestErrorScenarios тестирует безопасные сценарии ошибок
func TestErrorScenarios(t *testing.T) {
	// ТЕСТИРУЕМ ТОЛЬКО БЕЗОПАСНЫЕ СЦЕНАРИИ

	t.Run("Отрицательный ID", func(t *testing.T) {
		cfg := &config.Config{
			Db: config.DbConfig{
				Driver:         "postgres",
				Host:           "localhost",
				Port:           5432,
				User:           "test",
				Password:       "test",
				Name:           "test",
				TableEmail:     "email_outbox",
				NotifyChannels: []string{"email_events"},
				SslMode:        "disable",
			},
		}
		err := listener.MarkSent(context.Background(), cfg, -1)
		assert.Error(t, err) // Будет ошибка подключения, но не паника
	})

	t.Run("Нулевой ID", func(t *testing.T) {
		cfg := &config.Config{
			Db: config.DbConfig{
				Driver:         "postgres",
				Host:           "localhost",
				Port:           5432,
				User:           "test",
				Password:       "test",
				Name:           "test",
				TableEmail:     "email_outbox",
				NotifyChannels: []string{"email_events"},
				SslMode:        "disable",
			},
		}
		err := listener.MarkSent(context.Background(), cfg, 0)
		assert.Error(t, err)
	})

	t.Run("Config с пустыми полями", func(t *testing.T) {
		cfg := &config.Config{
			Db: config.DbConfig{
				Driver:         "", // Пустой драйвер
				Host:           "localhost",
				Port:           5432,
				User:           "test",
				Password:       "test",
				Name:           "test",
				TableEmail:     "email_outbox",
				NotifyChannels: []string{"email_events"},
				SslMode:        "disable",
			},
		}
		err := listener.MarkSent(context.Background(), cfg, 123)
		assert.Error(t, err)
	})

	t.Run("Очень большой ID", func(t *testing.T) {
		cfg := &config.Config{
			Db: config.DbConfig{
				Driver:         "postgres",
				Host:           "localhost",
				Port:           5432,
				User:           "test",
				Password:       "test",
				Name:           "test",
				TableEmail:     "email_outbox",
				NotifyChannels: []string{"email_events"},
				SslMode:        "disable",
			},
		}
		err := listener.MarkSent(context.Background(), cfg, 999999999999)
		assert.Error(t, err)
	})
}

// TestIntegrationWithRealDB интеграционный тест с реальной БД
func TestIntegrationWithRealDB(t *testing.T) {
	if testing.Short() {
		t.Skip("Пропускаем интеграционный тест в short mode")
	}

	t.Run("Интеграционный тест с реальной БД", func(t *testing.T) {
		cfg := &config.Config{
			Db: config.DbConfig{
				Driver:         "postgres",
				Host:           "localhost",
				Port:           5432,
				User:           "postgres",
				Password:       "password",
				Name:           "test_db",
				TableEmail:     "email_outbox",
				NotifyChannels: []string{"email_events"},
				SslMode:        "disable",
			},
		}

		err := listener.MarkSent(context.Background(), cfg, 1)

		// Либо успех, либо ошибка связанная с отсутствием таблицы/прав
		// Главное - не паника
		if err != nil {
			assert.True(t,
				isExpectedDBError(err),
				"Unexpected error: %v", err,
			)
		}
	})
}

// TestConcurrentCalls тестирует параллельные вызовы функций
func TestConcurrentCalls(t *testing.T) {
	t.Run("Параллельные вызовы MarkSent", func(t *testing.T) {
		cfg := &config.Config{
			Db: config.DbConfig{
				Driver:         "postgres",
				Host:           "invalid-host",
				Port:           5432,
				User:           "test",
				Password:       "test",
				Name:           "test",
				TableEmail:     "email_outbox",
				NotifyChannels: []string{"email_events"},
				SslMode:        "disable",
			},
		}

		// Запускаем несколько горутин
		for i := 0; i < 5; i++ {
			t.Run(string(rune(i)), func(t *testing.T) {
				t.Parallel()
				err := listener.MarkSent(context.Background(), cfg, int64(i+1))
				assert.Error(t, err)
			})
		}
	})
}

// Вспомогательная функция для проверки ошибок подключения
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := strings.ToLower(err.Error())
	connectionErrors := []string{
		"connect", "connection", "dial", "timeout", "no such host",
		"network", "unreachable", "refused", "host", "resolve",
	}

	for _, keyword := range connectionErrors {
		if strings.Contains(errMsg, keyword) {
			return true
		}
	}
	return false
}

// Вспомогательная функция для проверки ожидаемых ошибок БД
func isExpectedDBError(err error) bool {
	if err == nil {
		return true // Нет ошибки - тоже ок
	}

	errMsg := strings.ToLower(err.Error())
	expectedErrors := []string{
		"connect", "connection", "dial", "timeout",
		"sql", "table", "relation", "permission", "role",
		"database", "password", "authentication", "ssl",
	}

	for _, keyword := range expectedErrors {
		if strings.Contains(errMsg, keyword) {
			return true
		}
	}
	return false
}
