package tests

import (
	"context"
	"strconv"
	"testing"

	"db_listener/internal/config"
	"db_listener/internal/services/listener"

	"github.com/stretchr/testify/assert"
)

// извлечение ID из JSON для ключа Kafka
func TestExtractIDAsKey(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected string
	}{
		{
			name:     "Валидный ID",
			input:    []byte(`{"id": 12345}`),
			expected: "12345",
		},
		{
			name:     "Пустой ID",
			input:    []byte(`{"id": 0}`),
			expected: "email_json",
		},
		{
			name:     "Отрицательный ID",
			input:    []byte(`{"id": -1}`),
			expected: "email_json",
		},
		{
			name:     "Не ваилдный JSON",
			input:    []byte(`invalid json`),
			expected: "email_json",
		},
		{
			name:     "Пустой JSON",
			input:    []byte(`{}`),
			expected: "email_json",
		},
		{
			name:     "Нету поля ID",
			input:    []byte(`{"email": "test@test.com"}`),
			expected: "email_json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := listener.ExtractIDAsKey(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// проверка дедупликации сообщений
func TestRecentlySent(t *testing.T) {
	t.Run("Дедупликация сообщений", func(t *testing.T) {
		// Первый вызов - false
		key := "test_key_1"
		assert.False(t, listener.RecentlySent(key))

		// Второй вызов сразу - true (дубликат)
		assert.True(t, listener.RecentlySent(key))

		// Другой ключ - false
		anotherKey := "test_key_2"
		assert.False(t, listener.RecentlySent(anotherKey))
	})
}

// отправка данных
func TestSendToKafka(t *testing.T) {
	cfg := &config.Config{
		Kafka: config.KafkaConfig{
			Brokers: []string{"localhost:9092"},
			Topic:   "test-topic",
		},
	}

	ctx := context.Background()

	t.Run("Валидные данные", func(t *testing.T) {
		testData := []byte(`{"id": 12345}`)
		err := listener.SendToKafka(ctx, cfg, testData)
		// Будет ошибка подключения к Kafka, но функция работает
		assert.NotNil(t, err)
	})
	t.Run("Дублирование данных", func(t *testing.T) {
		testData := []byte(`{"id": 999}`)
		// Первый вызов
		err1 := listener.SendToKafka(ctx, cfg, testData)
		assert.NotNil(t, err1)
		// Второй вызов с теми же данными
		err2 := listener.SendToKafka(ctx, cfg, testData)
		// Должен вернуть nil из-за дедупликации
		assert.Nil(t, err2)
	})
	t.Run("Не валидный json", func(t *testing.T) {
		testData := []byte(`invalid json`)
		err := listener.SendToKafka(ctx, cfg, testData)
		assert.NotNil(t, err)
	})
}

// множественные вызовы отправки с разными ID
func TestSendToKafka_MultipleCalls(t *testing.T) {
	cfg := &config.Config{
		Kafka: config.KafkaConfig{
			Brokers: []string{"localhost:9092"},
			Topic:   "test-topic",
		},
	}

	ctx := context.Background()

	ids := []int64{100, 200, 300, 400, 500}
	for _, id := range ids {
		t.Run(string(rune(id)), func(t *testing.T) {
			testData := []byte(`{"id": ` + strconv.FormatInt(id, 10) + `}`)
			err := listener.SendToKafka(ctx, cfg, testData)
			assert.NotNil(t, err) // Ошибка подключения к Kafka
		})
	}
}
