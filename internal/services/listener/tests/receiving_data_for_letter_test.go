package tests

import (
	"context"
	"errors"
	"testing"

	"db_listener/internal/config"
	"db_listener/internal/services/listener"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Мок для DBConnector
type MockDBConnector struct {
	mock.Mock
}

func (m *MockDBConnector) Connect(ctx context.Context, connString string) (listener.DB, error) {
	args := m.Called(ctx, connString)
	// Если есть ошибка - возвращаем nil для DB
	if args.Error(1) != nil {
		return nil, args.Error(1)
	}
	// Если нет ошибки - возвращаем DB
	return args.Get(0).(listener.DB), nil
}

// Мок для DB
type MockDB struct {
	mock.Mock
}

func (m *MockDB) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	callArgs := m.Called(ctx, sql, args)
	return callArgs.Get(0).(pgx.Row)
}

func (m *MockDB) Close(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// Мок для pgx.Row
type MockPgxRow struct {
	mock.Mock
}

func (m *MockPgxRow) Scan(dest ...interface{}) error {
	args := m.Called(dest[0])
	return args.Error(0)
}

func TestGetMessageWithDB_AllScenarios(t *testing.T) {
	// успешное получение сообщения
	t.Run("Success", func(t *testing.T) {
		realConfig := &config.Config{
			Db: config.DbConfig{
				Driver:         "postgres",
				Host:           "localhost",
				Port:           5432,
				User:           "test",
				Password:       "test",
				Name:           "testdb",
				TableEmail:     "emails",
				NotifyChannels: []string{"test"},
				SslMode:        "disable",
			},
		}

		mockConnector := &MockDBConnector{}
		mockDB := &MockDB{}
		mockRow := &MockPgxRow{}

		// Настройка моков
		mockConnector.On("Connect", mock.Anything, mock.Anything).Return(mockDB, nil)
		mockDB.On("Close", mock.Anything).Return(nil)
		mockRow.On("Scan", mock.Anything).Run(func(args mock.Arguments) {
			jsonData := args.Get(0).(*[]byte)
			*jsonData = []byte(`{"id": "123", "to_address": "test@example.com"}`)
		}).Return(nil)
		mockDB.On("QueryRow", mock.Anything, mock.Anything, mock.Anything).Return(mockRow)

		result, err := listener.GetMessage("123", realConfig, mockConnector)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Contains(t, string(result), "test@example.com")
		mockDB.AssertCalled(t, "Close", mock.Anything)
	})
	// когда сообщение не найдено:
	t.Run("MessageNotFound", func(t *testing.T) {
		realConfig := &config.Config{
			Db: config.DbConfig{
				Driver:         "postgres",
				Host:           "localhost",
				Port:           5432,
				User:           "test",
				Password:       "test",
				Name:           "testdb",
				TableEmail:     "emails",
				NotifyChannels: []string{"test"},
				SslMode:        "disable",
			},
		}

		mockConnector := &MockDBConnector{}
		mockDB := &MockDB{}
		mockRow := &MockPgxRow{}

		mockConnector.On("Connect", mock.Anything, mock.Anything).Return(mockDB, nil)
		mockDB.On("Close", mock.Anything).Return(nil)
		mockRow.On("Scan", mock.Anything).Return(pgx.ErrNoRows) // Сообщение не найдено!
		mockDB.On("QueryRow", mock.Anything, mock.Anything, mock.Anything).Return(mockRow)

		result, err := listener.GetMessage("999", realConfig, mockConnector)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to get query")
	})
	// ошибка подключения к БД
	t.Run("ConnectionError", func(t *testing.T) {
		realConfig := &config.Config{
			Db: config.DbConfig{
				Driver:         "postgres",
				Host:           "localhost",
				Port:           5432,
				User:           "test",
				Password:       "test",
				Name:           "testdb",
				TableEmail:     "emails",
				NotifyChannels: []string{"test"},
				SslMode:        "disable",
			},
		}

		mockConnector := &MockDBConnector{}
		mockConnector.On("Connect", mock.Anything, mock.Anything).Return(nil, errors.New("connection failed"))

		result, err := listener.GetMessage("123", realConfig, mockConnector)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to connect")
	})
	// невалидный DSN
	t.Run("InvalidDSN", func(t *testing.T) {
		realConfig := &config.Config{
			Db: config.DbConfig{
				Driver: "invalid",
			},
		}

		mockConnector := &MockDBConnector{}

		result, err := listener.GetMessage("123", realConfig, mockConnector)

		assert.Error(t, err)
		assert.Nil(t, result)
	})
}
