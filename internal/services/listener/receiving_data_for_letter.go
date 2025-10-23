package listener

import (
	"context"
	"db_listener/internal/config"
	"fmt"

	"github.com/jackc/pgx/v5"
)

type DB interface {
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
	Close(ctx context.Context) error
}

// DBConnector интерфейс для создания соединения
type DBConnector interface {
	Connect(ctx context.Context, connString string) (DB, error)
}

// Реализация DBConnector для pgx
type PgxConnector struct{}

func (p *PgxConnector) Connect(ctx context.Context, connString string) (DB, error) {
	return pgx.Connect(ctx, connString)
}

func GetMessage(id string, cfg *config.Config, connector DBConnector) ([]byte, error) {
	dsn, err := cfg.Db.DSN()
	if err != nil {
		return nil, err
	}

	conn, err := connector.Connect(context.Background(), dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close(context.Background())

	query := `
        UPDATE "email_outbox" AS e
		SET "status" = 'enqueued', "updated_at" = now()
		WHERE e."id" = $1 AND e."status" = 'pending'
		RETURNING row_to_json((
			SELECT t FROM (
				SELECT e."id", e."to_address", e."subject", e."body_html",
						encode(e."zip_bytes",'base64') AS zip_bytes,
						encode(e."zip_sha256",'hex')  AS zip_sha256,
						e."profile"
			) t
		));
    `

	var jsonData []byte
	err = conn.QueryRow(context.Background(), query, id).Scan(&jsonData)
	if err != nil {
		return nil, fmt.Errorf("failed to get query: %w", err)
	}

	return jsonData, nil
}
