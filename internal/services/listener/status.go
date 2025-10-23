package listener

import (
	"context"
	"db_listener/internal/config"
	"errors"

	"github.com/jackc/pgx/v5"
)

func updateStatus(ctx context.Context, cfg *config.Config, id int64, newStatus string) error {
	if cfg == nil {
		return errors.New("config is nil")
	}
	dsn, err := cfg.Db.DSN()
	if err != nil {
		return err
	}

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, `
		UPDATE "email_outbox"
		   SET "status" = $2, "updated_at" = now()
		 WHERE "id" = $1
	`, id, newStatus)
	return err
}

func MarkSent(ctx context.Context, cfg *config.Config, id int64) error {
	return updateStatus(ctx, cfg, id, "sented")
}

func MarkFailed(ctx context.Context, cfg *config.Config, id int64) error {
	return updateStatus(ctx, cfg, id, "failed")
}

func MarkSended(ctx context.Context, cfg *config.Config, id int64) error {
	return MarkSent(ctx, cfg, id)
}
