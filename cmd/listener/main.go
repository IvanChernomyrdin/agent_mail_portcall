package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"db_listener/internal/adapters/pgnotify"
	"db_listener/internal/config"
	"db_listener/internal/services/listener"
)

func main() {
	cfg := config.MustLoad()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	dsn, err := cfg.Db.DSN()
	if err != nil {
		log.Fatalf("dsn build error: %v", err)
	}

	n := pgnotify.New(dsn)
	if err := n.Connect(ctx); err != nil {
		log.Fatalf("pg connect error: %v", err)
	}
	defer n.Close()

	svc := listener.New(n, cfg)

	log.Printf("portcall agent started")
	if err := svc.Run(ctx, "send_email"); err != nil && err != context.Canceled {
		log.Fatalf("listener stopped: %v", err)
	}
}
