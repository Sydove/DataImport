package main

import (
	_ "DataImport/internal/pkg/config"
	"DataImport/internal/service/syncES"
	"context"
	"errors"
	"os/signal"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := syncES.DefaultConfig()
	if err := syncES.Run(ctx, cfg); err != nil && !errors.Is(err, context.Canceled) {
		panic(err)
	}
}
