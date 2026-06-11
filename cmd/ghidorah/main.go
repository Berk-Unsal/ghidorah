package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"ghidorah/internal/cluster"
	"ghidorah/internal/events"
	"ghidorah/internal/server"
)

func main() {
	// signal.NotifyContext gives every long-running component one shared
	// cancellation signal. Pressing Ctrl+C drains both the informer and HTTP
	// server instead of leaving watcher goroutines behind.
	signalCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	ctx, cancel := context.WithCancel(signalCtx)
	defer cancel()

	clientset, err := cluster.NewClientset()
	if err != nil {
		log.Fatalf("create kubernetes client: %v", err)
	}

	metricsClientset, err := cluster.NewMetricsClientset()
	if err != nil {
		log.Fatalf("create kubernetes metrics client: %v", err)
	}

	errCh := make(chan error, 3)

	go func() {
		errCh <- cluster.RunInformers(ctx, clientset)
	}()

	go func() {
		errCh <- cluster.RunNodeMetricsPoller(ctx, clientset, metricsClientset)
	}()

	go func() {
		errCh <- server.Run(ctx, ":8042", events.EventBus)
	}()

	var runErr error
	for range 3 {
		if err := <-errCh; err != nil && runErr == nil {
			runErr = err
			cancel()
		}
	}

	if runErr != nil {
		log.Fatalf("run ghidorah: %v", runErr)
	}

	log.Println("ghidorah stopped")
}
