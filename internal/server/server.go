package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"ghidorah/internal/events"
)

const shutdownTimeout = 5 * time.Second

// Run starts Ghidorah's HTTP delivery surface and blocks until the server
// fails or ctx is cancelled.
func Run(ctx context.Context, addr string, bus <-chan events.ClusterEvent) error {
	stream := &streamHandler{
		ctx: ctx,
		bus: bus,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/stream", stream.serveHTTP)

	httpServer := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Printf("http server listening on %s", addr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}

		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()

		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown http server: %w", err)
		}

		if err := <-errCh; err != nil {
			return fmt.Errorf("run http server: %w", err)
		}

		return nil
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("run http server: %w", err)
		}

		return nil
	}
}

type streamHandler struct {
	ctx context.Context
	bus <-chan events.ClusterEvent
}

func (h *streamHandler) serveHTTP(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	header := w.Header()
	header.Set("Content-Type", "text/event-stream")
	header.Set("Cache-Control", "no-cache")
	header.Set("Connection", "keep-alive")
	header.Set("Access-Control-Allow-Origin", "*")

	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	for {
		select {
		case <-h.ctx.Done():
			return
		case <-r.Context().Done():
			return
		case event, ok := <-h.bus:
			if !ok {
				return
			}

			payload, err := json.Marshal(event)
			if err != nil {
				log.Printf("marshal cluster event: %v", err)
				continue
			}

			if _, err := fmt.Fprintf(w, "data: %s\n\n", payload); err != nil {
				return
			}

			flusher.Flush()
		}
	}
}
