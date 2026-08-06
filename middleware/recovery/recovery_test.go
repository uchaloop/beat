package recovery_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/uchaloop/beat"
	"github.com/uchaloop/beat/middleware/recovery"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestMiddleware_Default_PanicBecomesPanicError(t *testing.T) {
	job := recovery.Middleware()(func(context.Context) (int, error) {
		panic("boom")
	})

	processed, err := job(context.Background())
	if processed != 0 {
		t.Fatalf("processed = %d, want 0", processed)
	}

	var pe *beat.PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("err = %T, want *beat.PanicError", err)
	}
	if pe.Value != "boom" {
		t.Errorf("panic value = %v, want boom", pe.Value)
	}
	if len(pe.Stack) == 0 {
		t.Error("PanicError.Stack is empty")
	}
}

func TestMiddleware_NoPanic_PassesResultThrough(t *testing.T) {
	want := errors.New("x")
	job := recovery.Middleware()(func(context.Context) (int, error) {
		return 9, want
	})

	processed, err := job(context.Background())
	if processed != 9 || !errors.Is(err, want) {
		t.Fatalf("got (%d, %v), want (9, x)", processed, err)
	}
}

func TestMiddleware_WithLogger_LogsAndRecovers(t *testing.T) {
	// The logging path must run without affecting the recovered result.
	job := recovery.Middleware(recovery.WithLogger(discardLogger()))(func(context.Context) (int, error) {
		panic("boom")
	})

	var pe *beat.PanicError
	if _, err := job(context.Background()); !errors.As(err, &pe) {
		t.Fatalf("err = %v, want *beat.PanicError", err)
	}
}
