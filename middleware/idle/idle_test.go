package idle_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/uchaloop/beat/middleware/idle"
)

func TestMiddleware_WaitsOnError_NotOnSuccess(t *testing.T) {
	mw := idle.Middleware(func(err error) time.Duration {
		if err != nil {
			return 40 * time.Millisecond
		}

		return 0
	})

	// Error path pauses and passes the result through.
	start := time.Now()
	processed, err := mw(func(context.Context) (int, error) {
		return 3, errors.New("e")
	})(context.Background())
	elapsed := time.Since(start)

	if processed != 3 || err == nil {
		t.Fatalf("got (%d, %v), want (3, e)", processed, err)
	}
	if elapsed < 30*time.Millisecond {
		t.Fatalf("did not pause on error: %v", elapsed)
	}

	// Success path does not pause.
	start = time.Now()
	_, _ = mw(func(context.Context) (int, error) {
		return 1, nil
	})(context.Background())
	if elapsed := time.Since(start); elapsed > 20*time.Millisecond {
		t.Fatalf("paused on success: %v", elapsed)
	}
}

func TestMiddleware_NilStrategy_NoPause(t *testing.T) {
	start := time.Now()
	processed, err := idle.Middleware(nil)(func(context.Context) (int, error) {
		return 2, nil
	})(context.Background())

	if processed != 2 || err != nil {
		t.Fatalf("got (%d, %v), want (2, nil)", processed, err)
	}
	if elapsed := time.Since(start); elapsed > 20*time.Millisecond {
		t.Fatalf("nil strategy paused: %v", elapsed)
	}
}

func TestMiddleware_ContextCancelEndsPause(t *testing.T) {
	mw := idle.Middleware(func(error) time.Duration { return time.Hour })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	_, _ = mw(func(context.Context) (int, error) {
		return 0, nil
	})(ctx)

	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("pause ignored context cancellation: %v", elapsed)
	}
}
