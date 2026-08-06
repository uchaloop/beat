package batch_test

import (
	"context"
	"testing"
	"time"

	"github.com/uchaloop/beat/middleware/batch"
)

func TestMiddleware_PauseFromProcessed(t *testing.T) {
	// Empty batch (processed == 0) backs off; a full batch drains immediately.
	mw := batch.Middleware(func(processed int, err error) time.Duration {
		if processed == 0 {
			return 40 * time.Millisecond
		}

		return 0
	})

	start := time.Now()
	_, _ = mw(func(context.Context) (int, error) {
		return 0, nil
	})(context.Background())
	if elapsed := time.Since(start); elapsed < 30*time.Millisecond {
		t.Fatalf("did not back off on empty batch: %v", elapsed)
	}

	start = time.Now()
	processed, err := mw(func(context.Context) (int, error) {
		return 5, nil
	})(context.Background())
	if processed != 5 || err != nil {
		t.Fatalf("got (%d, %v), want (5, nil)", processed, err)
	}
	if elapsed := time.Since(start); elapsed > 20*time.Millisecond {
		t.Fatalf("paused on a full batch: %v", elapsed)
	}
}

func TestMiddleware_NilStrategy_NoPause(t *testing.T) {
	start := time.Now()
	processed, err := batch.Middleware(nil)(func(context.Context) (int, error) {
		return 7, nil
	})(context.Background())

	if processed != 7 || err != nil {
		t.Fatalf("got (%d, %v), want (7, nil)", processed, err)
	}
	if elapsed := time.Since(start); elapsed > 20*time.Millisecond {
		t.Fatalf("nil strategy paused: %v", elapsed)
	}
}
