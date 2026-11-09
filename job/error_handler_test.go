package job

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestErrorHandler_IndependentBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		type key struct{}
		ctx := context.WithValue(context.Background(), key{}, "value")
		calls := 0
		runner := mustRunner(t, Config{Timeout: time.Second, ErrorHandlerTimeout: 3 * time.Second},
			func(ctx context.Context) (int64, error) { <-ctx.Done(); return 7, ctx.Err() },
			WithErrorHandler(func(ctx context.Context, err error) error {
				calls++
				if ctx.Err() != nil || ctx.Value(key{}) != "value" {
					t.Fatal("handler inherited cancellation or lost values")
				}
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal(err)
				}
				time.Sleep(2 * time.Second)
				return nil
			}))
		result := runner.Run(ctx)
		if calls != 1 || result.Processed != 7 || result.Outcome != OutcomeTimeout || result.ErrorHandlerErr != nil {
			t.Fatalf("calls=%d result=%+v", calls, result)
		}
		if result.WorkDuration != time.Second || result.ErrorHandlerDuration != 2*time.Second || result.Duration != 3*time.Second {
			t.Fatalf("durations: %+v", result)
		}
	})
}

func TestErrorHandler_FailureDoesNotReplaceWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		workErr, deliveryErr := errors.New("work"), errors.New("delivery")
		calls := 0
		r := mustRunner(t, Config{Timeout: time.Second, ErrorHandlerTimeout: 2 * time.Second},
			func(context.Context) (int64, error) { return 2, workErr },
			WithErrorHandler(func(ctx context.Context, err error) error { calls++; <-ctx.Done(); return deliveryErr }))
		result := r.Run(context.Background())
		if result.Err != workErr || result.Outcome != OutcomeError || calls != 1 {
			t.Fatalf("%+v calls=%d", result, calls)
		}
		if !errors.Is(result.ErrorHandlerErr, deliveryErr) || !errors.Is(result.ErrorHandlerErr, context.DeadlineExceeded) {
			t.Fatal(result.ErrorHandlerErr)
		}
		if result.Duration != 2*time.Second {
			t.Fatal(result.Duration)
		}
	})
}

func TestErrorHandler_ReportsSwallowedDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := mustRunner(t, Config{ErrorHandlerTimeout: time.Second}, func(context.Context) (int64, error) { return 0, errors.New("work") },
			WithErrorHandler(func(ctx context.Context, _ error) error { <-ctx.Done(); return nil }))
		result := r.Run(context.Background())
		if !errors.Is(result.ErrorHandlerErr, context.DeadlineExceeded) || result.Outcome != OutcomeError {
			t.Fatalf("%+v", result)
		}
	})
}

func TestErrorHandler_NotCalledWithoutReturnedError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		r := mustRunner(t, Config{Timeout: time.Second}, func(context.Context) (int64, error) { time.Sleep(2 * time.Second); return 0, nil },
			WithErrorHandler(func(context.Context, error) error { calls++; return nil }))
		result := r.Run(context.Background())
		if calls != 0 || result.IsErrorHandlerCalled || result.Outcome != OutcomeTimeout {
			t.Fatalf("calls=%d result=%+v", calls, result)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		result = r.Run(ctx)
		if calls != 0 || result.IsErrorHandlerCalled || !result.StartedAt.IsZero() {
			t.Fatalf("skipped result=%+v calls=%d", result, calls)
		}
	})
}

func TestErrorHandler_ParentCancellationDoesNotCancelDelivery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := mustRunner(t, Config{}, func(context.Context) (int64, error) { cancel(); return 0, context.Canceled },
		WithErrorHandler(func(ctx context.Context, _ error) error {
			if ctx.Err() != nil {
				t.Fatal(ctx.Err())
			}
			return nil
		}))
	result := r.Run(ctx)
	if result.Outcome != OutcomeCanceled || result.ErrorHandlerErr != nil {
		t.Fatalf("%+v", result)
	}
}

func TestErrorHandler_Configuration(t *testing.T) {
	if _, err := MakeRunner(Config{ErrorHandlerTimeout: -time.Second}, func(context.Context) (int64, error) { return 0, nil }); err == nil {
		t.Fatal("accepted negative timeout")
	}
	r := mustRunner(t, Config{}, func(context.Context) (int64, error) { return 0, errors.New("work") }, WithErrorHandler(func(context.Context, error) error { t.Fatal("disabled handler ran"); return nil }), WithErrorHandler(nil))
	if r.errorHandlerTimeout != time.Minute {
		t.Fatal(r.errorHandlerTimeout)
	}
	if result := r.Run(context.Background()); result.ErrorHandlerErr != nil || result.ErrorHandlerDuration != 0 {
		t.Fatalf("%+v", result)
	}
}

func TestErrorHandler_CalledEvenWithZeroDuration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		work := func(context.Context) (int64, error) { return 0, errors.New("work") }
		r := mustRunner(t, Config{}, work, WithErrorHandler(func(context.Context, error) error { return nil }))
		result := r.Run(context.Background())
		if !result.IsErrorHandlerCalled || result.ErrorHandlerDuration != 0 || result.ErrorHandlerErr != nil {
			t.Fatalf("instant callback: %+v", result)
		}
		r = mustRunner(t, Config{}, work)
		if result := r.Run(context.Background()); result.IsErrorHandlerCalled {
			t.Fatal("absent callback marked called")
		}
	})
}
