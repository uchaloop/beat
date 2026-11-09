package job_test

import (
	"context"
	"errors"
	"testing"

	"github.com/uchaloop/beat/job"
)

func TestCompositionOwnsSlices(t *testing.T) {
	middleware := []job.Middleware{func(job.Func) job.Func {
		return func(context.Context) (int64, error) { return 1, nil }
	}}
	option := job.WithMiddleware(middleware...)
	middleware[0] = func(job.Func) job.Func {
		return func(context.Context) (int64, error) { return 2, nil }
	}
	runner, err := job.MakeRunner(job.Config{}, func(context.Context) (int64, error) { return 0, nil }, option)
	if err != nil {
		t.Fatal(err)
	}

	if result := runner.Run(context.Background()); result.Processed != 1 {
		t.Fatal("caller changed middleware composition")
	}

	var calls int
	handlers := []job.Handler{job.HandlerFunc(func(context.Context, job.Result) { calls++ })}
	handler := job.MakeMultiHandler(handlers...)
	handlers[0] = job.HandlerFunc(func(context.Context, job.Result) { t.Error("replacement handler called") })
	handler.Handle(context.Background(), job.Result{})
	if calls != 1 {
		t.Fatal("caller changed handler composition")
	}
}

func TestInvalidRunArguments(t *testing.T) {
	runner, err := job.MakeRunner(job.Config{}, func(context.Context) (int64, error) {
		t.Error("invalid call executed work")

		return 0, nil
	}, job.WithErrorHandler(func(context.Context, error) error {
		t.Error("invalid call executed callback")

		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}

	result := runner.Run(nil)
	if !errors.Is(result.Err, job.ErrContextIsNil) || result.Outcome != job.OutcomeError || !result.StartedAt.IsZero() {
		t.Fatalf("unexpected result: %+v", result)
	}

	for _, invalid := range []*job.Runner{nil, {}} {
		result := invalid.Run(context.Background())
		if !errors.Is(result.Err, job.ErrRunnerIsInvalid) || result.Outcome != job.OutcomeError || result.IsErrorHandlerCalled {
			t.Fatalf("unexpected result: %+v", result)
		}
	}
}

func TestNilMiddlewareResult(t *testing.T) {
	var isOuterCalled bool
	runner, err := job.MakeRunner(
		job.Config{},
		func(context.Context) (int64, error) { return 0, nil },
		job.WithMiddleware(
			func(next job.Func) job.Func {
				isOuterCalled = true

				return next
			},
			func(job.Func) job.Func { return nil },
		),
	)
	if runner != nil || !errors.Is(err, job.ErrMiddlewareFuncIsNil) || isOuterCalled {
		t.Fatalf("runner=%v err=%v outer=%v", runner, err, isOuterCalled)
	}

	if _, err := job.MakeRunner(job.Config{}, nil); !errors.Is(err, job.ErrFuncIsNil) {
		t.Fatal(err)
	}
}
