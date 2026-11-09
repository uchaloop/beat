package recovery_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/uchaloop/beat/job"
	"github.com/uchaloop/beat/job/middleware/recovery"
)

func TestMiddleware_TurnsAPanicIntoAResult(t *testing.T) {
	runner, err := job.MakeRunner(
		job.Config{},
		func(context.Context) (int64, error) { panic("boom") },
		job.WithMiddleware(recovery.MakeMiddleware()),
	)
	if err != nil {
		t.Fatalf("MakeRunner: %v", err)
	}

	result := runner.Run(context.Background())

	if result.Outcome != job.OutcomePanic {
		t.Errorf("Outcome = %q, want %q", result.Outcome, job.OutcomePanic)
	}

	panicErr, isOK := errors.AsType[*job.PanicError](result.Err)
	if !isOK {
		t.Fatalf("Err = %v, want a *job.PanicError", result.Err)
	}
	if panicErr.Value != "boom" {
		t.Errorf("Value = %v, want boom", panicErr.Value)
	}
	// The stack is captured where the panic happened, which is the only place
	// it is still complete.
	if !strings.Contains(string(panicErr.Stack), "recovery_test") {
		t.Error("the stack does not reach the panicking function")
	}
}

func TestMiddleware_LogsWhenAsked(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	runner, err := job.MakeRunner(
		job.Config{},
		func(context.Context) (int64, error) { panic("boom") },
		job.WithMiddleware(recovery.MakeMiddleware(recovery.WithLogger(logger))),
	)
	if err != nil {
		t.Fatalf("MakeRunner: %v", err)
	}

	runner.Run(context.Background())

	if !strings.Contains(buf.String(), "panic recovered") {
		t.Errorf("nothing was logged: %q", buf.String())
	}
}

func TestMiddleware_LetsOrdinaryResultsThrough(t *testing.T) {
	runner, err := job.MakeRunner(
		job.Config{},
		func(context.Context) (int64, error) { return 5, nil },
		job.WithMiddleware(recovery.MakeMiddleware(), nil),
	)
	if err != nil {
		t.Fatalf("MakeRunner: %v", err)
	}

	if result := runner.Run(context.Background()); result.Processed != 5 || result.Outcome != job.OutcomeOK {
		t.Errorf("result = %+v, want 5 processed and ok", result)
	}
}

func TestPanicDiagnosticsDoNotExposeValue(t *testing.T) {
	const token = "synthetic-private-token"
	var output bytes.Buffer
	runner, err := job.MakeRunner(
		job.Config{},
		func(context.Context) (int64, error) { panic(token) },
		job.WithMiddleware(recovery.MakeMiddleware(recovery.WithLogger(slog.New(slog.NewJSONHandler(&output, nil))))),
	)
	if err != nil {
		t.Fatal(err)
	}

	result := runner.Run(context.Background())
	if strings.Contains(result.Err.Error(), token) || strings.Contains(output.String(), token) || strings.Contains(output.String(), "stack") {
		t.Fatal("sensitive panic details in automatic diagnostics")
	}

	panicErr, isOK := errors.AsType[*job.PanicError](result.Err)
	if !isOK || panicErr.Value != token || len(panicErr.Stack) == 0 || result.Outcome != job.OutcomePanic {
		t.Fatal("explicit panic inspection lost")
	}
}
