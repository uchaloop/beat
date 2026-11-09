package job_test

import (
	"context"
	"slices"
	"testing"

	"github.com/uchaloop/beat/job"
)

func TestMakeMultiHandler_FansOutInOrderSkippingNil(t *testing.T) {
	var order []string

	makeHandler := func(name string) job.Handler {
		return job.HandlerFunc(func(context.Context, job.Result) {
			order = append(order, name)
		})
	}

	job.MakeMultiHandler(makeHandler("a"), nil, makeHandler("b")).Handle(context.Background(), job.Result{})

	if want := []string{"a", "b"}; !slices.Equal(order, want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
}

func TestMakeMultiHandler_Empty(t *testing.T) {
	// Must not panic with no handlers.
	job.MakeMultiHandler().Handle(context.Background(), job.Result{})
}

func TestHandlerFunc_NilIsNoOp(t *testing.T) {
	var h job.HandlerFunc

	h.Handle(context.Background(), job.Result{})
}
