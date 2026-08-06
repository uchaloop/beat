package beat_test

import (
	"context"
	"testing"

	"github.com/uchaloop/beat"
)

func TestMultiHandler_FansOutInOrderSkippingNil(t *testing.T) {
	var order []string

	mk := func(name string) beat.Handler {
		return beat.HandlerFunc(func(context.Context, beat.Record) {
			order = append(order, name)
		})
	}

	h := beat.MultiHandler(mk("a"), nil, mk("b"))
	h.Handle(context.Background(), beat.Record{})

	if len(order) != 2 || order[0] != "a" || order[1] != "b" {
		t.Fatalf("order = %v, want [a b]", order)
	}
}

func TestMultiHandler_Empty(t *testing.T) {
	// Must not panic with no handlers.
	beat.MultiHandler().Handle(context.Background(), beat.Record{})
}
