package beat_test

import (
	"context"
	"testing"

	"github.com/uchaloop/beat"
)

func TestMakeMultiHandler_FansOutInOrderSkippingNil(t *testing.T) {
	var order []string

	makeHandler := func(name string) beat.Handler {
		return beat.HandlerFunc(func(context.Context, beat.Record) {
			order = append(order, name)
		})
	}

	h := beat.MakeMultiHandler(makeHandler("a"), nil, makeHandler("b"))
	h.Handle(context.Background(), beat.Record{})

	if len(order) != 2 || order[0] != "a" || order[1] != "b" {
		t.Fatalf("order = %v, want [a b]", order)
	}
}

func TestMakeMultiHandler_Empty(t *testing.T) {
	// Must not panic with no handlers.
	beat.MakeMultiHandler().Handle(context.Background(), beat.Record{})
}

func TestMakeMultiHandler_CopiesInputSlice(t *testing.T) {
	var got string
	handlers := []beat.Handler{beat.HandlerFunc(func(context.Context, beat.Record) {
		got = "original"
	})}
	handler := beat.MakeMultiHandler(handlers...)
	handlers[0] = beat.HandlerFunc(func(context.Context, beat.Record) {
		got = "replacement"
	})
	handler.Handle(context.Background(), beat.Record{})
	if got != "original" {
		t.Fatalf("handler = %q, want original", got)
	}
}
