package job

import (
	"context"
	"slices"
	"testing"
)

func noop(context.Context) (int64, error) { return 0, nil }

func TestChain_FirstMiddlewareIsOutermost(t *testing.T) {
	var order []string

	mw := func(name string) Middleware {
		return func(next Func) Func {
			return func(ctx context.Context) (int64, error) {
				order = append(order, name)

				return next(ctx)
			}
		}
	}

	fn := chain(
		func(context.Context) (int64, error) {
			order = append(order, "work")

			return 0, nil
		},
		[]Middleware{mw("a"), mw("b"), mw("c")},
	)

	if _, err := fn(context.Background()); err != nil {
		t.Fatalf("chain: %v", err)
	}

	if want := []string{"a", "b", "c", "work"}; !slices.Equal(order, want) {
		t.Errorf("order = %v, want %v", order, want)
	}
}

func TestChain_SkipsNil(t *testing.T) {
	isCalled := false
	fn := chain(
		func(context.Context) (int64, error) { isCalled = true; return 0, nil },
		[]Middleware{nil, nil},
	)

	if _, err := fn(context.Background()); err != nil {
		t.Fatalf("chain: %v", err)
	}
	if !isCalled {
		t.Fatal("the work was not isCalled")
	}
}
