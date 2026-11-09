package job_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/uchaloop/beat/job"
)

func ExampleWithErrorHandler() {
	failure := errors.New("batch rejected")
	handled := false
	runner, err := job.MakeRunner(
		job.Config{Timeout: time.Minute, ErrorHandlerTimeout: 10 * time.Second},
		func(context.Context) (int64, error) { return 3, failure },
		job.WithErrorHandler(func(ctx context.Context, err error) error {
			// Replace with application-owned DLQ persistence or additional logging.
			handled = errors.Is(err, failure)
			return ctx.Err()
		}),
	)
	if err != nil {
		panic(err)
	}
	result := runner.Run(context.Background())
	fmt.Println(result.Processed, result.Outcome, handled, result.ErrorHandlerErr)
	// Output: 3 error true <nil>
}
