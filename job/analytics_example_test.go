package job_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/uchaloop/beat/job"
)

// ExampleWithErrorHandler_analytics uses a deterministic destination failure.
// The application owns export delivery, checkpoints and failure persistence.
func ExampleWithErrorHandler_analytics() {
	var isFailureRecorded bool
	runner, err := job.MakeRunner(
		job.Config{Timeout: time.Minute, ErrorHandlerTimeout: 5 * time.Second},
		func(context.Context) (int64, error) {
			return 3, errors.New("destination unavailable")
		},
		job.WithErrorHandler(func(ctx context.Context, _ error) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			isFailureRecorded = true
			return nil
		}),
	)
	if err != nil {
		panic(err)
	}
	result := runner.Run(context.Background())
	observer := job.HandlerFunc(func(_ context.Context, result job.Result) {
		fmt.Println("outcome:", result.Outcome)
		fmt.Println("processed:", result.Processed)
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	observer.Handle(ctx, result)
	fmt.Println("failure recorded:", isFailureRecorded)
	fmt.Println("error handler called:", result.IsErrorHandlerCalled)
	fmt.Println("error handler failed:", result.ErrorHandlerErr != nil)
	// Output:
	// outcome: error
	// processed: 3
	// failure recorded: true
	// error handler called: true
	// error handler failed: false
}
