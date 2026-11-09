package beat_test

import (
	"context"
	"fmt"
	"time"

	"github.com/uchaloop/beat"
	"github.com/uchaloop/beat/job"
)

// ExampleMakeBeat_queue keeps batch claiming in the application. A PostgreSQL
// implementation would own SQL, transactions and acknowledgement here.
func ExampleMakeBeat_queue() {
	batches := []int64{3, 0}
	runner, err := job.MakeRunner(job.Config{}, func(ctx context.Context) (int64, error) {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		count := batches[0]
		batches = batches[1:]
		return count, nil
	})
	if err != nil {
		panic(err)
	}
	records := make(chan beat.Record, 2)
	scheduler, err := beat.MakeBeat(
		beat.Config{Period: time.Millisecond},
		runner,
		beat.WithMode(beat.ModeFixedDelay),
		beat.WithHandler(beat.HandlerFunc(func(_ context.Context, record beat.Record) {
			records <- record
		})),
		beat.WithBackoff(func(record beat.Record) time.Duration {
			if record.Result.Processed == 0 {
				return time.Hour
			}
			return 0
		}),
	)
	if err != nil {
		panic(err)
	}
	if err := scheduler.Start(context.Background()); err != nil {
		panic(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := scheduler.Stop(ctx); err != nil {
			panic(err)
		}
	}()
	for range 2 {
		select {
		case record := <-records:
			fmt.Println("processed:", record.Result.Processed)
		case <-time.After(5 * time.Second):
			panic("attempt did not finish")
		}
	}
	// Output:
	// processed: 3
	// processed: 0
}
