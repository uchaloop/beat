package beat

import "context"

// Job is the unit of work beat runs on each tick. It reports how many items it
// processed - return 0 when the job is not item-oriented - and an error if the
// run failed. A Job must respect ctx: beat bounds it with the job timeout and,
// unless graceful stop is enabled, cancels it on shutdown.
type Job func(ctx context.Context) (int, error)

// Middleware wraps a Job to add behaviour around it - recovery, backoff,
// context enrichment, extra logging. It is classic Go composition: a Middleware
// receives the next Job and returns a Job that calls it.
type Middleware func(next Job) Job

// chain applies mws around job so that the first middleware passed to
// WithMiddleware is the outermost layer and therefore runs first. Nil entries
// are skipped.
func chain(job Job, mws []Middleware) Job {
	for i := len(mws) - 1; i >= 0; i-- {
		if mws[i] != nil {
			job = mws[i](job)
		}
	}

	return job
}
