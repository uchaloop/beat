package beat

import (
	"context"
	"slices"
)

// Job is the unit of work beat runs on each tick. It reports how many items it
// processed - return 0 when the job is not item-oriented - and an error if the
// run failed. A Job must respect ctx: beat bounds it with the job timeout and,
// unless graceful stop is enabled, cancels it on shutdown.
type Job func(ctx context.Context) (int, error)

// Middleware wraps a Job to add behaviour around it - recovery, context
// enrichment, extra logging. It is classic Go composition: a Middleware
// receives the next Job and returns a Job that calls it.
//
// A Middleware does not decide when the next run happens: pausing inside the
// Job would spend the job timeout and land in Record.Duration. Use WithBackoff
// for that.
type Middleware func(next Job) Job

// chain applies mws around job so that the first middleware passed to
// WithMiddleware is the outermost layer and therefore runs first. Nil entries
// are skipped.
func chain(job Job, mws []Middleware) Job {
	for _, mw := range slices.Backward(mws) {
		if mw != nil {
			job = mw(job)
		}
	}

	return job
}
