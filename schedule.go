package beat

import (
	"context"
	crand "crypto/rand"
	"math/big"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

// parserOptions matches the classic crontab plus an optional seconds field and
// descriptors (@every, @daily, ...).
const parserOptions = cron.SecondOptional | cron.Minute | cron.Hour |
	cron.Dom | cron.Month | cron.Dow | cron.Descriptor

// parseSchedule parses a beat spec: an interval (for example "@every 5s") or a
// cron expression (for example "*/5 * * * * *").
func parseSchedule(spec string) (cron.Schedule, error) {
	return cron.NewParser(parserOptions).Parse(spec)
}

// isEverySpec reports whether spec is an "@every" interval. Only "@every " (the
// trailing space is required, matching robfig/cron) runs on the interval loop;
// the descriptors "@daily", "@hourly" and friends are genuine cron schedules and
// run on the cron loop.
func isEverySpec(spec string) bool {
	return strings.HasPrefix(strings.TrimSpace(spec), "@every ")
}

// Wait blocks for d or until ctx is done, whichever comes first. A non-positive
// d returns immediately. It is exported for middleware authors that need a
// cancellable delay.
func Wait(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}

	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// randomDelay returns a uniformly distributed delay in [0, max) drawn from a
// cryptographically secure source. A non-positive max yields 0.
func randomDelay(max time.Duration) (time.Duration, error) {
	if max <= 0 {
		return 0, nil
	}

	n, err := crand.Int(crand.Reader, big.NewInt(int64(max)))
	if err != nil {
		return 0, err
	}

	return time.Duration(n.Int64()), nil
}
