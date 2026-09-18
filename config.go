package beat

import (
	"time"

	"github.com/uchaloop/validate"
)

// Config is the configuration beat declares. The env tags are inert strings: an
// application loads them with github.com/uchaloop/confmaker and provides the
// filled Config into the container. beat itself never reads the environment.
type Config struct {
	// Period is how often the Job runs. Under ModeFixedRate it is the spacing
	// of the grid the runs sit on; under ModeFixedDelay it is the pause between
	// the end of one run and the start of the next. The deployment has to
	// supply it: there is no period that makes sense for every job.
	Period time.Duration `env:"PERIOD,notEmpty"`

	// JobTimeout cancels a run's context after this duration. Zero selects one
	// minute. Cancellation is cooperative: Job must return and join its own
	// goroutines. A longer timeout can allow a run to cover multiple grid points.
	JobTimeout time.Duration `env:"JOB_TIMEOUT"`

	// Jitter is the upper bound of the random offset that staggers replicas so
	// they do not all fire at once. The draw is half-open, [0, Jitter), and
	// happens once when the Beat is built, so the offset holds for the life of
	// the process. Zero disables it; WithOffset replaces the draw with a value
	// the application chooses.
	//
	// It may be as large as Period - spreading replicas over the whole period is
	// what an evenly polling deployment wants. Work tied to a boundary is the
	// exception: a wide jitter delays it by that much.
	//
	// It spreads load. It is not a guard against two replicas doing the same
	// work: runs longer than the offset overlap regardless.
	Jitter time.Duration `env:"JITTER"`
}

// SetDefaults sets JobTimeout to one minute. Configuration loaders call it
// before applying input. MakeBeat also defaults a zero JobTimeout.
func (c *Config) SetDefaults() {
	c.JobTimeout = defaultJobTimeout
}

// ConfigName is the default instance name, "beat": a loader such as confmaker
// reads BEAT_PERIOD and the rest of BEAT_* unless the application names the
// instance itself.
func (Config) ConfigName() string { return "beat" }

// Validate reports whether the Config is usable. confmaker calls it after
// filling the struct, and it reports every problem at once rather than the
// first: a deployment is fixed in a config map and rolled out, so one report is
// one round trip.
func (c Config) Validate() error {
	var errs validate.Errors

	errs.Require(c.Period > 0, "period must be > 0")
	errs.Require(c.JobTimeout >= 0, "job_timeout must be >= 0")
	errs.Require(c.Jitter >= 0, "jitter must be >= 0")

	// Jitter bounds a half-open draw, so a jitter equal to the period still
	// yields offsets inside it - and spreading replicas over the whole period is
	// exactly what a poller wants. Only a jitter past the period could put a run
	// on the next point instead of its own. A concrete offset is stricter; see
	// WithOffset.
	if c.Period > 0 && c.Jitter > c.Period {
		errs.Addf("jitter must be <= period (%v)", c.Period)
	}

	return errs.Err()
}
