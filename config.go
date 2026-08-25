package beat

import (
	"errors"
	"fmt"
	"time"
)

// Config is the configuration beat declares. The env tags are inert strings: an
// application loads them with confmaker/confx and provides the filled Config
// into the container. beat itself never reads the environment.
type Config struct {
	// Spec is the schedule: an interval such as "@every 5s" or a cron
	// expression such as "*/5 * * * * *". The deployment has to supply it: there
	// is no schedule that makes sense for every job.
	Spec string `env:"SPEC,notEmpty"`

	// JobTimeout bounds the context of a single Job execution. Every execution is
	// bounded, so set a larger value for a legitimately long Job.
	JobTimeout time.Duration `env:"JOB_TIMEOUT"`

	// Jitter is the maximum random delay used to stagger instances so replicas
	// do not all fire at once. For an interval it is applied once before the
	// first run; for cron it is added to every tick, so keep it below the cron
	// interval. Zero disables it.
	Jitter time.Duration `env:"JITTER"`
}

// SetDefaults establishes the values a deployment does not have to think about.
// confmaker/confx calls it before the environment is applied, so a variable left
// unset keeps what is set here, and a generated .env.example carries the real
// default rather than a blank.
//
// A Config built in Go by hand does not go through it, which is why MakeBeat
// still treats a zero JobTimeout as the default. Both paths apply the same
// constant; neither states the value twice.
func (c *Config) SetDefaults() {
	c.JobTimeout = defaultJobTimeout
}

// Validate reports whether the Config is usable. confmaker/confx calls it after
// filling the struct, and it reports every problem at once rather than the
// first: a deployment is fixed in a config map and rolled out, so one report is
// one round trip.
//
// The Spec check overlaps the notEmpty tag on purpose. The tag speaks to a
// deployment - it names the variable and fires before anything is built - while
// this speaks to any caller, including one that builds a Config in Go and never
// goes near a loader.
func (c Config) Validate() error {
	var errs []error

	switch {
	case len(c.Spec) == 0:
		errs = append(errs, errors.New("spec is required"))
	default:
		if _, err := parseSchedule(c.Spec); err != nil {
			errs = append(errs, fmt.Errorf("invalid spec %q: %w", c.Spec, err))
		}
	}
	if c.JobTimeout < 0 {
		errs = append(errs, errors.New("job_timeout must be >= 0"))
	}
	if c.Jitter < 0 {
		errs = append(errs, errors.New("jitter must be >= 0"))
	}

	return errors.Join(errs...)
}
