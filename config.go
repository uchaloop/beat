package beat

import (
	"errors"
	"fmt"
	"time"
)

// Config is the configuration beat declares. Both tag namespaces are inert
// strings: an application loads them with confmaker/confx (koanf from the file,
// env from the environment) and provides the filled Config into the container.
// beat itself never reads files or the environment.
type Config struct {
	// Spec is the schedule: an interval such as "@every 5s" or a cron
	// expression such as "*/5 * * * * *". Required.
	Spec string `koanf:"spec" env:"SPEC"`

	// JobTimeout bounds the context of a single Job execution. Zero (unset)
	// uses the default, 1m; every execution is bounded, so set a larger value
	// for a legitimately long Job.
	JobTimeout time.Duration `koanf:"job_timeout" env:"JOB_TIMEOUT"`

	// Jitter is the maximum random delay used to stagger instances so replicas
	// do not all fire at once. For an interval it is applied once before the
	// first run; for cron it is added to every tick, so keep it below the cron
	// interval. Zero disables it.
	Jitter time.Duration `koanf:"jitter" env:"JITTER"`
}

// Validate reports whether the Config is usable. confmaker/confx calls it after
// filling the struct.
func (c Config) Validate() error {
	if len(c.Spec) == 0 {
		return errors.New("spec is required")
	}
	if _, err := parseSchedule(c.Spec); err != nil {
		return fmt.Errorf("invalid spec %q: %w", c.Spec, err)
	}
	if c.JobTimeout < 0 {
		return errors.New("job_timeout must be >= 0")
	}
	if c.Jitter < 0 {
		return errors.New("jitter must be >= 0")
	}

	return nil
}
