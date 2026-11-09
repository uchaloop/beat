package job

import (
	"testing"
	"time"
)

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		isOK bool
	}{
		{name: "zero means the default", cfg: Config{}, isOK: true},
		{name: "a positive timeout", cfg: Config{Timeout: time.Second}, isOK: true},
		{name: "a negative timeout", cfg: Config{Timeout: -time.Second}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()

			if tc.isOK && err != nil {
				t.Errorf("rejected a valid config: %v", err)
			}
			if !tc.isOK && err == nil {
				t.Error("accepted an invalid config")
			}
		})
	}
}

func TestSetDefaults(t *testing.T) {
	var cfg Config
	cfg.SetDefaults()

	if cfg.Timeout != defaultTimeout {
		t.Errorf("Timeout = %v, want %v", cfg.Timeout, defaultTimeout)
	}
}
