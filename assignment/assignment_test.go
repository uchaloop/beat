package assignment

import (
	"math"
	"math/big"
	"math/rand/v2"
	"testing"
	"time"
)

var epoch = time.Unix(0, 0).UTC()

func at(d time.Duration) time.Time { return epoch.Add(d) }

func mustRotation(t *testing.T, cfg Config) *Rotation {
	t.Helper()

	r, err := MakeRotation(cfg)
	if err != nil {
		t.Fatalf("MakeRotation: %v", err)
	}

	return r
}

func TestMakeRotation_Rejects(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{"no clusters", Config{Current: "el", Period: time.Minute}},
		{"empty name", Config{Clusters: []string{"el", ""}, Current: "el", Period: time.Minute}},
		{"surrounding space", Config{Clusters: []string{"el", " xc"}, Current: "el", Period: time.Minute}},
		{"duplicate", Config{Clusters: []string{"el", "el"}, Current: "el", Period: time.Minute}},
		{"unknown current", Config{Clusters: []string{"el", "xc"}, Current: "dm", Period: time.Minute}},
		{"case matters", Config{Clusters: []string{"el", "xc"}, Current: "EL", Period: time.Minute}},
		{"zero period", Config{Clusters: []string{"el"}, Current: "el"}},
		{"negative period", Config{Clusters: []string{"el"}, Current: "el", Period: -time.Minute}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := MakeRotation(tc.cfg); err == nil {
				t.Error("accepted an invalid config")
			}
		})
	}
}

// The order given must not matter, and the caller must be free to reuse its
// slice afterwards.
func TestMakeRotation_SortsAndCopies(t *testing.T) {
	clusters := []string{"xc", "el", "dm"}

	rotation := mustRotation(t, Config{Clusters: clusters, Current: "el", Period: time.Minute})

	clusters[0] = "zz"

	decision, err := rotation.Decide(Invocation{ScheduledAt: at(0)})
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if decision.Owner != "dm" {
		t.Errorf("slot 0 owner = %q, want dm (sorted first)", decision.Owner)
	}
	if rotation.ClusterCount() != 3 {
		t.Errorf("ClusterCount() = %d, want 3", rotation.ClusterCount())
	}
}

// Every permutation of the same set must produce the same owner, or clusters
// would disagree with each other.
func TestDecide_PermutationsAgree(t *testing.T) {
	permutations := [][]string{
		{"el", "xc", "dm"},
		{"dm", "el", "xc"},
		{"xc", "dm", "el"},
		{"el", "dm", "xc"},
	}

	for slot := range int64(9) {
		var want string

		for _, clusters := range permutations {
			rotation := mustRotation(t, Config{Clusters: clusters, Current: "el", Period: 4 * time.Hour})

			decision, err := rotation.Decide(Invocation{ScheduledAt: at(time.Duration(slot) * 4 * time.Hour)})
			if err != nil {
				t.Fatalf("Decide: %v", err)
			}
			if decision.Slot != uint64(slot) {
				t.Fatalf("Slot = %d, want %d", decision.Slot, slot)
			}

			if len(want) == 0 {
				want = decision.Owner
				continue
			}
			if decision.Owner != want {
				t.Fatalf("slot %d: %v gave owner %q, want %q", slot, clusters, decision.Owner, want)
			}
		}
	}
}

func TestDecide_RotatesInSortedOrder(t *testing.T) {
	rotation := mustRotation(t, Config{
		Clusters: []string{"el", "xc", "dm"},
		Current:  "el",
		Period:   4 * time.Hour,
	})

	// Sorted: dm, el, xc.
	want := []string{"dm", "el", "xc", "dm", "el", "xc"}

	for slot, owner := range want {
		decision, err := rotation.Decide(Invocation{
			ScheduledAt: at(time.Duration(slot) * 4 * time.Hour),
		})
		if err != nil {
			t.Fatalf("Decide: %v", err)
		}

		if decision.Owner != owner {
			t.Errorf("slot %d owner = %q, want %q", slot, decision.Owner, owner)
		}
		if isExecutable := decision.IsExecutable; isExecutable != (owner == "el") {
			t.Errorf("slot %d IsExecutable = %v for owner %q",
				slot,
				isExecutable,
				owner)
		}
	}
}

// One cluster owns every point; the policy is then a no-op that still validates.
func TestDecide_SingleCluster(t *testing.T) {
	rotation := mustRotation(t, Config{Clusters: []string{"el"}, Current: "el", Period: time.Minute})

	for slot := range int64(5) {
		decision, err := rotation.Decide(Invocation{ScheduledAt: at(time.Duration(slot) * time.Minute)})
		if err != nil {
			t.Fatalf("Decide: %v", err)
		}
		if !decision.IsExecutable || decision.Owner != "el" {
			t.Errorf("slot %d = %+v, want el executing", slot, decision)
		}
	}
}

func TestDecide_Rejects(t *testing.T) {
	rotation := mustRotation(t, Config{
		Clusters: []string{"el", "xc", "dm"},
		Current:  "el",
		Period:   5 * time.Minute,
	})

	tests := []struct {
		name string
		when time.Time
	}{
		{"zero time", time.Time{}},
		{"off the grid", at(5*time.Minute + time.Second)},
		{"off the grid by a nanosecond", at(5*time.Minute + 1)},
		{"before the epoch", epoch.Add(-5 * time.Minute)},
		{"beyond the representable range", time.Unix(math.MaxInt64/1_000_000_000+1, 0)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := rotation.Decide(Invocation{ScheduledAt: tc.when}); err == nil {
				t.Error("accepted an invalid invocation")
			}
		})
	}
}

// A late attempt and its retry decide as the original point did.
func TestDecide_OwnerFollowsThePointNotTheClock(t *testing.T) {
	rotation := mustRotation(t, Config{
		Clusters: []string{"el", "xc", "dm"},
		Current:  "el",
		Period:   4 * time.Hour,
	})

	point := Invocation{ScheduledAt: at(4 * time.Hour)} // slot 1, owner el

	first, err := rotation.Decide(point)
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}

	// The same point decided again - hours later, on a retry - is unchanged.
	second, err := rotation.Decide(point)
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}

	if first != second {
		t.Errorf("the same point gave %+v then %+v", first, second)
	}
	if !first.IsExecutable {
		t.Errorf("slot 1 should belong to el, got %q", first.Owner)
	}
}

// The timezone of the point is not part of its identity: the grid is absolute.
func TestDecide_LocationDoesNotChangeTheOwner(t *testing.T) {
	rotation := mustRotation(t, Config{
		Clusters: []string{"el", "xc", "dm"},
		Current:  "el",
		Period:   4 * time.Hour,
	})

	utc := at(8 * time.Hour)

	elsewhere, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip(err)
	}

	a, err := rotation.Decide(Invocation{ScheduledAt: utc})
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	b, err := rotation.Decide(Invocation{ScheduledAt: utc.In(elsewhere)})
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}

	if a.Owner != b.Owner || a.Slot != b.Slot {
		t.Errorf("same instant gave %+v and %+v", a, b)
	}
}

func TestOwnedAfter(t *testing.T) {
	rotation := mustRotation(t, Config{
		Clusters: []string{"el", "xc", "dm"}, Current: "el", Period: time.Minute,
	})
	// Sorted: dm, el, xc. el owns slots 1, 4, 7, ...
	tests := []struct {
		name               string
		after, count, want uint64
	}{
		{"empty", 3, 0, 0},
		{"foreign", 2, 1, 0},
		{"own", 3, 1, 1},
		{"exclude starting slot", 1, 1, 0},
		{"full turn", 1, 3, 1},
		{"two turns", 1, 6, 2},
		{"across start", 0, 10, 4},
		{"a day", 0, 1440, 480},
		{"beyond maximum slot", math.MaxUint64, 1, 1},
		{"maximum count", math.MaxUint64, math.MaxUint64, math.MaxUint64 / 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := rotation.OwnedAfter(tc.after, tc.count); got != tc.want {
				t.Fatalf("OwnedAfter(%d, %d) = %d, want %d", tc.after, tc.count, got, tc.want)
			}
		})
	}
	single := mustRotation(t, Config{Clusters: []string{"el"}, Current: "el", Period: time.Second})
	if got := single.OwnedAfter(math.MaxUint64, math.MaxUint64); got != math.MaxUint64 {
		t.Fatalf("single cluster count = %d", got)
	}
}

func TestOwnedAfter_MatchesAWalk(t *testing.T) {
	names := []string{"dm", "el", "xc"}
	for _, name := range names {
		rotation := mustRotation(t, Config{Clusters: names, Current: name, Period: time.Minute})
		for after := range uint64(12) {
			for count := range uint64(24) {
				var want uint64
				for slot := after + 1; slot <= after+count; slot++ {
					decision, err := rotation.Decide(Invocation{ScheduledAt: at(time.Duration(slot) * time.Minute)})
					if err != nil {
						t.Fatal(err)
					}
					if decision.IsExecutable {
						want++
					}
				}
				if got := rotation.OwnedAfter(after, count); got != want {
					t.Fatalf("%s OwnedAfter(%d, %d) = %d, want %d", name, after, count, got, want)
				}
			}
		}
	}
}

// A big-integer interval is an independent oracle: unlike uint64, its end
// cannot overflow even when both inputs are MaxUint64.
func TestOwnedAfter_MatchesUnboundedInterval(t *testing.T) {
	rng := rand.New(rand.NewPCG(73, 99))
	names := []string{"a", "b", "c", "d", "e", "f", "g"}
	for size := 1; size <= len(names); size++ {
		for owner := range size {
			rotation := mustRotation(t, Config{Clusters: names[:size], Current: names[owner], Period: time.Second})
			for trial := range 1000 {
				after, count := rng.Uint64(), rng.Uint64()
				if trial == 0 {
					after, count = math.MaxUint64, math.MaxUint64
				}
				if trial == 1 {
					after, count = 0, math.MaxUint64
				}
				divisor := big.NewInt(int64(size))
				lower := new(big.Int).SetUint64(after)
				lower.Sub(lower, big.NewInt(int64(owner)))
				upper := new(big.Int).Add(lower, new(big.Int).SetUint64(count))
				upper.Div(upper, divisor)
				lower.Div(lower, divisor)
				want := upper.Sub(upper, lower).Uint64()
				if got := rotation.OwnedAfter(after, count); got != want || got > count {
					t.Fatalf("size=%d owner=%d after=%d count=%d got=%d want=%d", size, owner, after, count, got, want)
				}
			}
		}
	}
}

func TestDecide_LastRepresentableNanosecond(t *testing.T) {
	rotation := mustRotation(t, Config{Clusters: []string{"el"}, Current: "el", Period: time.Nanosecond})
	last := time.Unix(0, math.MaxInt64)
	decision, err := rotation.Decide(Invocation{ScheduledAt: last})
	if err != nil || decision.Slot != uint64(math.MaxInt64) {
		t.Fatalf("decision=%+v err=%v", decision, err)
	}
	if _, err := rotation.Decide(Invocation{ScheduledAt: last.Add(time.Nanosecond)}); err == nil {
		t.Fatal("accepted a point beyond the supported time range")
	}
}
