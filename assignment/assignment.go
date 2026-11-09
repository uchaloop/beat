/*
Package assignment picks one cluster to own a scheduled point when the same
deployment runs in several of them.

It is a pure policy: it reads no clock, opens no connection and keeps no state
between decisions. Every process computes the same answer for the same point
from the same configuration, so nothing has to be coordinated at run time.

	rotation, err := assignment.MakeRotation(assignment.Config{
		Clusters: []string{"el", "xc", "dm"},
		Current:  clusterName,
		Period:   5 * time.Minute,
	})

	decision, err := rotation.Decide(assignment.Invocation{ScheduledAt: point})

Ownership rotates over the clusters in sorted order, one point each:

	slot    = (ScheduledAt - Unix epoch) / Period
	owner   = sorted(Clusters)[slot % len(Clusters)]
	execute = owner == Current

# What it does and does not promise

It decides who should try, not that anyone did. If the owner of a point is down,
that point is simply not served - no other cluster takes it over, because that
would need shared state this package deliberately does not have. The work must
therefore survive a skipped attempt: keep it in a queue and let the next
attempt pick it up.

It also does not prevent overlap. A delayed attempt and its retry keep the owner
of their original point, so an owner still working on one point can overlap the
next owner's work.

With N clusters a point comes round to each of them every N periods. Three
clusters on a five-minute period means each one gets an attempt every fifteen
minutes, and the whole deployment attempts every five.

# Changing the deployment

A cluster added, removed or renamed - or a different period - changes who owns
which point, and every process has to be deciding from the same configuration
for the rotation to divide anything. Nothing here checks that they are, so a
rolling update, with the old and the new configuration deciding side by side,
produces duplicates and gaps at the same time.

Before changing the topology or period, stop scheduling in every cluster and
wait for all old attempts to finish. Then start every participant with the same
new configuration, accepting the pause. A rolling restart is not sufficient:
old and new configurations must not execute side by side. There is no hot
reconfiguration and no agreement protocol, by choice: either one would need
exactly the shared state this package exists without.

# What it needs from the caller

The identity of the scheduled point, not the current time. A pod that starts
late, or a retry of the same scheduled Job, must decide as the original point
did - so the caller passes that point in rather than reading a clock. A
long-lived scheduler takes it from its own grid, before any per-replica offset:
an offset staggers replicas, it must not move a point to another cluster.
*/
package assignment

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"
)

// Config describes the deployment this process belongs to.
type Config struct {
	// Clusters is every cluster the deployment runs in. The list is copied and
	// sorted, so the order given does not matter, but the set must be the same
	// in every cluster or they will not agree on an owner.
	Clusters []string

	// Current is the cluster this process runs in. It must be one of Clusters:
	// an identity the list does not know is a configuration error, never a
	// reason to run everywhere.
	Current string

	// Period is the spacing of the scheduled points, which must match the
	// schedule that produces them.
	Period time.Duration
}

// Invocation is the scheduled point a decision is about.
type Invocation struct {
	// ScheduledAt is the nominal point of the grid, before any per-replica
	// offset. It is an identity, not a measurement: a late start or a retry
	// carries the point it was meant to serve.
	ScheduledAt time.Time
}

// Decision is what the policy concluded about one point.
type Decision struct {
	Invocation   Invocation
	Slot         uint64
	Owner        string
	IsExecutable bool
}

// Rotation answers who owns a scheduled point. Build it with MakeRotation; it
// is immutable and safe for concurrent use.
type Rotation struct {
	clusters []string
	current  string
	period   time.Duration

	// ownRemainder is the remainder every slot of this process leaves: the point
	// is ours exactly when slot % len(clusters) equals it.
	ownRemainder uint64
}

// MakeRotation validates cfg and returns the policy it describes. The cluster
// list is copied and sorted, so callers may reuse their slice.
//
// Identifiers are case-sensitive and must be non-empty and free of surrounding
// space; an empty list, a duplicate, a Current the list does not contain, or a
// non-positive Period are errors. Nothing is corrected silently: a typo that
// quietly turned into "run everywhere" would be worse than a failed start.
func MakeRotation(cfg Config) (*Rotation, error) {
	if len(cfg.Clusters) == 0 {
		return nil, errors.New("clusters are required")
	}

	if cfg.Period <= 0 {
		return nil, fmt.Errorf("period %v must be > 0", cfg.Period)
	}

	clusters := slices.Clone(cfg.Clusters)
	for _, name := range clusters {
		if len(name) == 0 {
			return nil, errors.New("cluster names must not be empty")
		}
		if name != strings.TrimSpace(name) {
			return nil, fmt.Errorf("cluster name %q has surrounding space", name)
		}
	}

	slices.Sort(clusters)
	if i := firstDuplicate(clusters); i >= 0 {
		return nil, fmt.Errorf("cluster %q is listed twice", clusters[i])
	}

	if !slices.Contains(clusters, cfg.Current) {
		return nil, fmt.Errorf("current cluster %q is not one of %v", cfg.Current, clusters)
	}

	return &Rotation{
		clusters:     clusters,
		current:      cfg.Current,
		period:       cfg.Period,
		ownRemainder: uint64(slices.Index(clusters, cfg.Current)),
	}, nil
}

// ClusterCount reports how many clusters share the rotation. A scheduler uses
// it to describe its own cadence: a point comes round to one cluster every
// ClusterCount periods.
func (r *Rotation) ClusterCount() int { return len(r.clusters) }

// Period reports the spacing the policy was built for, so a scheduler can check
// that it matches its own.
func (r *Rotation) Period() time.Duration { return r.period }

// Current reports the cluster this process runs in.
func (r *Rotation) Current() string { return r.current }

// Decide reports who owns the given point and whether this process should run
// it. An invocation that is not a point of the configured grid is an error, not
// a silent yes or no.
// Supported instants range from the Unix epoch through epoch + MaxInt64
// nanoseconds, inclusive. The unsigned Slot does not extend this time range.
func (r *Rotation) Decide(in Invocation) (Decision, error) {
	if in.ScheduledAt.IsZero() {
		return Decision{}, errors.New("scheduled point is required")
	}

	elapsed, isOK := nanosSinceEpoch(in.ScheduledAt)
	if !isOK {
		return Decision{}, fmt.Errorf("scheduled point %v is outside the representable range", in.ScheduledAt)
	}

	period := int64(r.period)
	if elapsed%period != 0 {
		return Decision{}, fmt.Errorf("scheduled point %v is not on the %v grid", in.ScheduledAt, r.period)
	}

	slot := uint64(elapsed / period)
	owner := r.clusters[slot%uint64(len(r.clusters))]

	return Decision{
		Invocation:   in,
		Slot:         slot,
		Owner:        owner,
		IsExecutable: owner == r.current,
	}, nil
}

// OwnedAfter reports how many of count consecutive slots following after
// belong to this process. The starting slot is excluded; a zero count
// reports zero. The result never exceeds count.
//
// It uses constant-time cyclic arithmetic without computing after + count.
// Both arguments may span uint64's full range: the mathematical sequence can
// extend beyond MaxUint64 without wrapping the slot numbering to zero.
// This counting operation does not extend the time range accepted by Decide.
func (r *Rotation) OwnedAfter(after, count uint64) uint64 {
	clusters := uint64(len(r.clusters))
	owned := count / clusters
	tail := count % clusters
	position := after % clusters

	// Distance to our next slot lies in [1, clusters], never zero: after
	// is excluded even when it is one of our own slots.
	var distance uint64
	if r.ownRemainder > position {
		distance = r.ownRemainder - position
	} else {
		distance = clusters - (position - r.ownRemainder)
	}

	if distance <= tail {
		owned++
	}

	return owned
}

// nanosSinceEpoch converts t to nanoseconds since the Unix epoch without the
// silent wrap of UnixNano or the saturation of Sub. Points before the epoch and
// beyond the int64 range of nanoseconds are refused rather than folded onto
// some other slot.
func nanosSinceEpoch(t time.Time) (int64, bool) {
	const nsPerSec = int64(time.Second)

	secs := t.Unix()
	if secs < 0 || secs > math.MaxInt64/nsPerSec {
		return 0, false
	}

	whole := secs * nsPerSec

	fraction := int64(t.Nanosecond())
	if whole > math.MaxInt64-fraction {
		return 0, false
	}

	return whole + fraction, true
}

// firstDuplicate reports the index of the first repeat in a sorted slice, or -1.
func firstDuplicate(sorted []string) int {
	for i := 1; i < len(sorted); i++ {
		if sorted[i] == sorted[i-1] {
			return i
		}
	}

	return -1
}
