package beat

import (
	"math"
	"testing"
	"time"
)

// epoch anchors every case below, because the grid is defined against the Unix
// epoch: expressing a time as an offset from it makes the expected point
// readable without arithmetic.
var epoch = time.Unix(0, 0)

func at(d time.Duration) time.Time { return epoch.Add(d) }

func TestSchedule_Grid(t *testing.T) {
	tests := []struct {
		name   string
		period time.Duration
		offset time.Duration
		now    time.Duration
		want   time.Duration
	}{
		{
			name:   "next point of the period",
			period: 5 * time.Minute,
			now:    10*time.Minute + 30*time.Second,
			want:   15 * time.Minute,
		},
		{
			name:   "a point exactly reached is behind us",
			period: 5 * time.Minute,
			now:    10 * time.Minute,
			want:   15 * time.Minute,
		},
		{
			name:   "the offset shifts every point",
			period: 5 * time.Minute,
			offset: 25 * time.Second,
			now:    10*time.Minute + 30*time.Second,
			want:   15*time.Minute + 25*time.Second,
		},
		{
			name:   "before this period's offset the point is still this period's",
			period: 5 * time.Minute,
			offset: 25 * time.Second,
			now:    10*time.Minute + 20*time.Second,
			want:   10*time.Minute + 25*time.Second,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := schedule{mode: ModeFixedRate, period: tc.period, offset: tc.offset}

			if got := s.nextGridPoint(at(tc.now)); !got.Equal(at(tc.want)) {
				t.Errorf("grid = %v, want %v", got.Sub(epoch), tc.want)
			}
		})
	}
}

func TestSchedule_First(t *testing.T) {
	now := at(10*time.Minute + 30*time.Second)

	rate := schedule{mode: ModeFixedRate, period: 5 * time.Minute, offset: 25 * time.Second}
	if got, want := rate.firstTarget(now), at(15*time.Minute+25*time.Second); !got.Equal(want) {
		t.Errorf("fixed rate first = %v, want %v", got.Sub(epoch), want.Sub(epoch))
	}

	// Fixed delay has no grid to join, so it starts as soon as the offset is up.
	delay := schedule{mode: ModeFixedDelay, period: 5 * time.Minute, offset: 25 * time.Second}
	if got, want := delay.firstTarget(now), at(10*time.Minute+55*time.Second); !got.Equal(want) {
		t.Errorf("fixed delay first = %v, want %v", got.Sub(epoch), want.Sub(epoch))
	}
}

func TestSchedule_After(t *testing.T) {
	tests := []struct {
		name       string
		mode       Mode
		period     time.Duration
		offset     time.Duration
		target     time.Duration
		end        time.Duration
		now        time.Duration
		backoff    time.Duration
		wantNext   time.Duration
		wantMissed uint64
	}{
		{
			name:     "a run inside its period keeps the cadence",
			mode:     ModeFixedRate,
			period:   5 * time.Minute,
			target:   5 * time.Minute,
			end:      5*time.Minute + 30*time.Second,
			now:      5*time.Minute + 30*time.Second,
			wantNext: 10 * time.Minute,
		},
		{
			// The regression this whole design exists for: deriving the next
			// point from the clock after the run made an offset cost a full
			// period whenever the job ran longer than period minus offset.
			name:     "an offset does not cost a period",
			mode:     ModeFixedRate,
			period:   5 * time.Minute,
			offset:   25 * time.Second,
			target:   5*time.Minute + 25*time.Second,
			end:      10*time.Minute + 15*time.Second,
			now:      10*time.Minute + 15*time.Second,
			wantNext: 10*time.Minute + 25*time.Second,
		},
		{
			name:       "a run longer than the period misses the points it covered",
			mode:       ModeFixedRate,
			period:     5 * time.Minute,
			target:     5 * time.Minute,
			end:        17 * time.Minute,
			now:        17 * time.Minute,
			wantNext:   20 * time.Minute,
			wantMissed: 2,
		},
		{
			name:       "a gap that is an exact multiple does not overshoot",
			mode:       ModeFixedRate,
			period:     5 * time.Minute,
			target:     5 * time.Minute,
			end:        20 * time.Minute,
			now:        20 * time.Minute,
			wantNext:   20 * time.Minute,
			wantMissed: 2,
		},
		{
			name:     "a slow handler does not shift the grid",
			mode:     ModeFixedRate,
			period:   time.Minute,
			target:   time.Minute,
			end:      time.Minute + 5*time.Second,
			now:      time.Minute + 50*time.Second,
			wantNext: 2 * time.Minute,
		},
		{
			// The pause moves the run past those points, but they are not a
			// loss: the application asked for the pause.
			name:       "a backoff moves the run on without missing anything",
			mode:       ModeFixedRate,
			period:     time.Minute,
			target:     time.Minute,
			end:        time.Minute + 10*time.Second,
			now:        time.Minute + 10*time.Second,
			backoff:    3 * time.Minute,
			wantNext:   5 * time.Minute,
			wantMissed: 0,
		},
		{
			// An idle poller resting ten minutes on a one-minute period. Counting
			// what the backoff held back would report ten losses per cycle, sixty
			// an hour, from a job doing exactly what it was told.
			name:       "an idle poller at rest reports no losses",
			mode:       ModeFixedRate,
			period:     time.Minute,
			end:        200 * time.Millisecond,
			now:        200 * time.Millisecond,
			backoff:    10 * time.Minute,
			wantNext:   11 * time.Minute,
			wantMissed: 0,
		},
		{
			// The Handler runs after the Job and inside the loop, so a slow one
			// costs points exactly as a slow Job does.
			name:       "a handler slow enough to outlast points loses them",
			mode:       ModeFixedRate,
			period:     time.Minute,
			target:     time.Minute,
			end:        time.Minute + 5*time.Second,
			now:        3*time.Minute + 30*time.Second,
			wantNext:   4 * time.Minute,
			wantMissed: 2,
		},
		{
			// A clock that jumped is one arithmetic step, not a walk over every
			// point in between.
			name:       "a clock jump lands on the first point after it",
			mode:       ModeFixedRate,
			period:     time.Second,
			target:     time.Second,
			end:        time.Second,
			now:        time.Hour,
			wantNext:   time.Hour,
			wantMissed: 3598,
		},
		{
			name:     "fixed delay measures the period from the end",
			mode:     ModeFixedDelay,
			period:   5 * time.Minute,
			target:   5 * time.Minute,
			end:      8 * time.Minute,
			now:      8 * time.Minute,
			wantNext: 13 * time.Minute,
		},
		{
			name:     "fixed delay treats a backoff as a floor, not an addition",
			mode:     ModeFixedDelay,
			period:   time.Minute,
			target:   time.Minute,
			end:      time.Minute,
			now:      time.Minute,
			backoff:  3 * time.Minute,
			wantNext: 4 * time.Minute,
		},
		{
			name:     "fixed delay ignores a backoff shorter than the period",
			mode:     ModeFixedDelay,
			period:   5 * time.Minute,
			target:   time.Minute,
			end:      time.Minute,
			now:      time.Minute,
			backoff:  time.Minute,
			wantNext: 6 * time.Minute,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := schedule{mode: tc.mode, period: tc.period, offset: tc.offset}

			next, missed := s.nextTarget(at(tc.now), at(tc.target), at(tc.end), tc.backoff)

			if !next.Equal(at(tc.wantNext)) {
				t.Errorf("next = %v, want %v", next.Sub(epoch), tc.wantNext)
			}
			if missed != tc.wantMissed {
				t.Errorf("missed = %d, want %d", missed, tc.wantMissed)
			}
		})
	}
}

func TestOffsetFor(t *testing.T) {
	const max = 30 * time.Second

	first := OffsetFor("daemon-ozon-ship-7d9c", max)
	if first < 0 || first >= max {
		t.Fatalf("offset %v is outside [0, %v)", first, max)
	}

	if second := OffsetFor("daemon-ozon-ship-7d9c", max); second != first {
		t.Errorf("offset is not stable: %v then %v", first, second)
	}

	if other := OffsetFor("daemon-ozon-ship-4a11", max); other == first {
		t.Error("two identities produced the same offset")
	}

	if got := OffsetFor("", max); got != 0 {
		t.Errorf("empty id = %v, want 0", got)
	}
	if got := OffsetFor("pod", 0); got != 0 {
		t.Errorf("zero max = %v, want 0", got)
	}
}

func TestRandomOffset_StaysBelowMax(t *testing.T) {
	const max = time.Second

	for range 100 {
		if got := randomOffset(max); got < 0 || got >= max {
			t.Fatalf("offset %v is outside [0, %v)", got, max)
		}
	}

	if got := randomOffset(0); got != 0 {
		t.Errorf("zero max = %v, want 0", got)
	}
}

func TestSchedule_ExtremeBackoffStaysOnFutureGrid(t *testing.T) {
	const period = 5 * time.Minute
	s := schedule{mode: ModeFixedRate, period: period}
	target := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	end := target.Add(10 * time.Minute)
	backoff := time.Duration(1<<63 - 1)
	boundary := end.Add(backoff)

	next, missed := s.nextTarget(end, target, end, backoff)
	if next.Before(boundary) || next.Sub(boundary) >= period {
		t.Fatalf("next = %v, boundary = %v", next, boundary)
	}
	// The grid is minute-aligned, including beyond UnixNano's supported range.
	if next.Second() != 0 || next.Nanosecond() != 0 || next.Minute()%5 != 0 {
		t.Fatalf("next is off grid: %v", next)
	}
	if missed != 1 {
		t.Fatalf("missed = %d, want 1; backoff is not a loss", missed)
	}
}

func TestSchedule_ExtremeGapCapsMissed(t *testing.T) {
	s := schedule{mode: ModeFixedRate, period: time.Nanosecond}
	target := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	boundary := target.AddDate(600, 0, 0)
	next, missed := s.advanceGrid(target, boundary)
	if !next.Equal(boundary) {
		t.Fatalf("next = %v, want %v", next, boundary)
	}
	if want := uint64(math.MaxUint64); missed != want {
		t.Fatalf("missed = %d, want capped count %d", missed, want)
	}
}

func TestSchedule_MissedExceedsSignedRange(t *testing.T) {
	s := schedule{mode: ModeFixedRate, period: time.Nanosecond}
	target := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	boundary := target.AddDate(400, 0, 0)
	next, missed := s.advanceGrid(target, boundary)
	// Sub would saturate; seconds are enough to derive the exact expected count.
	want := uint64(boundary.Unix()-target.Unix())*uint64(time.Second) - 1
	if !next.Equal(boundary) || missed != want {
		t.Fatalf("next=%v missed=%d, want %v/%d", next, missed, boundary, want)
	}
}

func TestAddMissed_Saturates(t *testing.T) {
	tests := []struct{ current, additional, want uint64 }{
		{0, 0, 0}, {3, 4, 7}, {0, math.MaxUint64, math.MaxUint64},
		{math.MaxUint64 - 1, 1, math.MaxUint64},
		{math.MaxUint64 - 1, 2, math.MaxUint64},
		{math.MaxUint64, math.MaxUint64, math.MaxUint64},
	}
	for _, tc := range tests {
		if got := addMissed(tc.current, tc.additional); got != tc.want {
			t.Fatalf("addMissed(%d, %d)=%d, want %d", tc.current, tc.additional, got, tc.want)
		}
	}
}
