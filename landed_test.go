package pgfallback

import (
	"testing"
	"time"
)

// A new connection that lands on read once an attempt at rw worked keeps
// read connections in use only when it tried rw after that attempt started.
func TestLandedOnReadAfterBack(t *testing.T) {
	attempt := time.Now()
	for _, c := range []struct {
		name     string
		started  time.Time
		wantBack bool
	}{
		{"started before the attempt", attempt.Add(-5 * time.Second), true},
		{"started after the attempt", attempt.Add(time.Millisecond), false},
	} {
		t.Run(c.name, func(t *testing.T) {
			// No rw config, so no attempt starts.
			f := &fallback{back: true, backFrom: attempt}
			f.landedOnRead(c.started)
			if f.back != c.wantBack {
				t.Errorf("back is %t, want %t", f.back, c.wantBack)
			}
		})
	}
}
