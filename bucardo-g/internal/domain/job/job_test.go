package job

import (
	"testing"
	"time"
)

func TestJobLifecycle(t *testing.T) {
	now := time.Now()
	got, err := Start("sync_a", now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := got.Finish(StatusGood, now.Add(time.Second), nil); err != nil {
		t.Fatalf("unexpected finish error: %v", err)
	}
	if got.Status != StatusGood || got.EndedAt.IsZero() {
		t.Fatalf("unexpected finished job: %+v", got)
	}
}
