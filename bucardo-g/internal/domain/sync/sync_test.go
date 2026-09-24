package sync

import "testing"

func TestNew(t *testing.T) {
	t.Parallel()

	replication, err := New("orders_sync", "orders", "targets", true, "delete")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if replication.Status != StatusActive {
		t.Errorf("Status = %q, want %q", replication.Status, StatusActive)
	}
	if !replication.Autokick {
		t.Error("Autokick = false, want true")
	}
}

func TestNewRejectsInvalidConfiguration(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name         string
		syncName     string
		sourceHerd   string
		targetGroup  string
		deleteMethod string
	}{
		{
			name:         "invalid sync name",
			syncName:     "1orders",
			sourceHerd:   "orders",
			targetGroup:  "targets",
			deleteMethod: "delete",
		},
		{
			name:         "missing source herd",
			syncName:     "orders",
			targetGroup:  "targets",
			deleteMethod: "delete",
		},
		{
			name:         "missing target group",
			syncName:     "orders",
			sourceHerd:   "orders",
			deleteMethod: "delete",
		},
		{
			name:         "unsupported delete method",
			syncName:     "orders",
			sourceHerd:   "orders",
			targetGroup:  "targets",
			deleteMethod: "upsert",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			_, err := New(
				testCase.syncName,
				testCase.sourceHerd,
				testCase.targetGroup,
				false,
				testCase.deleteMethod,
			)
			if err == nil {
				t.Fatal("New() error = nil, want validation error")
			}
		})
	}
}

func TestTransitionTo(t *testing.T) {
	t.Parallel()

	replication, err := New("orders_sync", "orders", "targets", false, "truncate")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	for _, status := range []Status{StatusPaused, StatusActive, StatusInactive, StatusActive} {
		if err := replication.TransitionTo(status); err != nil {
			t.Fatalf("TransitionTo(%q) error = %v", status, err)
		}
	}

	if replication.Status != StatusActive {
		t.Errorf("Status = %q, want %q", replication.Status, StatusActive)
	}
}

func TestTransitionToRejectsUnknownCurrentState(t *testing.T) {
	t.Parallel()

	replication := Sync{Name: "orders_sync", Status: Status("unknown")}
	if err := replication.TransitionTo(StatusActive); err == nil {
		t.Fatal("TransitionTo() error = nil, want validation error")
	}
}
