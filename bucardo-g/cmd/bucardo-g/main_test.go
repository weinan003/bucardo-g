package main

import (
	"testing"

	"github.com/bucardo-g/internal/replication"
)

func TestStatusForStats(t *testing.T) {
	tests := []struct {
		name  string
		stats replication.Stats
		want  string
	}{
		{name: "empty", want: "empty"},
		{name: "insert", stats: replication.Stats{Inserts: 1}, want: "good"},
		{name: "update", stats: replication.Stats{Updates: 1}, want: "good"},
		{name: "delete", stats: replication.Stats{Deletes: 1}, want: "good"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := statusForStats(test.stats); got != test.want {
				t.Fatalf("statusForStats(%+v) = %q, want %q", test.stats, got, test.want)
			}
		})
	}
}
