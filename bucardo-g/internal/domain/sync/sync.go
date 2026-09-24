// Package sync defines the Bucardo-G replication configuration aggregate.
package sync

import (
	"fmt"
	"regexp"
)

var namePattern = regexp.MustCompile(`^[A-Za-z]\w*$`)

// Status represents the administrative lifecycle state of a Sync.
type Status string

const (
	// StatusActive permits a Sync controller to schedule replication jobs.
	StatusActive Status = "active"
	// StatusPaused prevents new jobs while preserving the configured Sync.
	StatusPaused Status = "paused"
	// StatusInactive prevents a Sync controller from running the Sync.
	StatusInactive Status = "inactive"
)

// Sync is the replication configuration aggregate.
type Sync struct {
	Name         string
	SourceHerd   string
	TargetGroup  string
	Status       Status
	Autokick     bool
	DeleteMethod string
}

// New validates and creates a Sync in its active state.
func New(name, sourceHerd, targetGroup string, autokick bool, deleteMethod string) (Sync, error) {
	if !namePattern.MatchString(name) {
		return Sync{}, fmt.Errorf("sync name %q must start with a letter and contain only letters, digits, or underscores", name)
	}
	if sourceHerd == "" {
		return Sync{}, fmt.Errorf("sync %q requires a source herd", name)
	}
	if targetGroup == "" {
		return Sync{}, fmt.Errorf("sync %q requires a target group", name)
	}
	if !validDeleteMethod(deleteMethod) {
		return Sync{}, fmt.Errorf("sync %q has unsupported delete method %q", name, deleteMethod)
	}

	return Sync{
		Name:         name,
		SourceHerd:   sourceHerd,
		TargetGroup:  targetGroup,
		Status:       StatusActive,
		Autokick:     autokick,
		DeleteMethod: deleteMethod,
	}, nil
}

// CanTransitionTo reports whether an administrative state change is valid.
func (s Sync) CanTransitionTo(next Status) bool {
	if s.Status == next {
		return true
	}

	switch s.Status {
	case StatusActive:
		return next == StatusPaused || next == StatusInactive
	case StatusPaused:
		return next == StatusActive || next == StatusInactive
	case StatusInactive:
		return next == StatusActive
	default:
		return false
	}
}

// TransitionTo changes the administrative state when the transition is valid.
func (s *Sync) TransitionTo(next Status) error {
	if !s.CanTransitionTo(next) {
		return fmt.Errorf("sync %q cannot transition from %q to %q", s.Name, s.Status, next)
	}

	s.Status = next
	return nil
}

func validDeleteMethod(method string) bool {
	return method == "delete" || method == "truncate" || method == "truncate_cascade"
}
