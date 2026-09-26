// Package job models the lifecycle and terminal status of one Sync run.
package job

import (
	"fmt"
	"time"
)

type Status string

const (
	StatusCreated Status = "created"
	StatusRunning Status = "running"
	StatusGood    Status = "good"
	StatusBad     Status = "bad"
	StatusEmpty   Status = "empty"
)

type Job struct {
	Sync      string
	Status    Status
	StartedAt time.Time
	EndedAt   time.Time
	Inserts   int64
	Updates   int64
	Deletes   int64
	Conflicts int64
	Err       error
}

func Start(sync string, now time.Time) (Job, error) {
	if sync == "" {
		return Job{}, fmt.Errorf("sync name is required")
	}
	return Job{Sync: sync, Status: StatusRunning, StartedAt: now}, nil
}

func (j *Job) Finish(status Status, now time.Time, err error) error {
	if j.Status != StatusRunning {
		return fmt.Errorf("job is not running")
	}
	if status != StatusGood && status != StatusBad && status != StatusEmpty {
		return fmt.Errorf("invalid terminal status %q", status)
	}
	j.Status, j.EndedAt, j.Err = status, now, err
	return nil
}
