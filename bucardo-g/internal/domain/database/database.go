// Package database defines PostgreSQL endpoints used by Bucardo-G.
package database

import (
	"fmt"
	"regexp"
)

var namePattern = regexp.MustCompile(`^[A-Za-z]\w*$`)

// Type identifies the database engine supported by Bucardo-G.
type Type string

const (
	// TypePostgreSQL is the only database engine supported by Bucardo-G.
	TypePostgreSQL Type = "postgres"
)

// Status represents the availability of a configured database endpoint.
type Status string

const (
	// StatusActive permits the endpoint to participate in replication.
	StatusActive Status = "active"
	// StatusInactive prevents the endpoint from participating in replication.
	StatusInactive Status = "inactive"
	// StatusStalled marks an endpoint that needs connection recovery.
	StatusStalled Status = "stalled"
)

// Database is a named PostgreSQL connection endpoint.
type Database struct {
	Name       string
	Type       Type
	Connection string
	Status     Status
	MakeDelta  bool
}

// New validates and creates a PostgreSQL database endpoint.
func New(name string, databaseType Type, connection string, status Status, makeDelta bool) (Database, error) {
	if !namePattern.MatchString(name) {
		return Database{}, fmt.Errorf("database name %q must start with a letter and contain only letters, digits, or underscores", name)
	}
	if databaseType != TypePostgreSQL {
		return Database{}, fmt.Errorf("database %q uses unsupported type %q; Bucardo-G supports only %q", name, databaseType, TypePostgreSQL)
	}
	if connection == "" {
		return Database{}, fmt.Errorf("database %q requires a connection string", name)
	}
	if !validStatus(status) {
		return Database{}, fmt.Errorf("database %q has unsupported status %q", name, status)
	}

	return Database{
		Name:       name,
		Type:       databaseType,
		Connection: connection,
		Status:     status,
		MakeDelta:  makeDelta,
	}, nil
}

func validStatus(status Status) bool {
	return status == StatusActive || status == StatusInactive || status == StatusStalled
}
