// Package topology defines the validated runtime topology consumed by replication.
// It intentionally contains no YAML or PostgreSQL client types.
package topology

import (
	"github.com/bucardo-g/internal/domain/database"
	"github.com/bucardo-g/internal/domain/table"
)

type Topology struct {
	Name             string
	Sources          []database.Database
	Targets          []database.Database
	DeleteMethod     string
	ConflictStrategy string
	SourcePriority   []string
	Tables           []table.Table
}
