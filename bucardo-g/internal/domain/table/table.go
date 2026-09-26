// Package table models source relations and their primary-key metadata.
package table

import (
	"fmt"
	"strings"
)

type RelationType string

const (
	RelationTable    RelationType = "table"
	RelationSequence RelationType = "sequence"
)

type Table struct {
	ID         int
	Database   string
	Schema     string
	Name       string
	Relation   RelationType
	PrimaryKey []string
}

func New(id int, database, schema, name string, relation RelationType, primaryKey []string) (Table, error) {
	t := Table{
		ID:         id,
		Database:   strings.TrimSpace(database),
		Schema:     strings.TrimSpace(schema),
		Name:       strings.TrimSpace(name),
		Relation:   relation,
		PrimaryKey: append([]string(nil), primaryKey...),
	}
	if err := t.Validate(); err != nil {
		return Table{}, err
	}
	return t, nil
}

func (t Table) Validate() error {
	if t.ID < 0 {
		return fmt.Errorf("table id must not be negative")
	}
	if t.Database == "" || t.Schema == "" || t.Name == "" {
		return fmt.Errorf("database, schema, and name are required")
	}
	if t.Relation != RelationTable && t.Relation != RelationSequence {
		return fmt.Errorf("unsupported relation type %q", t.Relation)
	}
	if t.Relation == RelationTable && len(t.PrimaryKey) == 0 {
		return fmt.Errorf("table %s requires a primary key", QualifiedName(t.Schema, t.Name))
	}
	for _, key := range t.PrimaryKey {
		if strings.TrimSpace(key) == "" {
			return fmt.Errorf("primary key columns must not be empty")
		}
	}
	return nil
}

func QualifiedName(schema, name string) string {
	return schema + "." + name
}
