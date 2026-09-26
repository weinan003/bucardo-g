// Package config defines the user-facing YAML configuration and its validation.
// The configuration is declarative; control-table rows are a derived projection.
package config

import (
	"bytes"
	"fmt"
	"os"
	"regexp"

	domainDatabase "github.com/bucardo-g/internal/domain/database"
	"github.com/bucardo-g/internal/domain/table"
	"gopkg.in/yaml.v3"
)

const template = "controlDatabase:\n" +
	"  dsn: postgres://user:password@127.0.0.1:5432/bucardo?sslmode=disable\n\n" +
	"databases:\n" +
	"  - name: source_a\n    role: source\n    dsn: postgres://user:password@127.0.0.1:5432/source_a?sslmode=disable\n" +
	"  - name: source_b\n    role: source\n    dsn: postgres://user:password@127.0.0.1:5433/source_b?sslmode=disable\n" +
	"  - name: target_a\n    role: target\n    dsn: postgres://user:password@127.0.0.1:5434/target_a?sslmode=disable\n" +
	"  - name: target_b\n    role: target\n    dsn: postgres://user:password@127.0.0.1:5435/target_b?sslmode=disable\n\n" +
	"syncs:\n  - name: bidirectional_sync\n    sources:\n      - source_a\n      - source_b\n    targets:\n      - target_a\n      - target_b\n    deleteMethod: delete\n    conflictStrategy: latest\n    # Other supported choices: abort, source_priority\n    # sourcePriority: [source_a, source_b]\n    tables:\n      - schema: public\n        name: example_table\n        primaryKey:\n          - id\n"

var namePattern = regexp.MustCompile(`^[A-Za-z]\w*$`)

type Config struct {
	ControlDatabase ControlDatabase `yaml:"controlDatabase"`
	Databases       []Database      `yaml:"databases"`
	Syncs           []Sync          `yaml:"syncs"`
}

type ControlDatabase struct {
	DSN string `yaml:"dsn"`
}

type Database struct {
	Name string `yaml:"name"`
	Role string `yaml:"role"`
	DSN  string `yaml:"dsn"`
}

type Sync struct {
	Name             string   `yaml:"name"`
	Source           string   `yaml:"source"`
	Sources          []string `yaml:"sources"`
	Target           string   `yaml:"target"`
	Targets          []string `yaml:"targets"`
	DeleteMethod     string   `yaml:"deleteMethod"`
	ConflictStrategy string   `yaml:"conflictStrategy"`
	SourcePriority   []string `yaml:"sourcePriority"`
	Tables           []Table  `yaml:"tables"`
}

type Table struct {
	Schema     string   `yaml:"schema"`
	Name       string   `yaml:"name"`
	PrimaryKey []string `yaml:"primaryKey"`
}

// Domain converts the YAML database DTO into the validated domain endpoint.
func (d Database) Domain() (domainDatabase.Database, error) {
	return domainDatabase.New(d.Name, domainDatabase.TypePostgreSQL, d.DSN, domainDatabase.StatusActive, true)
}

func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config %q: %w", path, err)
	}
	var cfg Config
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("parse YAML config %q: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// WriteTemplate creates a new multi-source/multi-target configuration template.
// O_EXCL is intentional: init must never overwrite an existing policy file.
func WriteTemplate(path string) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create config template %q: %w", path, err)
	}
	defer file.Close()
	if _, err := file.WriteString(template); err != nil {
		return fmt.Errorf("write config template %q: %w", path, err)
	}
	return nil
}

func (c Config) Validate() error {
	if c.ControlDatabase.DSN == "" {
		return fmt.Errorf("controlDatabase.dsn is required")
	}
	if len(c.Databases) == 0 {
		return fmt.Errorf("at least one database is required")
	}
	seen := make(map[string]bool)
	for _, db := range c.Databases {
		if !namePattern.MatchString(db.Name) {
			return fmt.Errorf("database name %q is invalid", db.Name)
		}
		if db.Role != "source" && db.Role != "target" {
			return fmt.Errorf("database %q role must be source or target", db.Name)
		}
		if db.DSN == "" {
			return fmt.Errorf("database %q dsn is required", db.Name)
		}
		if _, err := db.Domain(); err != nil {
			return err
		}
		if seen[db.Name] {
			return fmt.Errorf("database %q is duplicated", db.Name)
		}
		seen[db.Name] = true
	}
	for _, sync := range c.Syncs {
		if !namePattern.MatchString(sync.Name) {
			return fmt.Errorf("sync name %q is invalid", sync.Name)
		}
		sources := sync.SourceNames()
		if sync.Source != "" && len(sync.Sources) > 0 {
			return fmt.Errorf("sync %q must use source or sources, not both", sync.Name)
		}
		if len(sources) == 0 {
			return fmt.Errorf("sync %q requires source or sources", sync.Name)
		}
		sourceSeen := make(map[string]bool)
		for _, source := range sources {
			if sourceSeen[source] {
				return fmt.Errorf("sync %q repeats source %q", sync.Name, source)
			}
			sourceSeen[source] = true
			if !seen[source] {
				return fmt.Errorf("sync %q references unknown source database %q", sync.Name, source)
			}
		}
		if sync.Target != "" && len(sync.Targets) > 0 {
			return fmt.Errorf("sync %q must use target or targets, not both", sync.Name)
		}
		targets := sync.TargetNames()
		if len(targets) == 0 {
			return fmt.Errorf("sync %q requires target or targets", sync.Name)
		}
		targetSeen := make(map[string]bool)
		for _, target := range targets {
			if targetSeen[target] {
				return fmt.Errorf("sync %q repeats target %q", sync.Name, target)
			}
			targetSeen[target] = true
			if !seen[target] {
				return fmt.Errorf("sync %q references unknown target database %q", sync.Name, target)
			}
			for _, source := range sources {
				if source == target {
					return fmt.Errorf("sync %q source and target must differ", sync.Name)
				}
			}
		}
		if sync.DeleteMethod == "" {
			sync.DeleteMethod = "delete"
		}
		if sync.DeleteMethod != "delete" && sync.DeleteMethod != "truncate" && sync.DeleteMethod != "truncate_cascade" {
			return fmt.Errorf("sync %q has unsupported deleteMethod %q", sync.Name, sync.DeleteMethod)
		}
		if sync.ConflictStrategy == "" {
			return fmt.Errorf("sync %q requires conflictStrategy", sync.Name)
		}
		if sync.ConflictStrategy != "abort" && sync.ConflictStrategy != "bucardo_abort" && sync.ConflictStrategy != "latest" && sync.ConflictStrategy != "source_priority" {
			return fmt.Errorf("sync %q has unsupported conflictStrategy %q", sync.Name, sync.ConflictStrategy)
		}
		if sync.ConflictStrategy == "source_priority" {
			if len(sync.SourcePriority) != len(sources) {
				return fmt.Errorf("sync %q sourcePriority must list every source exactly once", sync.Name)
			}
			prioritySeen := make(map[string]bool)
			for _, source := range sync.SourcePriority {
				if !sourceSeen[source] || prioritySeen[source] {
					return fmt.Errorf("sync %q sourcePriority references unknown or duplicate source %q", sync.Name, source)
				}
				prioritySeen[source] = true
			}
		}
		if len(sync.Tables) == 0 {
			return fmt.Errorf("sync %q requires at least one table", sync.Name)
		}
		for _, item := range sync.Tables {
			if item.Schema == "" || item.Name == "" || len(item.PrimaryKey) == 0 {
				return fmt.Errorf("sync %q tables require schema, name, and primaryKey", sync.Name)
			}
		}
	}
	return nil
}

func (s Sync) TargetGroupName() string { return s.Name + "_targets" }

func (s Sync) TargetNames() []string {
	if len(s.Targets) > 0 {
		return append([]string(nil), s.Targets...)
	}
	if s.Target == "" {
		return nil
	}
	return []string{s.Target}
}

func (s Sync) SourceNames() []string {
	if len(s.Sources) > 0 {
		return append([]string(nil), s.Sources...)
	}
	if s.Source == "" {
		return nil
	}
	return []string{s.Source}
}

func (s Sync) DomainTables(databaseName string) []table.Table {
	result := make([]table.Table, 0, len(s.Tables))
	for index, item := range s.Tables {
		result = append(result, table.Table{ID: index + 1, Database: databaseName, Schema: item.Schema, Name: item.Name, Relation: table.RelationTable, PrimaryKey: item.PrimaryKey})
	}
	return result
}
