package config

import (
	"bytes"
	"fmt"
	"os"
	"regexp"

	"github.com/bucardo-g/internal/domain/table"
	"gopkg.in/yaml.v3"
)

const template = `controlDatabase:
  dsn: postgres://user:password@127.0.0.1:5432/bucardo?sslmode=disable

databases:
  - name: source
    role: source
    dsn: postgres://user:password@127.0.0.1:5432/source?sslmode=disable
  - name: target
    role: target
    dsn: postgres://user:password@127.0.0.1:5433/target?sslmode=disable

syncs:
  - name: example_sync
    source: source
    target: target
    deleteMethod: delete
    tables:
      - schema: public
        name: example_table
        primaryKey:
          - id
`

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
	Name         string  `yaml:"name"`
	Source       string  `yaml:"source"`
	Target       string  `yaml:"target"`
	DeleteMethod string  `yaml:"deleteMethod"`
	Tables       []Table `yaml:"tables"`
}

type Table struct {
	Schema     string   `yaml:"schema"`
	Name       string   `yaml:"name"`
	PrimaryKey []string `yaml:"primaryKey"`
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
		if seen[db.Name] {
			return fmt.Errorf("database %q is duplicated", db.Name)
		}
		seen[db.Name] = true
	}
	for _, sync := range c.Syncs {
		if !namePattern.MatchString(sync.Name) {
			return fmt.Errorf("sync name %q is invalid", sync.Name)
		}
		if !seen[sync.Source] || !seen[sync.Target] {
			return fmt.Errorf("sync %q references an unknown database", sync.Name)
		}
		if sync.Source == sync.Target {
			return fmt.Errorf("sync %q source and target must differ", sync.Name)
		}
		if sync.DeleteMethod == "" {
			sync.DeleteMethod = "delete"
		}
		if sync.DeleteMethod != "delete" && sync.DeleteMethod != "truncate" && sync.DeleteMethod != "truncate_cascade" {
			return fmt.Errorf("sync %q has unsupported deleteMethod %q", sync.Name, sync.DeleteMethod)
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

func (s Sync) TargetGroupName() string {
	return s.Name + "_targets"
}

func (s Sync) DomainTables(databaseName string) []table.Table {
	result := make([]table.Table, 0, len(s.Tables))
	for index, item := range s.Tables {
		result = append(result, table.Table{ID: index + 1, Database: databaseName, Schema: item.Schema, Name: item.Name, Relation: table.RelationTable, PrimaryKey: item.PrimaryKey})
	}
	return result
}
