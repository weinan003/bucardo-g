package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSyncSourcesAndTargets(t *testing.T) {
	cfg := Config{
		ControlDatabase: ControlDatabase{DSN: "postgres://control"},
		Databases: []Database{
			{Name: "a", Role: "source", DSN: "postgres://a"},
			{Name: "b", Role: "source", DSN: "postgres://b"},
			{Name: "c", Role: "target", DSN: "postgres://c"},
		},
		Syncs: []Sync{{
			Name:    "ab_to_c",
			Sources: []string{"a", "b"},
			Targets: []string{"c"},
			Tables:  []Table{{Schema: "public", Name: "items", PrimaryKey: []string{"id"}}},
		}},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if got := cfg.Syncs[0].SourceNames(); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("unexpected sources: %v", got)
	}
}

func TestSyncRejectsSourceAndSourcesTogether(t *testing.T) {
	cfg := Config{
		ControlDatabase: ControlDatabase{DSN: "postgres://control"},
		Databases:       []Database{{Name: "a", Role: "source", DSN: "postgres://a"}, {Name: "b", Role: "target", DSN: "postgres://b"}},
		Syncs:           []Sync{{Name: "invalid", Source: "a", Sources: []string{"a"}, Target: "b", Tables: []Table{{Schema: "public", Name: "items", PrimaryKey: []string{"id"}}}}},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected source/sources conflict")
	}
}

func TestWriteTemplateUsesMultiSourceShape(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bucardo.yaml")
	if err := WriteTemplate(path); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Syncs) != 1 || len(cfg.Syncs[0].Sources) != 2 || len(cfg.Syncs[0].Targets) != 2 {
		t.Fatalf("template does not use multi-source shape: %+v", cfg.Syncs)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
}
