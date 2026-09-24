package table

import "testing"

func TestNewTableRequiresPrimaryKey(t *testing.T) {
	if _, err := New(1, "source", "public", "items", RelationTable, nil); err == nil {
		t.Fatal("expected missing primary key to fail")
	}
}

func TestNewSequenceDoesNotRequirePrimaryKey(t *testing.T) {
	got, err := New(1, "source", "public", "items_id_seq", RelationSequence, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Relation != RelationSequence {
		t.Fatalf("unexpected relation type: %q", got.Relation)
	}
}
