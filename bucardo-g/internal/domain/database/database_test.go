package database

import "testing"

func TestNew(t *testing.T) {
	t.Parallel()

	endpoint, err := New(
		"source_a",
		TypePostgreSQL,
		"postgres://bucardo:secret@localhost:5432/app",
		StatusActive,
		true,
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if endpoint.Type != TypePostgreSQL {
		t.Errorf("Type = %q, want %q", endpoint.Type, TypePostgreSQL)
	}
	if !endpoint.MakeDelta {
		t.Error("MakeDelta = false, want true")
	}
}

func TestNewRejectsInvalidConfiguration(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name         string
		database     string
		databaseType Type
		connection   string
		status       Status
	}{
		{
			name:         "invalid database name",
			database:     "1source",
			databaseType: TypePostgreSQL,
			connection:   "postgres://localhost/app",
			status:       StatusActive,
		},
		{
			name:         "missing connection string",
			database:     "source",
			databaseType: TypePostgreSQL,
			status:       StatusActive,
		},
		{
			name:         "unsupported status",
			database:     "source",
			databaseType: TypePostgreSQL,
			connection:   "postgres://localhost/app",
			status:       Status("retired"),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			_, err := New(testCase.database, testCase.databaseType, testCase.connection, testCase.status, false)
			if err == nil {
				t.Fatal("New() error = nil, want validation error")
			}
		})
	}
}

func TestNewRejectsNonPostgreSQLType(t *testing.T) {
	t.Parallel()

	_, err := New("source", Type("mysql"), "postgres://localhost/app", StatusActive, false)
	if err == nil {
		t.Fatal("New() error = nil, want unsupported database type error")
	}
}
