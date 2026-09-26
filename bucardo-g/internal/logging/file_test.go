package logging

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDailyFileWriterCreatesDatedFile(t *testing.T) {
	directory := t.TempDir()
	writer, err := NewDailyFileWriter(filepath.Join(directory, "bucardo-g.log"), 7)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("test log\n")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || !strings.HasPrefix(entries[0].Name(), "bucardo-g-") || filepath.Ext(entries[0].Name()) != ".log" {
		t.Fatalf("unexpected rolling log files: %+v", entries)
	}
}
