package logging

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type DailyFileWriter struct {
	mu         sync.Mutex
	basePath   string
	retention  int
	location   *time.Location
	currentDay string
	file       *os.File
}

func NewDailyFileWriter(basePath string, retentionDays int) (*DailyFileWriter, error) {
	if basePath == "" {
		return nil, fmt.Errorf("log file path is required")
	}
	if retentionDays < 0 {
		return nil, fmt.Errorf("log retention days must not be negative")
	}
	writer := &DailyFileWriter{
		basePath:  basePath,
		retention: retentionDays,
		location:  time.Local,
	}
	if err := writer.rotate(time.Now()); err != nil {
		return nil, err
	}
	return writer, nil
}

func (w *DailyFileWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.ensureDay(time.Now()); err != nil {
		return 0, err
	}
	return w.file.Write(data)
}

func (w *DailyFileWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

func (w *DailyFileWriter) ensureDay(now time.Time) error {
	day := now.In(w.location).Format("2006-01-02")
	if day == w.currentDay && w.file != nil {
		return nil
	}
	return w.rotate(now)
}

func (w *DailyFileWriter) rotate(now time.Time) error {
	if w.file != nil {
		if err := w.file.Close(); err != nil {
			return fmt.Errorf("close log file: %w", err)
		}
	}
	day := now.In(w.location).Format("2006-01-02")
	path := datedPath(w.basePath, day)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return fmt.Errorf("create log directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o640)
	if err != nil {
		return nilIfFileError(path, err)
	}
	w.file = file
	w.currentDay = day
	if err := w.cleanup(now); err != nil {
		return err
	}
	return nil
}

func (w *DailyFileWriter) cleanup(now time.Time) error {
	if w.retention == 0 {
		return nil
	}
	entries, err := os.ReadDir(filepath.Dir(w.basePath))
	if err != nil {
		return fmt.Errorf("read log directory: %w", err)
	}
	prefix, suffix := datedPattern(w.basePath)
	cutoff := now.In(w.location).AddDate(0, 0, -w.retention).Format("2006-01-02")
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), prefix) || !strings.HasSuffix(entry.Name(), suffix) {
			continue
		}
		day := strings.TrimSuffix(strings.TrimPrefix(entry.Name(), prefix), suffix)
		if len(day) != len("2006-01-02") || day >= cutoff {
			continue
		}
		if err := os.Remove(filepath.Join(filepath.Dir(w.basePath), entry.Name())); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove expired log %q: %w", entry.Name(), err)
		}
	}
	return nil
}

func datedPath(basePath, day string) string {
	extension := filepath.Ext(basePath)
	stem := strings.TrimSuffix(basePath, extension)
	return stem + "-" + day + extension
}

func datedPattern(basePath string) (string, string) {
	extension := filepath.Ext(basePath)
	stem := filepath.Base(strings.TrimSuffix(basePath, extension))
	return stem + "-", extension
}

func nilIfFileError(path string, err error) error {
	return fmt.Errorf("open log file %q: %w", path, err)
}

var _ io.WriteCloser = (*DailyFileWriter)(nil)
