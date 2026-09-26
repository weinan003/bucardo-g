package logging

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
)

type nopCloser struct{}

func (nopCloser) Close() error { return nil }

func Configure(levelName, format string, output io.Writer) (*slog.Logger, error) {
	level, err := parseLevel(levelName)
	if err != nil {
		return nil, err
	}
	options := &slog.HandlerOptions{Level: level}
	if output == nil {
		output = os.Stderr
	}
	var handler slog.Handler
	switch strings.ToLower(format) {
	case "json", "":
		handler = slog.NewJSONHandler(output, options)
	case "text":
		handler = slog.NewTextHandler(output, options)
	default:
		return nil, fmt.Errorf("unsupported log format %q; use json or text", format)
	}
	logger := slog.New(handler)
	slog.SetDefault(logger)
	return logger, nil
}

func ConfigureOutput(levelName, format, filePath string, retentionDays int) (*slog.Logger, io.Closer, error) {
	if filePath == "" {
		logger, err := Configure(levelName, format, os.Stderr)
		return logger, nopCloser{}, err
	}
	writer, err := NewDailyFileWriter(filePath, retentionDays)
	if err != nil {
		return nil, nil, err
	}
	logger, err := Configure(levelName, format, writer)
	if err != nil {
		_ = writer.Close()
		return nil, nil, err
	}
	return logger, writer, nil
}

func parseLevel(value string) (slog.Level, error) {
	switch strings.ToLower(value) {
	case "", "info":
		return slog.LevelInfo, nil
	case "debug":
		return slog.LevelDebug, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("unsupported log level %q; use debug, info, warn, or error", value)
	}
}
