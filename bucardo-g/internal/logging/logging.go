package logging

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
)

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
