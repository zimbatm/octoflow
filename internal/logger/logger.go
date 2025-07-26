package logger

import (
	"log/slog"
	"os"
)

var Logger *slog.Logger

func init() {
	// Initialize with JSON handler for structured logging
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}

	// Check if we should use text handler for development
	if os.Getenv("LOG_FORMAT") == "text" {
		Logger = slog.New(slog.NewTextHandler(os.Stdout, opts))
	} else {
		Logger = slog.New(slog.NewJSONHandler(os.Stdout, opts))
	}

	// Set as default logger
	slog.SetDefault(Logger)
}

// SetLevel updates the logging level
func SetLevel(level string) {
	var slogLevel slog.Level
	switch level {
	case "debug":
		slogLevel = slog.LevelDebug
	case "info":
		slogLevel = slog.LevelInfo
	case "warn":
		slogLevel = slog.LevelWarn
	case "error":
		slogLevel = slog.LevelError
	default:
		slogLevel = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level: slogLevel,
	}

	if os.Getenv("LOG_FORMAT") == "text" {
		Logger = slog.New(slog.NewTextHandler(os.Stdout, opts))
	} else {
		Logger = slog.New(slog.NewJSONHandler(os.Stdout, opts))
	}

	slog.SetDefault(Logger)
}
