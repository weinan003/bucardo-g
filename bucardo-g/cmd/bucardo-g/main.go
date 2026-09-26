// Command bucardo-g is the cross-platform CLI for configuration, one-shot runs,
// manual kicks, and the notification-driven serve loop.
package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bucardo-g/internal/config"
	"github.com/bucardo-g/internal/control"
	"github.com/bucardo-g/internal/domain/job"
	"github.com/bucardo-g/internal/logging"
	"github.com/bucardo-g/internal/replication"
	"github.com/spf13/cobra"
)

var (
	logLevel         string
	logFormat        string
	logFile          string
	logRetentionDays int
	logFileCloser    io.Closer
)

func main() {
	defer func() {
		if logFileCloser != nil {
			_ = logFileCloser.Close()
		}
	}()
	if err := newRootCommand().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func newRootCommand() *cobra.Command {
	root := &cobra.Command{
		Use:   "bucardo-g",
		Short: "PostgreSQL replication controller",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			_, closer, err := logging.ConfigureOutput(logLevel, logFormat, logFile, logRetentionDays)
			if err == nil {
				logFileCloser = closer
			}
			return err
		},
	}
	root.PersistentFlags().StringVar(&logLevel, "log-level", "info", "log level: debug, info, warn, or error")
	root.PersistentFlags().StringVar(&logFormat, "log-format", "json", "log format: json or text")
	root.PersistentFlags().StringVar(&logFile, "log-file", "", "daily rolling log file path; empty writes to stderr")
	root.PersistentFlags().IntVar(&logRetentionDays, "log-retention-days", 7, "number of days of rolling logs to retain; 0 disables cleanup")
	root.AddCommand(newInitCommand())
	root.AddCommand(newApplyCommand())
	root.AddCommand(newRunCommand())
	root.AddCommand(newKickCommand())
	root.AddCommand(newServeCommand())
	return root
}

func newApplyCommand() *cobra.Command {
	apply := &cobra.Command{
		Use:   "apply <config-file>",
		Short: "Apply YAML configuration to the control database",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return applyConfig(args[0])
		},
	}
	return apply
}

func newInitCommand() *cobra.Command {
	initCommand := &cobra.Command{
		Use:   "init <config-file>",
		Short: "Create a YAML configuration template",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := config.WriteTemplate(args[0]); err != nil {
				return err
			}
			fmt.Printf("configuration template created: %s\n", args[0])
			return nil
		},
	}
	return initCommand
}

func newRunCommand() *cobra.Command {
	var syncName string
	runCommand := &cobra.Command{
		Use:   "run <config-file>",
		Short: "Run one configured Sync",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if syncName == "" {
				return fmt.Errorf("sync is required")
			}
			cfg, err := config.Load(args[0])
			if err != nil {
				return err
			}
			return run(cfg.ControlDatabase.DSN, syncName)
		},
	}
	runCommand.Flags().StringVar(&syncName, "sync", "", "name of the active sync to run")
	return runCommand
}

func newKickCommand() *cobra.Command {
	var syncName string
	kick := &cobra.Command{
		Use:   "kick <config-file>",
		Short: "Send a manual kick notification for one Sync",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if syncName == "" {
				return fmt.Errorf("sync is required")
			}
			cfg, err := config.Load(args[0])
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := control.NotifyKick(ctx, cfg.ControlDatabase.DSN, syncName); err != nil {
				return err
			}
			fmt.Printf("kick sent: sync=%s\n", syncName)
			return nil
		},
	}
	kick.Flags().StringVar(&syncName, "sync", "", "name of the Sync to kick")
	return kick
}

func newServeCommand() *cobra.Command {
	serve := &cobra.Command{
		Use:   "serve <config-file>",
		Short: "Listen for kick notifications and run configured Syncs",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.Load(args[0])
			if err != nil {
				return err
			}
			if err := applyConfig(args[0]); err != nil {
				return err
			}
			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			logger := slog.Default()
			configured := make(map[string]bool, len(cfg.Syncs))
			for _, syncConfig := range cfg.Syncs {
				configured[syncConfig.Name] = true
			}
			logger.Info("serve started")
			return control.ListenKicks(ctx, cfg.ControlDatabase.DSN, func(ctx context.Context, syncName string) error {
				if !configured[syncName] {
					logger.Warn("ignoring kick for unconfigured sync", "sync", syncName)
					return nil
				}
				runCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
				defer cancel()
				if err := runWithContext(runCtx, cfg.ControlDatabase.DSN, syncName); err != nil {
					logger.Error("kick sync failed", "sync", syncName, "error", err)
				}
				return nil
			})
		},
	}
	return serve
}

func applyConfig(configPath string) error {
	logger := slog.Default()
	logger.Info("applying configuration", "config_file", configPath)
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := control.Open(ctx, cfg.ControlDatabase.DSN)
	if err != nil {
		return err
	}
	defer store.Close()
	if err := store.ApplyConfig(ctx, cfg); err != nil {
		return err
	}
	logger.Info("configuration applied", "databases", len(cfg.Databases), "syncs", len(cfg.Syncs))
	fmt.Printf("configuration applied: databases=%d syncs=%d\n", len(cfg.Databases), len(cfg.Syncs))
	return nil
}

func run(controlDSN, syncName string) (runErr error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	return runWithContext(ctx, controlDSN, syncName)
}

func runWithContext(ctx context.Context, controlDSN, syncName string) (runErr error) {
	// The lock covers topology loading through replication and run recording, so
	// two controllers cannot consume the same Sync concurrently.
	logger := slog.Default()
	logger.Info("sync run started", "sync", syncName)
	store, err := control.Open(ctx, controlDSN)
	if err != nil {
		return err
	}
	defer store.Close()
	runtimeTopology, err := store.LoadTopology(ctx, syncName)
	if err != nil {
		return err
	}
	logger.Debug("sync topology loaded", "sync", runtimeTopology.Name, "sources", len(runtimeTopology.Sources), "targets", len(runtimeTopology.Targets), "tables", len(runtimeTopology.Tables))
	lock, err := store.TryLockSync(ctx, runtimeTopology.Name)
	if err != nil {
		return err
	}
	defer func() {
		if err := lock.Close(); err != nil && runErr == nil {
			runErr = fmt.Errorf("release sync lock: %w", err)
		}
	}()

	started := time.Now()
	runJob, err := job.Start(runtimeTopology.Name, started)
	if err != nil {
		return err
	}
	stats, err := replication.RunTopology(ctx, runtimeTopology)
	status := statusForStats(stats)
	runJob.Inserts = stats.Inserts
	runJob.Updates = stats.Updates
	runJob.Deletes = stats.Deletes
	details := fmt.Sprintf("sources=%d targets=%d", len(runtimeTopology.Sources), len(runtimeTopology.Targets))
	if err != nil {
		status = "bad"
		details = err.Error()
		logger.Error("sync run failed", "sync", runtimeTopology.Name, "error", err, "inserts", stats.Inserts, "updates", stats.Updates, "deletes", stats.Deletes)
	}
	if finishErr := runJob.Finish(job.Status(status), time.Now(), err); finishErr != nil {
		return finishErr
	}
	if recordErr := store.RecordRun(ctx, runJob, details); recordErr != nil && err == nil {
		return recordErr
	}
	fmt.Printf("sync=%s status=%s inserts=%d updates=%d deletes=%d\n",
		runtimeTopology.Name, status, stats.Inserts, stats.Updates, stats.Deletes)
	if err != nil {
		return err
	}
	logger.Info("sync run completed", "sync", runtimeTopology.Name, "status", status, "inserts", stats.Inserts, "updates", stats.Updates, "deletes", stats.Deletes)
	return nil
}

func statusForStats(stats replication.Stats) string {
	if stats.Inserts == 0 && stats.Updates == 0 && stats.Deletes == 0 {
		return "empty"
	}
	return "good"
}
