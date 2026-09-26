package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/bucardo-g/internal/config"
	"github.com/bucardo-g/internal/control"
	"github.com/bucardo-g/internal/logging"
	"github.com/bucardo-g/internal/replication"
	"github.com/spf13/cobra"
)

var (
	logLevel  string
	logFormat string
)

func main() {
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
			_, err := logging.Configure(logLevel, logFormat, os.Stderr)
			return err
		},
	}
	root.PersistentFlags().StringVar(&logLevel, "log-level", "info", "log level: debug, info, warn, or error")
	root.PersistentFlags().StringVar(&logFormat, "log-format", "json", "log format: json or text")
	root.AddCommand(newInitCommand())
	root.AddCommand(newApplyCommand())
	root.AddCommand(newRunCommand())
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
	logger := slog.Default()
	logger.Info("sync run started", "sync", syncName)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	store, err := control.Open(ctx, controlDSN)
	if err != nil {
		return err
	}
	defer store.Close()
	topology, err := store.LoadSync(ctx, syncName)
	if err != nil {
		return err
	}
	logger.Debug("sync topology loaded", "sync", topology.Name, "source", topology.Source.Name, "target", topology.Target.Name, "tables", len(topology.Tables))
	lock, err := store.TryLockSync(ctx, topology.Name)
	if err != nil {
		return err
	}
	defer func() {
		if err := lock.Close(); err != nil && runErr == nil {
			runErr = fmt.Errorf("release sync lock: %w", err)
		}
	}()

	started := time.Now()
	stats, err := replication.RunOnce(ctx, topology)
	status := statusForStats(stats)
	details := fmt.Sprintf("source=%s target=%s", topology.Source.Name, topology.Target.Name)
	if err != nil {
		status = "bad"
		details = err.Error()
		logger.Error("sync run failed", "sync", topology.Name, "error", err, "inserts", stats.Inserts, "updates", stats.Updates, "deletes", stats.Deletes)
	}
	if recordErr := store.RecordRun(ctx, topology.Name, status, details, stats.Inserts, stats.Deletes, started); recordErr != nil && err == nil {
		return recordErr
	}
	fmt.Printf("sync=%s status=%s inserts=%d updates=%d deletes=%d\n",
		topology.Name, status, stats.Inserts, stats.Updates, stats.Deletes)
	if err != nil {
		return err
	}
	logger.Info("sync run completed", "sync", topology.Name, "status", status, "inserts", stats.Inserts, "updates", stats.Updates, "deletes", stats.Deletes)
	return nil
}

func statusForStats(stats replication.Stats) string {
	if stats.Inserts == 0 && stats.Updates == 0 && stats.Deletes == 0 {
		return "empty"
	}
	return "good"
}
