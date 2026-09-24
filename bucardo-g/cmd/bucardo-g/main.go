package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/bucardo-g/internal/config"
	"github.com/bucardo-g/internal/control"
	"github.com/bucardo-g/internal/replication"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "apply" {
		applyMain(os.Args[2:])
		return
	}
	controlDSN := flag.String("control-dsn", "", "PostgreSQL connection string for the Bucardo control database")
	syncName := flag.String("sync", "", "name of the active sync to run")
	flag.Parse()
	if *controlDSN == "" || *syncName == "" {
		fmt.Fprintln(os.Stderr, "usage: bucardo-g -control-dsn <dsn> -sync <name>")
		os.Exit(2)
	}

	if err := run(*controlDSN, *syncName); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func applyMain(args []string) {
	flags := flag.NewFlagSet("apply", flag.ExitOnError)
	configPath := flags.String("config", "", "YAML configuration file")
	flags.Parse(args)
	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "usage: bucardo-g apply -config <file.yaml>")
		os.Exit(2)
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := control.Open(ctx, cfg.ControlDatabase.DSN)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer store.Close()
	if err := store.ApplyConfig(ctx, cfg); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("configuration applied: databases=%d syncs=%d\n", len(cfg.Databases), len(cfg.Syncs))
}

func run(controlDSN, syncName string) (runErr error) {
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
	status := "good"
	details := fmt.Sprintf("source=%s target=%s", topology.Source.Name, topology.Target.Name)
	if err != nil {
		status = "bad"
		details = err.Error()
	}
	if recordErr := store.RecordRun(ctx, topology.Name, status, details, stats.Inserts, stats.Deletes, started); recordErr != nil && err == nil {
		return recordErr
	}
	if err != nil {
		return err
	}
	fmt.Printf("sync=%s status=%s inserts=%d updates=%d deletes=%d\n",
		topology.Name, status, stats.Inserts, stats.Updates, stats.Deletes)
	return nil
}
