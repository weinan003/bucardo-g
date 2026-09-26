// Package control owns Bucardo control-database access, configuration projection,
// run history, advisory locks, and PostgreSQL notifications.
package control

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

const notificationChannel = "bucardo"
const kickPrefix = "kick_sync_"

func NotifyKick(ctx context.Context, dsn, syncName string) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect for kick: %w", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `SELECT pg_notify($1, $2)`, notificationChannel, kickPrefix+syncName); err != nil {
		return fmt.Errorf("send kick notification: %w", err)
	}
	return nil
}

func ListenKicks(ctx context.Context, dsn string, handle func(context.Context, string) error) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect for notifications: %w", err)
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(ctx, `LISTEN `+notificationChannel); err != nil {
		return fmt.Errorf("listen for notifications: %w", err)
	}
	for {
		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("wait for notification: %w", err)
		}
		if !strings.HasPrefix(notification.Payload, kickPrefix) {
			continue
		}
		syncName := strings.TrimPrefix(notification.Payload, kickPrefix)
		if syncName == "" {
			continue
		}
		if err := handle(ctx, syncName); err != nil {
			return err
		}
	}
}
