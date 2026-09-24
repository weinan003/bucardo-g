package control

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestTryLockSyncSerializesSameSync(t *testing.T) {
	dsn := os.Getenv("BUCARDO_TEST_CONTROL_DSN")
	if dsn == "" {
		t.Skip("BUCARDO_TEST_CONTROL_DSN is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	first, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()

	syncName := fmt.Sprintf("lock-test-%d", time.Now().UnixNano())
	lock, err := first.TryLockSync(ctx, syncName)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()

	unexpectedLock, err := second.TryLockSync(ctx, syncName)
	if err == nil {
		_ = unexpectedLock.Close()
		t.Fatal("second connection acquired a lock already held for the sync")
	}

	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
	lock, err = second.TryLockSync(ctx, syncName)
	if err != nil {
		t.Fatalf("lock was not released: %v", err)
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
}
