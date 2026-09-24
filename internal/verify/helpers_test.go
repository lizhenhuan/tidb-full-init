package verify

import (
	"context"
	"database/sql"
	"os"
	"testing"

	goorav2 "github.com/sijms/go-ora/v2"
)

func openRaw(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("ping mysql: %v", err)
	}
	return db
}

func exec(t *testing.T, db *sql.DB, q string) {
	t.Helper()
	if _, err := db.Exec(q); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

func openOracle(t *testing.T, ctx context.Context) *sql.DB {
	t.Helper()
	url := goorav2.BuildUrl(
		os.Getenv("ORACLE_FREE_HOST"), 1521,
		envOr("ORACLE_FREE_SERVICE", "FREEPDB1"),
		envOr("ORACLE_FREE_USER", "system"),
		os.Getenv("ORACLE_FREE_PASSWORD"), nil)
	db, err := sql.Open("oracle", url)
	if err != nil {
		t.Fatalf("open oracle: %v", err)
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping oracle: %v", err)
	}
	return db
}

func oexec(t *testing.T, db *sql.DB, q string) {
	t.Helper()
	if _, err := db.Exec(q); err != nil {
		t.Fatalf("exec oracle %q: %v", q, err)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
