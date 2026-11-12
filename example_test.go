package pgfallback_test

import (
	"context"
	"database/sql"
	"log"
	"net"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/djosh34/pgfallback"
)

// The examples of README.md. TestDocExamples checks that each Go
// block of the doc is in this file as it is, so the doc's code compiles.

const exampleDSN = "host=node1.example.com,node2.example.com,node3.example.com port=5432 dbname=orders user=orders " +
	"sslmode=verify-full sslrootcert=ca.crt sslcert=orders-app.crt sslkey=orders-app.key connect_timeout=2"

// connectPool opens a pgxpool.Pool that falls back to the read port.
func connectPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	// Set the pool's own hooks and limits on cfg before this call.
	err = pgfallback.Pool(cfg, "node1.example.com:5433", "node2.example.com:5433", "node3.example.com:5433")
	if err != nil {
		return nil, err
	}
	return pgxpool.NewWithConfig(ctx, cfg)
}

// connectDB opens a *sql.DB that falls back to the read port.
func connectDB(dsn string) (*sql.DB, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	opts, err := pgfallback.Stdlib("node1.example.com:5433", "node2.example.com:5433", "node3.example.com:5433")
	if err != nil {
		return nil, err
	}
	return stdlib.OpenDB(*cfg, opts...), nil
}

// connectGORM opens a *gorm.DB that falls back to the read port.
func connectGORM(dsn string) (*gorm.DB, error) {
	opts, err := pgfallback.Stdlib("node1.example.com:5433", "node2.example.com:5433", "node3.example.com:5433")
	if err != nil {
		return nil, err
	}
	return gorm.Open(postgres.New(postgres.Config{DSN: dsn, OptionOpenDB: opts}), &gorm.Config{})
}

func ExamplePool() {
	pool, err := connectPool(context.Background(), exampleDSN)
	if err != nil {
		log.Print(err)
		return
	}
	defer pool.Close()
}

// A pool whose TCP connects give up after 500 ms (README.md, A
// short dial timeout).
func ExamplePool_dialTimeout() {
	cfg, err := pgxpool.ParseConfig(exampleDSN)
	if err != nil {
		log.Print(err)
		return
	}
	cfg.ConnConfig.DialFunc = (&net.Dialer{Timeout: 500 * time.Millisecond}).DialContext
	err = pgfallback.Pool(cfg, "node1.example.com:5433", "node2.example.com:5433", "node3.example.com:5433")
	if err != nil {
		log.Print(err)
		return
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		log.Print(err)
		return
	}
	defer pool.Close()
}

func ExampleStdlib() {
	db, err := connectDB(exampleDSN)
	if err != nil {
		log.Print(err)
		return
	}
	if err := db.Close(); err != nil {
		log.Print(err)
	}
}

func ExampleStdlib_gorm() {
	db, err := connectGORM(exampleDSN)
	if err != nil {
		log.Print(err)
		return
	}
	sqlDB, err := db.DB()
	if err != nil {
		log.Print(err)
		return
	}
	if err := sqlDB.Close(); err != nil {
		log.Print(err)
	}
}

func TestDocExamples(t *testing.T) {
	doc, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	src, err := os.ReadFile("example_test.go")
	if err != nil {
		t.Fatal(err)
	}
	blocks := regexp.MustCompile("(?s)```go\n(.*?)```").FindAllSubmatch(doc, -1)
	if len(blocks) == 0 {
		t.Fatal("README.md has no Go block")
	}
	for _, b := range blocks {
		if !strings.Contains(string(src), string(b[1])) {
			t.Errorf("this Go block of README.md is not in example_test.go:\n%s", b[1])
		}
	}
}
