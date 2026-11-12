package pgfallback_test

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/djosh34/pgfallback"
)

// The bound after which no read may be served by a read address once an rw
// proxy accepts again, when the rw proxies refuse.
const backBound = 2 * time.Second

// Every rw proxy refuses and the primary's read proxy is gone, as when the
// primary's node is lost.
func TestNoPrimary(t *testing.T) {
	eachKind(t, kinds, func(t *testing.T, e *env, open func(func(*pgx.ConnConfig)) client) {
		e.refuseRW()
		e.read[0].refuse()
		c := open(nil)
		for range 5 {
			s, err := c.read(t.Context(), 0)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if !onStandby(s) {
				t.Errorf("read served by %s (%s), want a standby's read address", s.proxy, s.server)
			}
		}
		s, err := c.write(t.Context())
		if !pgfallback.NoPrimary(err) {
			t.Fatalf("write on %s: %v, want SQLSTATE 25006", s.proxy, err)
		}
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "25006" {
			t.Errorf("write error %v, want a *pgconn.PgError with code 25006", err)
		}
	})
}

// Every rw proxy refuses, and the primary's read proxy comes first in the
// read list: the fallback lands on the primary, and writes work there.
func TestReadPortOfPrimary(t *testing.T) {
	eachKind(t, kinds, func(t *testing.T, e *env, open func(func(*pgx.ConnConfig)) client) {
		e.refuseRW()
		c := open(nil)
		s, err := c.read(t.Context(), 0)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if s.proxy != "read-primary" || s.server != "primary" {
			t.Errorf("read served by %s (%s), want read-primary (primary)", s.proxy, s.server)
		}
		s, err = c.write(t.Context())
		if err != nil {
			t.Fatalf("write: %v", err)
		}
		if s.proxy != "read-primary" {
			t.Errorf("write served by %s, want read-primary", s.proxy)
		}
	})
}

// One rw proxy accepts again: within 2 s no read is served by a read
// address, and writes work.
func TestBackToRW(t *testing.T) {
	eachKind(t, kinds, func(t *testing.T, e *env, open func(func(*pgx.ConnConfig)) client) {
		e.refuseRW()
		e.read[0].refuse()
		c := open(nil)
		fill(t, c)
		back := time.Now()
		e.rw[1].up()
		checkBack(t, load(c, 4*time.Second), back, backBound)
		s, err := c.write(t.Context())
		if err != nil || !s.onRW() {
			t.Errorf("write after rw came back: served by %s, %v", s.proxy, err)
		}
	})
}

// The first rw address does not answer while the second comes back: the
// bound grows by connect_timeout.
func TestBackToRWFirstSilent(t *testing.T) {
	eachKind(t, kinds, func(t *testing.T, e *env, open func(func(*pgx.ConnConfig)) client) {
		e.rw[0].silence()
		e.rw[1].refuse()
		e.rw[2].refuse()
		e.read[0].refuse()
		c := open(nil)
		fill(t, c)
		back := time.Now()
		e.rw[1].up()
		checkBack(t, load(c, 7*time.Second), back, backBound+connectTimeout)
	})
}

// rw comes back while the client is idle: its first write after that goes to
// rw, although no attempt at rw ran in the meantime.
func TestBackToRWWhileIdle(t *testing.T) {
	eachKind(t, kinds, func(t *testing.T, e *env, open func(func(*pgx.ConnConfig)) client) {
		e.refuseRW()
		e.read[0].refuse()
		c := open(nil)
		fill(t, c)
		e.rw[1].up()
		// Longer than the quiet spell after which a read connection is
		// replaced: two seconds after the last attempt ended.
		time.Sleep(3 * time.Second)
		s, err := c.write(t.Context())
		if err != nil || !s.onRW() {
			t.Errorf("first write after an idle spell: served by %s, %v", s.proxy, err)
		}
	})
}

// The application's BeforeConnect stalls during an attempt at rw until its
// context ends: the hook gets connect_timeout, and the next attempt finds rw
// back.
func TestStalledAttempt(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.refuseRW()
	e.read[0].refuse()
	cfg, err := pgxpool.ParseConfig(e.dsn())
	if err != nil {
		t.Fatal(err)
	}
	var stall atomic.Bool
	cfg.BeforeConnect = func(ctx context.Context, _ *pgx.ConnConfig) error {
		if stall.Load() {
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}
	c := poolClient{e: e, pool: newPool(t, cfg, e)}
	fill(t, c)
	// The pool has its connections, so only an attempt connects now.
	stall.Store(true)
	time.Sleep(time.Second)
	if _, err := c.read(t.Context(), 0); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	stall.Store(false)
	back := time.Now()
	e.rw[1].up()
	checkBack(t, load(c, 6*time.Second), back, connectTimeout+backBound)
}

// One rw name resolves to two addresses, and the first does not answer:
// attempts wait connect_timeout on it and then reach the second.
func TestRWNameFirstAddressSilent(t *testing.T) {
	eachKind(t, kinds[:3], func(t *testing.T, e *env, open func(func(*pgx.ConnConfig)) client) {
		e.rw[0].silence()
		e.rw[1].refuse()
		e.read[0].refuse()
		c := open(func(cc *pgx.ConnConfig) {
			cc.Fallbacks = nil
			cc.LookupFunc = func(ctx context.Context, host string) ([]string, error) {
				if host == "localhost" {
					return []string{e.rw[0].addr(), e.rw[1].addr()}, nil
				}
				return net.DefaultResolver.LookupHost(ctx, host)
			}
		})
		fill(t, c)
		back := time.Now()
		e.rw[1].up()
		checkBack(t, load(c, 7*time.Second), back, backBound+connectTimeout)
	})
}

// A transaction that runs on a read connection when rw comes back finishes
// on that connection, which closes once it is released.
func TestTransactionFinishesFirst(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.refuseRW()
	e.read[0].refuse()
	c, ok := openPool(t, e, nil).(poolClient)
	if !ok {
		t.Fatal("openPool returned no poolClient")
	}
	fill(t, c)

	conn, err := c.pool.Acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	// The proxy sees the connection close; IsClosed may not be read while
	// the pool closes it.
	local := conn.Conn().PgConn().Conn().LocalAddr().String()
	through := e.byName(servedBy(e, conn.Conn()).proxy)
	tx, err := conn.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	inTx := func() {
		t.Helper()
		var server string
		if err := tx.QueryRow(t.Context(), "SELECT current_setting('cluster_name')").Scan(&server); err != nil {
			t.Fatalf("query in the transaction: %v", err)
		}
		if s := servedBy(e, conn.Conn()); !onStandby(served{proxy: s.proxy, server: server}) {
			t.Errorf("transaction on %s (%s), want a standby's read address", s.proxy, server)
		}
	}
	inTx()

	back := time.Now()
	e.rw[1].up()
	checkBack(t, load(c, 3*time.Second), back, backBound)
	inTx()
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if !through.hasClient(local) {
		t.Fatal("the read connection closed before its transaction finished")
	}
	conn.Release()
	deadline := time.Now().Add(time.Second)
	for through.hasClient(local) {
		if time.Now().After(deadline) {
			t.Fatal("the read connection is still open 1 s after its release")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// rw refuses again right after a switch back: reads go to read addresses
// again, on reused connections, and no checkout fails.
func TestRWGoneAgain(t *testing.T) {
	eachKind(t, kinds, func(t *testing.T, e *env, open func(func(*pgx.ConnConfig)) client) {
		e.refuseRW()
		e.read[0].refuse()
		c := open(nil)
		fill(t, c)
		back := time.Now()
		e.rw[1].up()
		checkBack(t, load(c, 3*time.Second), back, backBound)

		e.refuseRW()
		// Let the pools see that the rw connections are gone: they ping a
		// connection that was idle for more than 1 s.
		time.Sleep(1500 * time.Millisecond)
		before := e.readAccepted()
		for _, r := range load(c, 3*time.Second) {
			if r.err != nil {
				t.Errorf("read: %v", r.err)
			} else if !onStandby(r.s) {
				t.Errorf("read served by %s (%s), want a standby's read address", r.s.proxy, r.s.server)
			}
		}
		if n := e.readAccepted() - before; n > maxConns {
			t.Errorf("%d new connections on read addresses, want at most %d", n, maxConns)
		}
	})
}

// rw accepts TCP while every login fails with 57P03: connections stay on
// read and are not reopened every second, while attempts at rw go on.
func TestRWLoginsFail(t *testing.T) {
	eachKind(t, kinds, func(t *testing.T, e *env, open func(func(*pgx.ConnConfig)) client) {
		for _, p := range e.rw {
			p.setTarget(cluster.addr["cold"])
		}
		e.read[0].refuse()
		c := open(nil)
		fill(t, c)
		readBefore, rwBefore := e.readAccepted(), e.rwAccepted()
		for _, r := range load(c, 4*time.Second) {
			if r.err != nil {
				t.Errorf("read: %v", r.err)
			} else if !onStandby(r.s) {
				t.Errorf("read served by %s (%s), want a standby's read address", r.s.proxy, r.s.server)
			}
		}
		if n := e.readAccepted() - readBefore; n > 0 {
			t.Errorf("%d new connections on read addresses, want none", n)
		}
		// One attempt reaches each of the three rw proxies once.
		if n := e.rwAccepted() - rwBefore; n < 2*len(e.rw) {
			t.Errorf("%d connections to rw in 4 s, want attempts at rw going on", n)
		}
	})
}

// The first rw address does not answer: a new connection takes about
// connect_timeout and lands on the next one.
func TestRWSilent(t *testing.T) {
	eachKind(t, kinds, func(t *testing.T, e *env, open func(func(*pgx.ConnConfig)) client) {
		e.rw[0].silence()
		start := time.Now()
		c := open(nil)
		s, err := c.read(t.Context(), 0)
		took := time.Since(start)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if s.proxy != "rw2" {
			t.Errorf("read served by %s, want rw2", s.proxy)
		}
		if took < connectTimeout || took > connectTimeout+time.Second {
			t.Errorf("first read took %v, want about connect_timeout %v", took, connectTimeout)
		}
	})
}

// The read name resolves to another IP between connects: connections are
// still told apart by the IPs of their own lookup.
func TestLookupChanges(t *testing.T) {
	eachKind(t, kinds[:3], func(t *testing.T, e *env, open func(func(*pgx.ConnConfig)) client) {
		var ip atomic.Value
		ip.Store("127.0.0.1")
		changing := func(cc *pgx.ConnConfig) {
			cc.LookupFunc = func(ctx context.Context, host string) ([]string, error) {
				if host == "127.0.0.1" {
					v, ok := ip.Load().(string)
					if !ok {
						return nil, errors.New("no IP stored")
					}
					return []string{v}, nil
				}
				return net.DefaultResolver.LookupHost(ctx, host)
			}
		}
		e.refuseRW()
		e.read[0].refuse()
		c := open(changing)
		fillN(t, c, maxConns/2)
		ip.Store("127.0.0.2")
		ips := map[string]bool{}
		for _, s := range fill(t, c) {
			ips[s.ip] = true
		}
		if !ips["127.0.0.1"] || !ips["127.0.0.2"] {
			t.Errorf("read connections on %v, want both 127.0.0.1 and 127.0.0.2", ips)
		}
		back := time.Now()
		e.rw[1].up()
		checkBack(t, load(c, 4*time.Second), back, backBound)
	})
}

// An application that set only BeforeAcquire keeps it.
func TestBeforeAcquireKept(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.refuseRW()
	e.read[0].refuse()
	cfg, err := pgxpool.ParseConfig(e.dsn())
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	cfg.BeforeAcquire = func(context.Context, *pgx.Conn) bool {
		calls.Add(1)
		return true
	}
	c := poolClient{e: e, pool: newPool(t, cfg, e)}
	fill(t, c)
	back := time.Now()
	e.rw[1].up()
	checkBack(t, load(c, 3*time.Second), back, backBound)
	if calls.Load() == 0 {
		t.Error("the application's BeforeAcquire was never called")
	}
}

// An application whose own BeforeConnect sets the client certificate: the
// read addresses and the attempts at rw get it too. Its PrepareConn,
// AfterConnect and AfterRelease are kept.
func TestApplicationHooksKept(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.refuseRW()
	e.read[0].refuse()
	cfg, err := pgxpool.ParseConfig(e.dsnWithoutCert())
	if err != nil {
		t.Fatal(err)
	}
	cert, err := tls.LoadX509KeyPair(certFile("app.crt"), certFile("app.key"))
	if err != nil {
		t.Fatal(err)
	}
	var beforeConnect, afterConnect, prepare, afterRelease atomic.Int64
	cfg.BeforeConnect = func(_ context.Context, cc *pgx.ConnConfig) error {
		beforeConnect.Add(1)
		cc.TLSConfig.Certificates = []tls.Certificate{cert}
		for _, fb := range cc.Fallbacks {
			fb.TLSConfig.Certificates = []tls.Certificate{cert}
		}
		return nil
	}
	cfg.AfterConnect = func(context.Context, *pgx.Conn) error {
		afterConnect.Add(1)
		return nil
	}
	cfg.PrepareConn = func(context.Context, *pgx.Conn) (bool, error) {
		prepare.Add(1)
		return true, nil
	}
	cfg.AfterRelease = func(*pgx.Conn) bool {
		afterRelease.Add(1)
		return true
	}
	c := poolClient{e: e, pool: newPool(t, cfg, e)}
	fill(t, c)
	back := time.Now()
	e.rw[1].up()
	checkBack(t, load(c, 3*time.Second), back, backBound)
	for name, n := range map[string]int64{
		"BeforeConnect": beforeConnect.Load(), "AfterConnect": afterConnect.Load(),
		"PrepareConn": prepare.Load(), "AfterRelease": afterRelease.Load(),
	} {
		if n == 0 {
			t.Errorf("the application's %s was never called", name)
		}
	}
}

func TestConfigErrors(t *testing.T) {
	t.Parallel()
	good := fmt.Sprintf("host=localhost port=1 user=app sslmode=verify-full sslrootcert=%s connect_timeout=2", certFile("ca.crt"))
	for _, tc := range []struct {
		name  string
		dsn   string
		reads []string
		want  string
	}{
		{"no read addresses", good, nil, "no read addresses"},
		{"no port", good, []string{"vm1"}, "not host:port"},
		{"no host", good, []string{":5433"}, "not host:port"},
		{"port not a number", good, []string{"vm1:read"}, "not host:port"},
		{"port 0", good, []string{"vm1:0"}, "not host:port"},
		{"port too big", good, []string{"vm1:65536"}, "not host:port"},
		{"unix socket", good, []string{"/var/run/postgresql:5433"}, "not host:port"},
		{"sslmode disable", strings.Replace(good, "verify-full", "disable", 1), []string{"vm1:5433"}, "TLS"},
		{"sslmode prefer", strings.Replace(good, "verify-full", "prefer", 1), []string{"vm1:5433"}, "TLS"},
		{"no connect_timeout", strings.Replace(good, " connect_timeout=2", "", 1), []string{"vm1:5433"}, "connect_timeout"},
		{"target_session_attrs read-write", good + " target_session_attrs=read-write", []string{"vm1:5433"}, "target_session_attrs"},
		{"target_session_attrs prefer-standby", good + " target_session_attrs=prefer-standby", []string{"vm1:5433"}, "target_session_attrs"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg, err := pgxpool.ParseConfig(tc.dsn)
			if err != nil {
				t.Fatal(err)
			}
			checkErr(t, "Pool", pgfallback.Pool(cfg, tc.reads...), tc.want)

			// Stdlib sees the config only when it connects.
			opts, err := pgfallback.Stdlib(tc.reads...)
			if tc.want == "no read addresses" || tc.want == "not host:port" {
				checkErr(t, "Stdlib", err, tc.want)
				return
			}
			if err != nil {
				t.Fatalf("Stdlib: %v", err)
			}
			cc, err := pgx.ParseConfig(tc.dsn)
			if err != nil {
				t.Fatal(err)
			}
			db := stdlib.OpenDB(*cc, opts...)
			checkErr(t, "connect through Stdlib", db.PingContext(t.Context()), tc.want)
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		})
	}
}

func checkErr(t *testing.T, what string, err error, want string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), "pgfallback: ") || !strings.Contains(err.Error(), want) {
		t.Errorf("%s: error %v, want a pgfallback error about %s", what, err, want)
	}
}

func TestNoPrimaryError(t *testing.T) {
	t.Parallel()
	readOnly := &pgconn.PgError{Code: "25006"}
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{nil, false},
		{errors.New("25006"), false},
		{&pgconn.PgError{Code: "0A000"}, false},
		{readOnly, true},
		{fmt.Errorf("insert: %w", readOnly), true},
	} {
		if got := pgfallback.NoPrimary(tc.err); got != tc.want {
			t.Errorf("NoPrimary(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}
