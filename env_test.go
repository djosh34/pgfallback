package pgfallback_test

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/djosh34/pgfallback"
)

// The instances of testdata/cluster.sh: their published addresses and the
// directory with the CA and the client certificate of the user app.
var cluster struct {
	dir  string
	addr map[string]string
}

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	dir, err := os.MkdirTemp("", "pgfallback")
	if err != nil {
		log.Print(err)
		return 1
	}
	cluster.dir = dir
	lastPort.Store(rand.Int32N(10000))
	code := 1
	if err := clusterSh("up"); err != nil {
		log.Print(err)
	} else if err := readAddrs(); err != nil {
		log.Print(err)
	} else {
		code = m.Run()
	}
	if err := clusterSh("down"); err != nil {
		log.Print(err)
		code = 1
	}
	if err := os.RemoveAll(dir); err != nil {
		log.Print(err)
		code = 1
	}
	return code
}

func clusterSh(action string) error {
	cmd := exec.Command("bash", "testdata/cluster.sh", action, cluster.dir)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("cluster.sh %s: %w", action, err)
	}
	return nil
}

func readAddrs() (err error) {
	f, err := os.Open(filepath.Join(cluster.dir, "addrs"))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	cluster.addr = map[string]string{}
	s := bufio.NewScanner(f)
	for s.Scan() {
		name, addr, ok := strings.Cut(s.Text(), " ")
		if !ok {
			return fmt.Errorf("addrs: bad line %q", s.Text())
		}
		cluster.addr[name] = addr
	}
	return s.Err()
}

func certFile(name string) string {
	return filepath.Join(cluster.dir, name)
}

// A proxy stands for one rw or read address. It forwards to its target, or
// refuses connections, or accepts them and never answers. Each change ends
// the connections through it, as when its node goes away.
type proxy struct {
	t    *testing.T
	name string
	ips  []string
	port int
	wg   sync.WaitGroup

	mu       sync.Mutex
	target   string
	silent   bool
	lns      []net.Listener
	conns    map[net.Conn]bool
	accepted int
}

// Proxy ports come from below Linux's ephemeral ports (32768 and up), so
// while a proxy refuses, no other listener or client socket takes its port.
// Each test run starts at a random port, so two runs on one host rarely
// share ports.
var lastPort atomic.Int32

const firstPort = 20000

func newProxy(t *testing.T, name, target string, ips ...string) *proxy {
	p := &proxy{t: t, name: name, ips: ips, target: target, conns: map[net.Conn]bool{}}
	// A port some other program holds is skipped.
	var err error
	for range 100 {
		p.port = firstPort + int(lastPort.Add(1))
		if err = p.change(false, true); err == nil {
			break
		}
	}
	if err != nil {
		t.Fatalf("proxy %s: %v", name, err)
	}
	t.Cleanup(func() {
		p.refuse()
		p.wg.Wait()
	})
	return p
}

func (p *proxy) addr() string {
	return net.JoinHostPort(p.ips[0], strconv.Itoa(p.port))
}

func (p *proxy) up() {
	p.set(false, true)
}

func (p *proxy) silence() {
	p.set(true, true)
}

func (p *proxy) refuse() {
	p.set(false, false)
}

func (p *proxy) set(silent, listen bool) {
	if err := p.change(silent, listen); err != nil {
		p.t.Errorf("proxy %s: %v", p.name, err)
	}
}

func (p *proxy) setTarget(target string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.target = target
}

func (p *proxy) acceptedCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.accepted
}

// hasClient reports whether the client connection from addr is open.
func (p *proxy) hasClient(addr string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for c := range p.conns {
		if c.RemoteAddr().String() == addr {
			return true
		}
	}
	return false
}

func (p *proxy) change(silent, listen bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.silent = silent
	for c := range p.conns {
		p.closeConn(c)
	}
	clear(p.conns)
	if !listen {
		for _, ln := range p.lns {
			p.closeConn(ln)
		}
		p.lns = nil
		return nil
	}
	if p.lns != nil {
		return nil
	}
	var lns []net.Listener
	for _, ip := range p.ips {
		ln, err := net.Listen("tcp", net.JoinHostPort(ip, strconv.Itoa(p.port)))
		if err != nil {
			for _, l := range lns {
				p.closeConn(l)
			}
			return err
		}
		lns = append(lns, ln)
	}
	p.lns = lns
	for _, ln := range lns {
		p.wg.Go(func() { p.accept(ln) })
	}
	return nil
}

func (p *proxy) accept(ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				p.t.Errorf("proxy %s: %v", p.name, err)
			}
			return
		}
		p.mu.Lock()
		p.accepted++
		p.conns[c] = true
		silent, target := p.silent, p.target
		p.mu.Unlock()
		if !silent {
			p.wg.Go(func() { p.forward(c, target) })
		}
	}
}

func (p *proxy) forward(c net.Conn, target string) {
	up, err := net.Dial("tcp", target)
	if err != nil {
		p.t.Errorf("proxy %s: %v", p.name, err)
		p.drop(c)
		return
	}
	p.mu.Lock()
	p.conns[up] = true
	p.mu.Unlock()
	var wg sync.WaitGroup
	wg.Go(func() { p.copy(up, c) })
	wg.Go(func() { p.copy(c, up) })
	wg.Wait()
}

// copy forwards one direction, then ends both, so the other direction ends
// too.
func (p *proxy) copy(dst, src net.Conn) {
	if _, err := io.Copy(dst, src); err != nil && !ended(err) {
		p.t.Errorf("proxy %s: %v", p.name, err)
	}
	p.drop(dst)
	p.drop(src)
}

func (p *proxy) drop(c net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conns[c] {
		delete(p.conns, c)
		p.closeConn(c)
	}
}

func (p *proxy) closeConn(c io.Closer) {
	if err := c.Close(); err != nil && !ended(err) {
		p.t.Errorf("proxy %s: %v", p.name, err)
	}
}

// ended reports whether err only says that a side of the connection is gone.
func ended(err error) bool {
	return errors.Is(err, net.ErrClosed) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE)
}

// An env is one test's set of proxies: three rw proxies on localhost to the
// primary, and three read proxies on 127.0.0.1 and 127.0.0.2, one to each
// instance, as each node's read port reaches its own instance.
type env struct {
	t     *testing.T
	rw    [3]*proxy
	read  [3]*proxy
	names map[int]string
}

func newEnv(t *testing.T) *env {
	e := &env{t: t, names: map[int]string{}}
	for i := range e.rw {
		e.rw[i] = e.proxy(fmt.Sprintf("rw%d", i+1), "primary", "127.0.0.1")
	}
	for i, target := range []string{"primary", "standby1", "standby2"} {
		e.read[i] = e.proxy("read-"+target, target, "127.0.0.1", "127.0.0.2")
	}
	return e
}

func (e *env) proxy(name, target string, ips ...string) *proxy {
	p := newProxy(e.t, name, cluster.addr[target], ips...)
	e.names[p.port] = name
	return p
}

// name is the proxy a client connection's remote address belongs to.
func (e *env) name(remote net.Addr) string {
	tcp, ok := remote.(*net.TCPAddr)
	if !ok {
		return remote.String()
	}
	if name, ok := e.names[tcp.Port]; ok {
		return name
	}
	return remote.String()
}

func (e *env) byName(name string) *proxy {
	for _, p := range append(e.rw[:], e.read[:]...) {
		if p.name == name {
			return p
		}
	}
	e.t.Fatalf("no proxy %s", name)
	return nil
}

func (e *env) refuseRW() {
	for _, p := range e.rw {
		p.refuse()
	}
}

func (e *env) readAccepted() int {
	n := 0
	for _, p := range e.read {
		n += p.acceptedCount()
	}
	return n
}

func (e *env) rwAccepted() int {
	n := 0
	for _, p := range e.rw {
		n += p.acceptedCount()
	}
	return n
}

func (e *env) dsn() string {
	return e.dsnWithoutCert() + fmt.Sprintf(" sslcert=%s sslkey=%s", certFile("app.crt"), certFile("app.key"))
}

func (e *env) dsnWithoutCert() string {
	return fmt.Sprintf("host=localhost,localhost,localhost port=%d,%d,%d dbname=postgres user=app"+
		" sslmode=verify-full sslrootcert=%s connect_timeout=%d",
		e.rw[0].port, e.rw[1].port, e.rw[2].port, certFile("ca.crt"), int(connectTimeout.Seconds()))
}

func (e *env) reads() []string {
	return []string{e.read[0].addr(), e.read[1].addr(), e.read[2].addr()}
}

const connectTimeout = 2 * time.Second

// served says which proxy, at which IP, and which instance served a query.
type served struct {
	proxy  string
	ip     string
	server string
}

func (s served) onRW() bool {
	return strings.HasPrefix(s.proxy, "rw")
}

// A client is one way an application reaches the database. Each read and
// write checks out one connection.
type client interface {
	// read returns the instance's cluster_name after hold.
	read(ctx context.Context, hold time.Duration) (served, error)
	write(ctx context.Context) (served, error)
}

const (
	readQuery  = "SELECT current_setting('cluster_name') FROM pg_sleep($1)"
	writeQuery = "INSERT INTO writes DEFAULT VALUES RETURNING current_setting('cluster_name')"
)

type poolClient struct {
	e    *env
	pool *pgxpool.Pool
}

func (c poolClient) query(ctx context.Context, sql string, args ...any) (served, error) {
	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		return served{}, err
	}
	defer conn.Release()
	s := servedBy(c.e, conn.Conn())
	err = conn.QueryRow(ctx, sql, args...).Scan(&s.server)
	return s, err
}

func (c poolClient) read(ctx context.Context, hold time.Duration) (served, error) {
	return c.query(ctx, readQuery, hold.Seconds())
}

func (c poolClient) write(ctx context.Context) (served, error) {
	return c.query(ctx, writeQuery)
}

func servedBy(e *env, conn *pgx.Conn) served {
	remote := conn.PgConn().Conn().RemoteAddr()
	s := served{proxy: e.name(remote)}
	if tcp, ok := remote.(*net.TCPAddr); ok {
		s.ip = tcp.IP.String()
	}
	return s
}

type sqlClient struct {
	e  *env
	db *sql.DB
}

func (c sqlClient) query(ctx context.Context, query string, args ...any) (s served, err error) {
	conn, err := c.db.Conn(ctx)
	if err != nil {
		return served{}, err
	}
	defer func() { err = errors.Join(err, conn.Close()) }()
	if err := rawServed(c.e, conn, &s); err != nil {
		return s, err
	}
	err = conn.QueryRowContext(ctx, query, args...).Scan(&s.server)
	return s, err
}

func (c sqlClient) read(ctx context.Context, hold time.Duration) (served, error) {
	return c.query(ctx, readQuery, hold.Seconds())
}

func (c sqlClient) write(ctx context.Context) (served, error) {
	return c.query(ctx, writeQuery)
}

func rawServed(e *env, conn *sql.Conn, s *served) error {
	return conn.Raw(func(dc any) error {
		sc, ok := dc.(*stdlib.Conn)
		if !ok {
			return fmt.Errorf("driver connection is %T", dc)
		}
		*s = servedBy(e, sc.Conn())
		return nil
	})
}

type gormClient struct {
	e  *env
	db *gorm.DB
}

func (c gormClient) query(ctx context.Context, query string, args ...any) (served, error) {
	var s served
	err := c.db.WithContext(ctx).Connection(func(tx *gorm.DB) error {
		conn, ok := tx.Statement.ConnPool.(*sql.Conn)
		if !ok {
			return fmt.Errorf("GORM connection is %T", tx.Statement.ConnPool)
		}
		if err := rawServed(c.e, conn, &s); err != nil {
			return err
		}
		return tx.Raw(query, args...).Scan(&s.server).Error
	})
	return s, err
}

func (c gormClient) read(ctx context.Context, hold time.Duration) (served, error) {
	return c.query(ctx, strings.Replace(readQuery, "$1", "?", 1), hold.Seconds())
}

func (c gormClient) write(ctx context.Context) (served, error) {
	return c.query(ctx, writeQuery)
}

const maxConns = 4

// A kind opens one sort of client on an env. edit, when not nil, changes the
// parsed config before the client uses it.
type kind struct {
	name string
	open func(t *testing.T, e *env, edit func(*pgx.ConnConfig)) client
}

var kinds = []kind{
	{"pgxpool", openPool},
	{"database-sql", openSQL},
	{"gorm", func(t *testing.T, e *env, edit func(*pgx.ConnConfig)) client { return openGORM(t, e, "", edit) }},
	{"gorm-timezone", func(t *testing.T, e *env, edit func(*pgx.ConnConfig)) client {
		return openGORM(t, e, " TimeZone=UTC", edit)
	}},
}

func openPool(t *testing.T, e *env, edit func(*pgx.ConnConfig)) client {
	cfg, err := pgxpool.ParseConfig(e.dsn())
	if err != nil {
		t.Fatal(err)
	}
	if edit != nil {
		edit(cfg.ConnConfig)
	}
	return poolClient{e: e, pool: newPool(t, cfg, e)}
}

func newPool(t *testing.T, cfg *pgxpool.Config, e *env) *pgxpool.Pool {
	cfg.MaxConns = maxConns
	if err := pgfallback.Pool(cfg, e.reads()...); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func openSQL(t *testing.T, e *env, edit func(*pgx.ConnConfig)) client {
	return sqlClient{e: e, db: openDB(t, e, edit)}
}

func openDB(t *testing.T, e *env, edit func(*pgx.ConnConfig)) *sql.DB {
	cc, err := pgx.ParseConfig(e.dsn())
	if err != nil {
		t.Fatal(err)
	}
	if edit != nil {
		edit(cc)
	}
	opts, err := pgfallback.Stdlib(e.reads()...)
	if err != nil {
		t.Fatal(err)
	}
	db := stdlib.OpenDB(*cc, opts...)
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db
}

// openGORM opens GORM with the application's DSN, as GORM parses it itself.
// A config that needs an edit goes through GORM's Conn instead, since a DSN
// cannot carry it.
func openGORM(t *testing.T, e *env, dsnSuffix string, edit func(*pgx.ConnConfig)) client {
	var dialector gorm.Dialector
	if edit != nil {
		dialector = postgres.New(postgres.Config{Conn: openDB(t, e, edit)})
	} else {
		opts, err := pgfallback.Stdlib(e.reads()...)
		if err != nil {
			t.Fatal(err)
		}
		dialector = postgres.New(postgres.Config{DSN: e.dsn() + dsnSuffix, OptionOpenDB: opts})
	}
	db, err := gorm.Open(dialector, &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(maxConns)
	sqlDB.SetMaxIdleConns(maxConns)
	if edit == nil {
		t.Cleanup(func() {
			if err := sqlDB.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	return gormClient{e: e, db: db}
}

// eachKind runs test for every kind of client, side by side, each on its
// own env.
func eachKind(t *testing.T, kinds []kind, test func(t *testing.T, e *env, open func(edit func(*pgx.ConnConfig)) client)) {
	t.Parallel()
	for _, k := range kinds {
		t.Run(k.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			test(t, e, func(edit func(*pgx.ConnConfig)) client { return k.open(t, e, edit) })
		})
	}
}

type result struct {
	start time.Time
	s     served
	err   error
}

// load reads through c from two goroutines, every 20 ms each, for d.
func load(c client, d time.Duration) []result {
	var mu sync.Mutex
	var results []result
	var wg sync.WaitGroup
	end := time.Now().Add(d)
	for range 2 {
		wg.Go(func() {
			for time.Now().Before(end) {
				r := result{start: time.Now()}
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				r.s, r.err = c.read(ctx, 0)
				cancel()
				mu.Lock()
				results = append(results, r)
				mu.Unlock()
				time.Sleep(20 * time.Millisecond)
			}
		})
	}
	wg.Wait()
	return results
}

// fill opens maxConns connections at once, each holding its read a moment,
// and checks each landed on a standby's read address.
func fill(t *testing.T, c client) []served {
	t.Helper()
	return fillN(t, c, maxConns)
}

func fillN(t *testing.T, c client, n int) []served {
	t.Helper()
	var mu sync.Mutex
	var all []served
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			s, err := c.read(t.Context(), 300*time.Millisecond)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				t.Errorf("read: %v", err)
				return
			}
			all = append(all, s)
		})
	}
	wg.Wait()
	for _, s := range all {
		if !onStandby(s) {
			t.Errorf("read served by %s (%s), want a standby's read address", s.proxy, s.server)
		}
	}
	return all
}

func onStandby(s served) bool {
	return (s.proxy == "read-standby1" && s.server == "standby1") || (s.proxy == "read-standby2" && s.server == "standby2")
}

// checkBack checks every read of results worked, and that each one that
// started later than bound after rw came back was served by rw.
func checkBack(t *testing.T, results []result, back time.Time, bound time.Duration) {
	t.Helper()
	after := 0
	for _, r := range results {
		if r.err != nil {
			t.Errorf("read at %v: %v", r.start.Sub(back).Round(time.Millisecond), r.err)
			continue
		}
		if r.start.Sub(back) <= bound {
			continue
		}
		after++
		if !r.s.onRW() || r.s.server != "primary" {
			t.Errorf("read %v after rw came back served by %s (%s)", r.start.Sub(back).Round(time.Millisecond), r.s.proxy, r.s.server)
		}
	}
	if after == 0 {
		t.Errorf("no read started later than %v after rw came back", bound)
	}
}
