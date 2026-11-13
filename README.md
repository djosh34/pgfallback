# pgfallback

A Go package for pgx, database/sql and GORM applications on a PostgreSQL cluster with a primary and streaming standbys. The application keeps its connection string with the rw addresses, which reach the primary. The package adds read addresses, which reach a standby, and uses them only while no rw address takes a new connection. Once one does again, new connections go back to rw.

So while there is no primary, reads keep working and writes fail with an error the application can recognise.

```bash
go get github.com/djosh34/pgfallback@latest
```

It needs the Go version in `go.mod`, which is the latest Go release, and adds pgx v5. It does not import GORM.

## What it does

- **A new connection** tries the rw hosts in order, then the read addresses in the order given. The first that lets it in serves it. Nothing asks the server for its role, so a read address that reaches the primary serves writes too.
- **A connection on a read address is a read connection.** While any are in use, the package tries a full connect to rw in the background, at most once a second, and closes it at once.
- **Once an attempt works,** read connections are closed instead of reused. pgxpool closes one when it is handed out or released, after the running query ends. database/sql and GORM close one the next time it is handed out. An idle one stays open, unused, until the pool's idle limit.
- **After more than 2 s without an attempt,** a read connection that is handed out is closed, so the next query tries rw first.

It does not retry anything, change any error, send reads to the read addresses while rw works, or reorder the hosts.

## Connection string

```
host=node1.example.com,node2.example.com,node3.example.com port=5432 dbname=orders user=orders
sslmode=verify-full sslrootcert=ca.crt sslcert=orders-app.crt sslkey=orders-app.key connect_timeout=2
```

- TLS on every host: `sslmode` `require`, `verify-ca` or `verify-full`.
- `connect_timeout` set; 2 works.
- `target_session_attrs` unset or `any`. Any other value makes pgx check each server's role.

`Pool` returns an error for a config that breaks one of these. With `Stdlib`, the error comes from the first connect: `db.PingContext`, or `gorm.Open`.

## Set up

The examples reach the primary on port 5432 of each node and a standby on port 5433. `TestDocExamples` checks that each Go block here is in `example_test.go`, where it compiles.

### pgx

```go
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
```

`Pool` keeps the config's hooks and calls them too. A change to the config after the call reaches neither.

### database/sql

```go
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
```

### GORM

```go
// connectGORM opens a *gorm.DB that falls back to the read port.
func connectGORM(dsn string) (*gorm.DB, error) {
	opts, err := pgfallback.Stdlib("node1.example.com:5433", "node2.example.com:5433", "node3.example.com:5433")
	if err != nil {
		return nil, err
	}
	return gorm.Open(postgres.New(postgres.Config{DSN: dsn, OptionOpenDB: opts}), &gorm.Config{})
}
```

`Stdlib` returns `stdlib.OptionBeforeConnect` and `stdlib.OptionResetSession`. The last option of each kind wins, so pass none of your own of those two kinds.

### A short dial timeout

A host that is down and does not refuse makes each new connection wait `connect_timeout` for it. A dialer with a shorter timeout gets past such hosts sooner. Set it before `pgfallback.Pool`, which copies the config for its attempts:

```go
cfg.ConnConfig.DialFunc = (&net.Dialer{Timeout: 500 * time.Millisecond}).DialContext
```

Pick a value above the network's normal connect time.

## Errors while there is no primary

The package passes every error on as it is.

- A write on a standby fails with SQLSTATE 25006. `pgfallback.NoPrimary(err)` is true for it. It checks only the SQLSTATE, so a read-only transaction on the primary gives true too.
- A pgx transaction with `AccessMode: pgx.ReadWrite` fails on a standby at `BEGIN` with SQLSTATE 0A000; `NoPrimary` is false for it.
- Whether a commit under way when its connection broke went through is not known.
- Retries are the application's, a whole transaction at a time.

## How far behind a read can be

A read on a read address sees what that standby has applied, and nothing bounds how far behind that is. A standby that comes back can serve reads before it has caught up. An application that must read its own last commit has that only on the primary.

## Tests

`go test -race ./...` needs docker and openssl. `testdata/cluster.sh` starts PostgreSQL 18 in docker: a primary, two streaming standbys and a standby with `hot_standby` off. Lint: `go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.15.0 run`.

## Updates

A daily run of [`autopatch.yml`](.github/workflows/autopatch.yml) keeps the `go` line at the latest Go release and the deps at their latest. When that changes anything and CI passes, it merges the change and releases the next patch version. When nothing changes, it runs the Trivy check on `main`.

## Licence

Apache-2.0, see [LICENSE](LICENSE).
