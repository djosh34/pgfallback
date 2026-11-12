#!/usr/bin/env bash
# Starts the PostgreSQL 18 instances the pgfallback tests run against, in
# docker: a primary, two streaming standbys and a third standby with
# hot_standby off, which answers every login with 57P03. Logins use client
# certificates (hostssl ... cert).
#
# up writes into DIR: ca.crt, app.crt and app.key for the user app, and
# addrs, one "<name> <host:port>" line per instance (primary, standby1,
# standby2, cold), each published on 127.0.0.1. The server certificates name
# localhost and 127.0.0.1 for the primary, only 127.0.0.1 for the standbys,
# and only localhost for cold.
# down removes the containers and the network that up started.
#
# Usage: bash testdata/cluster.sh up|down DIR
# Needs docker and openssl.
set -euo pipefail
shopt -s inherit_errexit

image=registry.access.redhat.com/hi/postgresql:18.6
instances=(primary standby1 standby2 cold)

if [[ $# -ne 2 || ($1 != up && $1 != down) ]]; then
  echo "usage: bash testdata/cluster.sh up|down DIR" >&2
  exit 2
fi
dir=$2

# The files a server needs, from environment variables, then the server.
# Runs in the container as its postgres user, so it expands there.
# shellcheck disable=SC2016
start_server='
set -euo pipefail
mkdir -m 700 /tmp/ssl
printf "%s\n" "$CA_CRT" > /tmp/ssl/ca.crt
printf "%s\n" "$SERVER_CRT" > /tmp/ssl/server.crt
printf "%s\n" "$SERVER_KEY" > /tmp/ssl/server.key
printf "%s\n" "$STREAMING_CRT" > /tmp/ssl/streaming.crt
printf "%s\n" "$STREAMING_KEY" > /tmp/ssl/streaming.key
chmod 600 /tmp/ssl/*.key
printf "%s\n" "local all all trust" "hostssl all app all cert" "hostssl replication streaming all cert" > /tmp/ssl/pg_hba.conf
if [[ $ROLE == primary ]]; then
  initdb -D "$PGDATA" -U postgres --auth-local=trust >/dev/null
else
  pg_basebackup -D "$PGDATA" -R -X stream --checkpoint=fast \
    -d "host=primary user=streaming sslmode=verify-ca sslrootcert=/tmp/ssl/ca.crt sslcert=/tmp/ssl/streaming.crt sslkey=/tmp/ssl/streaming.key"
  chmod 700 "$PGDATA"
fi
exec postgres -D "$PGDATA" -c listen_addresses="*" -c cluster_name="$NAME" -c hot_standby="$HOT_STANDBY" -c wal_keep_size=256MB \
  -c ssl=on -c ssl_ca_file=/tmp/ssl/ca.crt -c ssl_cert_file=/tmp/ssl/server.crt -c ssl_key_file=/tmp/ssl/server.key \
  -c hba_file=/tmp/ssl/pg_hba.conf
'

init_sql="
CREATE ROLE app LOGIN;
CREATE ROLE streaming LOGIN REPLICATION;
CREATE TABLE writes (n bigserial PRIMARY KEY, at timestamptz NOT NULL DEFAULT now());
GRANT SELECT, INSERT ON writes TO app;
GRANT USAGE ON SEQUENCE writes_n_seq TO app;
"

# Waits up to 60 s for a command to succeed.
wait_for() {
  local what=$1
  shift
  local deadline=$((SECONDS + 60))
  until "$@" >/dev/null 2>&1; do
    if ((SECONDS >= deadline)); then
      echo "cluster.sh: $what not ready after 60 s" >&2
      return 1
    fi
    sleep 0.5
  done
}

streaming() {
  local n
  n=$(docker exec "$prefix-primary" psql -U postgres -Atc "SELECT count(*) FROM pg_stat_replication WHERE state = 'streaming'")
  [[ $n == 3 ]]
}

cert() {
  local name=$1 cn=$2 san=$3
  openssl req -new -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -subj "/CN=$cn" \
    -keyout "$dir/$name.key" -out "$dir/$name.csr" 2>/dev/null
  echo "basicConstraints=CA:FALSE" > "$dir/$name.ext"
  if [[ -n $san ]]; then
    echo "subjectAltName=$san" >> "$dir/$name.ext"
  fi
  openssl x509 -req -in "$dir/$name.csr" -CA "$dir/ca.crt" -CAkey "$dir/ca.key" -CAcreateserial \
    -days 2 -extfile "$dir/$name.ext" -out "$dir/$name.crt" 2>/dev/null
  rm "$dir/$name.csr" "$dir/$name.ext"
}

run_instance() {
  local name=$1 role=$2 hot=$3 server=$4
  docker run -d --name "$prefix-$name" --network "$prefix" --network-alias "$name" \
    --label pgfallback-test="$prefix" -p 127.0.0.1::5432 --entrypoint bash \
    -e ROLE="$role" -e NAME="$name" -e HOT_STANDBY="$hot" \
    -e CA_CRT="$(<"$dir/ca.crt")" -e SERVER_CRT="$(<"$dir/$server.crt")" -e SERVER_KEY="$(<"$dir/$server.key")" \
    -e STREAMING_CRT="$(<"$dir/streaming.crt")" -e STREAMING_KEY="$(<"$dir/streaming.key")" \
    "$image" -c "$start_server" >/dev/null
}

if [[ $1 == down ]]; then
  prefix=$(<"$dir/prefix")
  mapfile -t ids < <(docker ps -aq --filter "label=pgfallback-test=$prefix")
  if ((${#ids[@]} > 0)); then
    docker rm -f "${ids[@]}" >/dev/null
  fi
  docker network rm "$prefix" >/dev/null
  exit 0
fi

prefix=pgfallback-$(od -An -N4 -tx4 /dev/urandom | tr -d ' ')
echo "$prefix" > "$dir/prefix"

openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -subj "/CN=pgfallback test CA" \
  -days 2 -keyout "$dir/ca.key" -out "$dir/ca.crt" 2>/dev/null
cert primary primary DNS:localhost,IP:127.0.0.1
cert standby standby IP:127.0.0.1
cert cold cold DNS:localhost
cert app app ""
cert streaming streaming ""

docker network create "$prefix" >/dev/null
if ! {
  run_instance primary primary on primary &&
    wait_for "primary" docker exec "$prefix-primary" pg_isready -h 127.0.0.1 -q &&
    docker exec "$prefix-primary" psql -U postgres -v ON_ERROR_STOP=1 -qc "$init_sql" &&
    run_instance standby1 standby on standby &&
    run_instance standby2 standby on standby &&
    run_instance cold standby off cold &&
    wait_for "streaming of three standbys" streaming &&
    wait_for "standby1" docker exec "$prefix-standby1" pg_isready -h 127.0.0.1 -q &&
    wait_for "standby2" docker exec "$prefix-standby2" pg_isready -h 127.0.0.1 -q
}; then
  mapfile -t started < <(docker ps -a --filter "label=pgfallback-test=$prefix" --format '{{.Names}}')
  for name in "${started[@]}"; do
    echo "== docker logs $name" >&2
    docker logs "$name" >&2
  done
  exit 1
fi

: > "$dir/addrs"
for name in "${instances[@]}"; do
  mapfile -t published < <(docker port "$prefix-$name" 5432/tcp)
  echo "$name ${published[0]}" >> "$dir/addrs"
done
