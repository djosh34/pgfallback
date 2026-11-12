#!/usr/bin/env bash
# Fetches pgfallback at <query> through proxy.golang.org only, as an
# application does, builds a file that imports it, checks that go recorded
# <version>, and runs go mod verify. Retries the fetch for up to two minutes,
# the proxy's delay for a new version. The last job of release.yml runs it
# with the tag as both arguments.
#
# The first request for a version fixes it in the proxy and in
# sum.golang.org for good, and a request before the tag exists can keep the
# proxy answering "not found" for up to 30 minutes. So ask for a tag only
# once it is pushed. To try the script before a release, give a pushed
# commit of main and its pseudo-version, which asks the proxy for no tag.
#
# Needs go.
#
# Usage: bash release-check.sh <tag or commit> <version>
set -euo pipefail
shopt -s inherit_errexit

query=${1:?usage: release-check.sh <tag or commit> <version>}
version=${2:?usage: release-check.sh <tag or commit> <version>}
module=github.com/djosh34/pgfallback

export GOPROXY=https://proxy.golang.org GOSUMDB=sum.golang.org GOFLAGS=""
export GOPRIVATE='' GONOPROXY='' GONOSUMDB='' GOINSECURE=''

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
cd "$work"
go version
go mod init example.com/release-check

deadline=$((SECONDS + 120))
until go get "$module@$query"; do
  if ((SECONDS >= deadline)); then
    echo "release-check.sh: go get $module@$query failed for two minutes" >&2
    exit 1
  fi
  sleep 10
done

printf 'package main\n\nimport _ "%s"\n\nfunc main() {}\n' "$module" >main.go
go build -o /dev/null .

got=$(go list -m -f '{{.Version}}' "$module")
if [[ $got != "$version" ]]; then
  echo "release-check.sh: go recorded $module $got, want $version" >&2
  exit 1
fi
go mod verify
echo "ok: $module@$version through proxy.golang.org"
