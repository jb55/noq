#!/usr/bin/env bash
# Runs one file of events through noq, nostrdb and strfry: ingest rate,
# disk used, and consume (noq: lease+ack; strfry: scan then delete;
# nostrdb: one query, as it has no delete). Prints each tool's numbers.
#
# usage: bench/compare.sh EVENTS [N]
#
# EVENTS holds one event per line, bare or as ["EVENT",<sub>,<event>]
# relay messages (nostrdb's testdata/many-events.json is the latter). N
# takes only its first N lines.
#
# env: NOQ      noq binary (default target/release/noq)
#      NDB_SRC  a built nostrdb checkout, to link bench/ndb-ingest.c
#               against; nostrdb is skipped without it
#      STRFRY   strfry binary (default strfry on PATH; skipped if none)
#      STRFRY_CONF  a strfry.conf, its db path overridden (default the
#               one beside the strfry binary)
#      THREADS  noq push and nostrdb ingest threads for the parallel runs
#               (default: one per core); each also runs with one
#      TOOLS    which to run (default "noq nostrdb strfry")
#      WORK     scratch directory (default a fresh mktemp -d, removed after)

set -euo pipefail

events=${1:?usage: bench/compare.sh EVENTS [N]}
n=${2:-}
here=$(dirname "$0")
noq=${NOQ:-target/release/noq}
strfry=${STRFRY:-strfry}
threads=${THREADS:-$(nproc)}
tools=" ${TOOLS:-noq nostrdb strfry} "
gnutime=$(type -P time) || { echo "need GNU time on PATH" >&2; exit 1; }

if [[ -n ${WORK:-} ]]; then
	work=$WORK
	mkdir -p "$work"
else
	work=$(mktemp -d)
	trap 'rm -rf "$work"' EXIT
fi

# Both forms of the input: bare events for noq and strfry, relay
# messages for nostrdb.
take() { if [[ -n $n ]]; then head -n "$n" "$events"; else cat "$events"; fi; }
# A relay message's event runs from its first { to the ] that ends it.
take | LC_ALL=C awk '/^\[/ { i = index($0, "{"); $0 = substr($0, i, length($0) - i) } { print }' \
	>"$work/bare.jsonl"
LC_ALL=C awk '{ print "[\"EVENT\",\"b\"," $0 "]" }' "$work/bare.jsonl" >"$work/relay.jsonl"
lines=$(wc -l <"$work/bare.jsonl")
echo "input: $lines events, $(stat -c %s "$work/bare.jsonl") bytes of json, $(nproc) cores"
echo

# Runs a command with its output to $1, printing the output and then its
# wall time and peak rss.
timed() {
	local log=$1 status=0
	shift
	"$gnutime" -f 'wall %e s, max rss %M KiB' -o "$work/time" "$@" >"$log" 2>&1 || status=$?
	cat "$log"
	grep '^wall' "$work/time"
	return $status
}
wall() { sed -nE 's/^wall ([0-9.]+) s.*/\1/p' "$work/time"; }
rate() { awk -v n="$1" -v t="$2" 'BEGIN { printf "%.0f ev/s", n / t }'; }

# Apparent size of every file under a directory: what the store wrote,
# whatever the filesystem's compression or sparse files make of it.
size() { find "$1" -type f -printf '%s\n' | awk '{ s += $1 } END { print s + 0 }'; }

for t in "$threads" 1; do
	[[ $tools == *" noq "* ]] || break
	echo "== noq bench, $t push thread(s), verified, fsync never"
	timed "$work/noq.log" "$noq" --fsync never bench "$work/bare.jsonl" --threads "$t"
	echo
done

if [[ $tools != *" nostrdb "* ]]; then
	:
elif [[ -n ${NDB_SRC:-} ]]; then
	s=$NDB_SRC
	cc -O2 -I"$s/src" -I"$s/deps/lmdb" -I"$s/deps/flatcc/include" -I"$s/ccan" \
		"$here/ndb-ingest.c" "$s/libnostrdb.a" "$s/deps/lmdb/liblmdb.a" \
		"$s/deps/secp256k1/.libs/libsecp256k1.a" \
		"$s/deps/libsodium/src/libsodium/.libs/libsodium.a" -lpthread \
		-o "$work/ndb-ingest" 2>"$work/cc.log" || { cat "$work/cc.log"; exit 1; }
	for t in "$threads" 1; do
		echo "== nostrdb, $t ingest thread(s), verified"
		rm -rf "$work/ndb"
		mkdir -p "$work/ndb"
		timed "$work/ndb.log" "$work/ndb-ingest" --threads "$t" "$work/ndb" "$work/relay.jsonl"
		echo "disk $(size "$work/ndb") bytes"
		echo
	done
else
	echo "== nostrdb: skipped, set NDB_SRC to a built nostrdb checkout"
	echo
fi

if [[ $tools != *" strfry "* ]]; then
	:
elif command -v "$strfry" >/dev/null; then
	conf=${STRFRY_CONF:-$(dirname "$(command -v "$strfry")")/strfry.conf}
	[[ -f $conf ]] || { echo "no strfry.conf at $conf (set STRFRY_CONF)" >&2; exit 1; }
	rm -rf "$work/strfry"
	mkdir -p "$work/strfry"
	# Big enough for the largest contact lists in nostrdb's corpus.
	sf=("$strfry" --config="$conf" --set "db=$work/strfry/" --set events__maxEventSize=16777216)
	echo "== strfry import, verified"
	timed "$work/strfry.log" "${sf[@]}" import <"$work/bare.jsonl" | tail -n 3 ||
		echo "(strfry import failed)"
	echo "import $(rate "$lines" "$(wall)")"
	echo "disk $(size "$work/strfry") bytes"
	echo
	echo "== strfry scan '{}' then delete --filter '{}'"
	"$gnutime" -f 'wall %e s, max rss %M KiB' -o "$work/time" \
		"${sf[@]}" scan '{}' >"$work/scan.jsonl" 2>"$work/scan.log"
	scanned=$(wc -l <"$work/scan.jsonl")
	grep '^wall' "$work/time"
	echo "scan $(rate "$scanned" "$(wall)") ($scanned events)"
	timed "$work/delete.log" "${sf[@]}" delete --filter '{}' | tail -n 3 ||
		echo "(strfry delete failed)"
	echo "delete $(rate "$scanned" "$(wall)")"
	echo "disk after delete $(size "$work/strfry") bytes"
else
	echo "== strfry: skipped, no $strfry on PATH (set STRFRY)"
fi
