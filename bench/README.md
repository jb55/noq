# Benchmarks

`noq bench FILE` pushes a file of events into a fresh queue, then leases and
acks all of it, and prints each phase's rate, the disk the queue took and
what was left after it drained. `bench/compare.sh FILE [N]` runs the same
file through noq, nostrdb and strfry. Its header lists the knobs.

```sh
cargo build --release
target/release/noq --fsync never bench events.jsonl --threads 32
NDB_SRC=~/src/c/nostrdb STRFRY=~/dev/github/hoytech/strfry/strfry \
  bench/compare.sh ~/src/c/nostrdb/testdata/many-events.json
```

nostrdb runs through `bench/ndb-ingest.c`, not `ndb import`. `ndb import`
tears down before its ingesters finish and sometimes hangs on exit. The
harness times ingest to the last stored note instead.

## Results, 2026-11-11

monad: 32 cores, 31 GiB, zfs. The input is nostrdb's
`testdata/many-events.json`: 1,004,866 events, 2.0 GiB of json, relay
traffic from 2022–23. Every run verifies ids and signatures unless it
says otherwise. noq runs with `--fsync never`. strfry and nostrdb use their
own LMDB settings.

noq now queues every version of a replaceable or addressable event
(headway:noq/approve-game-clown). The file holds 211,848 kind-3 contact
lists, 1.5 GiB of its 2.0 GiB, so noq stores twice the bytes it did when
older versions collapsed, and pushes at half the event rate: push is bound
by the bytes it appends. The rows marked † were measured before that
change, with 830,665 queued and 678,014 left pending.

### Ingest

| store | threads | time | rate | kept |
|---|---|---|---|---|
| noq push | 32 | 40.5 s | 24,835 ev/s, 50 MiB/s | 1,004,844 pending (all but 16 invalid and 6 duplicates) |
| noq push † | 32 | 20.5 s | 48,926 ev/s, 99 MiB/s | 678,014 pending |
| noq push † | 1 | 61.0 s | 16,463 ev/s | 678,014 pending |
| noq push, unverified † | 32 | 17.4 s | 57,820 ev/s, 117 MiB/s | |
| noq push, unverified † | 1 | 28.0 s | 35,870 ev/s, 73 MiB/s | |
| nostrdb | 32 | 6.9 s | 145,581 lines/s | **82,769 notes stored** |
| nostrdb | 1 | 47.3 s | 21,236 lines/s | **396,283 notes stored** |
| strfry import | 1 writer | 318.4 s | 3,156 ev/s | 958,067 added, 675,671 after replacement |

nostrdb drops notes under load. The count it keeps varies from run to run
and falls far short of the input. On the first 100k lines, 1 ingest thread
kept 99,928 notes at 18.5k ev/s, and 32 threads kept 42–47k. Its lines/s
is not a stored-notes rate. See `bench/ndb-ingest.c` for what was ruled out.

† On the first 100k lines, where the events are smaller (0.7 KB on average,
against 2.1 KB over the whole file), noq pushes 204k ev/s with 32 threads
and 25.6k with 1. strfry imports 15.1k ev/s.

Verified push is about 75% secp256k1 (perf, 1 thread). With 32 threads the
signature checks run in parallel, and the serial part (append and index
insert under the push lock) caps push at about 100–125 MiB/s of json.

### Disk

| store | after ingest | after consuming everything |
|---|---|---|
| noq | 2.6 GiB: 2.4 GiB in 40 segments + 165 MiB in 39 sidecars (1.31× the queued json) | 12.7 MiB: the tail segment's written bytes |
| noq † | 1.2 GiB: 1.1 GiB in 19 segments + 98 MiB in 18 sidecars (1.33× the queued json) | 8.4 MiB |
| nostrdb (32 threads, 82,769 notes) | 308 MB | no delete |
| nostrdb (1 thread, 396,283 notes) | 2.4 GB | no delete |
| strfry | 1.57 GB | 1.64 GB: LMDB doesn't shrink |

These are logical sizes: noq's written segment bytes plus sidecars, and
the LMDB files' size. Allocated blocks on zfs are compressed and counted
late. noq's anonymous RSS after the push is 564 MiB (index and heap,
not the mmaps), 384 MiB †. Reopening the full queue takes 1.9 s, 0.75 s †.

### Consume

| store | how | rate |
|---|---|---|
| noq | lease+ack `{}`, batch 100, 1 consumer | 183,241 ev/s (1,004,844 events) |
| noq † | lease+ack `{}`, batch 100, 1 consumer | 218,988 ev/s |
| noq † | lease+ack `{}`, batch 1000 | 622,789 ev/s |
| noq † | lease+ack `{}`, batch 10 | 28,973 ev/s |
| noq † | lease+ack `{}`, batch 100, 4 consumers | 70,896 ev/s |
| noq † | lease+ack `{}`, batch 100, 16 consumers | 57,203 ev/s |
| noq † | lease+ack `{"kinds":[1]}`, batch 100 | 2,537,408 ev/s (211,523 events) |
| strfry | `scan '{}'` (read) then `delete --filter '{}'` | 86,403 ev/s read, 77,932 ev/s delete |
| nostrdb | one query, read only | 1.4M ev/s (32-thread db), 436k ev/s (1-thread db) |

A `{}` lease costs about the same per call at any batch size: 0.35 ms at
10, 0.46 ms at 100. `Plan::Scan` walks the log from `Seq(0)` on every call
(`Noq::scan_live`), so it re-reads every consumed record still in the head
segment. A filter that plans to a posting list skips its consumed prefix
and drains about 11× faster. More consumers on `{}` are slower, not faster: each
one walks past the same consumed and leased records.

### fsync

100k events, 32 threads, verified †:

| `--fsync` | push |
|---|---|
| never | 203,532 ev/s |
| every 100 ms (default) | 93,617 ev/s |
| always | 388 ev/s |

`always` syncs inside the push lock on every push, about 2.6 ms each on
zfs, and concurrent pushes never share a sync.
