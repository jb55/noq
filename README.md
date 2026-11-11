<p align="center">
  <img src="docs/logo.svg" width="128" height="128" alt="noq logo: a light capital Q on a dark tile, its tail an amber bar">
</p>

<h1 align="center">noq</h1>

<p align="center">A nostr message queue that forgets what you've consumed.</p>

<p align="center"><a href="https://jb55.github.io/noq/">jb55.github.io/noq</a></p>

noq stores signed nostr events in an append-only log. Consumers lease the
events that match a NIP-01 filter and ack them when they're done. When every
event in a segment file is consumed, the file is deleted, so the disk drains
with the queue.

```
cargo install --git https://github.com/jb55/noq
```

## Push, lease, ack

```
$ noq push < notes.jsonl
617 queued, 0 not queued, 0 invalid

$ noq lease '{"kinds":[1]}' -n 3 --ack
{"id":"b2e03951843b…","kind":1,"content":"hello, this is my new key",…}
…

$ noq push < forged.jsonl
invalid 1 id does not match the event hash
```

Every push checks the event's id and signature. A lease that isn't acked ends
when its holder exits, and the events are queued again. `noq --help` lists the
rest: `peek`, `ack`, `compact`, `stats` and `bench`.

## As a library

```rust
use noq::{Config, Filter, Noq};
use std::time::Duration;

let q = Noq::open("queue", Config::default())?;
q.push(json)?;                          // id and sig checked; Queued, Duplicate, Seen or Expired

let dms = Filter::new().kinds([4, 1059]);
for lease in q.wait_lease(&dms, 100, Duration::from_secs(30), Duration::from_secs(5)) {
    deliver(lease.json())?;             // the original event json, as it was pushed
    lease.ack()?;                       // or lease.nack() to queue it again
}
```

A process opens the queue once and shares it between threads, so one pushes
while others lease.

## How it works

- **Segments.** Events are appended to fixed-size, memory-mapped segment files
  in arrival order. Each record starts with a state byte, and a lease is one
  compare-and-swap on it, from live to leased, so two consumers never take the
  same event.
- **Verified on the way in.** One pass over the JSON checks it strictly,
  computes the event id and verifies the schnorr signature.
- **The JSON is the wire format.** Each record keeps the original JSON next to
  a small span index, so a leased event goes back out byte for byte.
- **The disk drains.** Fully consumed segments are deleted. Mostly consumed
  ones are compacted so the file can go too.
- **Crash safe.** Each record carries a CRC32C, and reopening after a crash
  drops a torn record at the tail.

Benchmarks against nostrdb and strfry are in [bench/README.md](bench/README.md).

## Status

Early, version 0.1. The storage, leasing and filtering work and are tested.
The API will still change.

- One process owns a queue directory. Its writer and consumers are threads.
- One queue per directory, with no consumer groups.
- No network front yet: an app embeds the crate or drives the CLI.
- Every event is queued on its own, replaceable and addressable kinds
  included. Deciding which version is newest is the consumer's job.
