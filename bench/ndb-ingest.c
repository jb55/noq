// Times nostrdb ingesting a file of relay messages, to the last note it
// stores, then one query reading them back.
//
// `ndb import` can't be timed for this: it tears down while its ingester
// threads still hold queued events, and sometimes hangs on exit. Here the
// ingest counts as done once the stored note count has not moved for a
// second; the time is when it last moved. Exits with _exit, leaving
// nostrdb's teardown out of it.
//
// usage: ndb-ingest [--skip-verification] [--threads N] DBDIR FILE
//
// --threads sets nostrdb's ingester threads (default: one per core).
// nostrdb loses notes under load: on nostrdb's testdata the notes stored
// vary run to run and fall well short of the input (100k lines: ~99.9k
// stored with one thread, ~40-47k with 32; 1M lines: ~400k with one).
// Its ingesters push to the writer's bounded queue without checking
// (prot_queue_push fails when full), but feeding 4096 lines at a time and
// waiting for the writer between batches still lost half with 32
// threads, so that isn't the whole cause. Compare notes stored per
// second, not lines.
//
// FILE holds one ["EVENT",<sub>,<event>] message per line. Build with
// bench/compare.sh, which links it against a nostrdb checkout ($NDB_SRC).

#include "nostrdb.h"

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

static double now(void)
{
	struct timespec t;
	clock_gettime(CLOCK_MONOTONIC, &t);
	return t.tv_sec + t.tv_nsec / 1e9;
}

static size_t stored(struct ndb *ndb)
{
	struct ndb_stat stat;
	if (!ndb_stat(ndb, &stat))
		return 0;
	return stat.dbs[NDB_DB_NOTE].count;
}

// Waits until the stored note count has not moved for `quiet` seconds.
// Returns when it last moved, and the count in *notes.
static double settle(struct ndb *ndb, double quiet, size_t *notes)
{
	double last = now();
	size_t n;

	*notes = stored(ndb);
	while (now() - last < quiet) {
		usleep(5000);
		if ((n = stored(ndb)) != *notes) {
			*notes = n;
			last = now();
		}
	}
	return last;
}

int main(int argc, char *argv[])
{
	struct ndb_config config;
	struct ndb *ndb;
	struct ndb_txn txn;
	struct ndb_filter filter;
	struct ndb_query_result *results;
	struct stat st;
	size_t lines = 0, waits = 0, notes;
	double t0, t;
	int flags = 0, count = 0, threads = 0, fd;
	const char *json;

	for (; argc > 1 && argv[1][0] == '-'; argv++, argc--) {
		if (!strcmp(argv[1], "--skip-verification")) {
			flags = NDB_FLAG_SKIP_NOTE_VERIFY;
		} else if (!strcmp(argv[1], "--threads") && argc > 2) {
			threads = atoi(argv[2]);
			argv++;
			argc--;
		} else {
			argc = 0;
			break;
		}
	}
	if (argc != 3) {
		fprintf(stderr, "usage: ndb-ingest [--skip-verification] [--threads N] DBDIR FILE\n");
		return 2;
	}

	if ((fd = open(argv[2], O_RDONLY)) < 0 || fstat(fd, &st)) {
		perror(argv[2]);
		return 2;
	}
	json = mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
	if (json == MAP_FAILED) {
		perror("mmap");
		return 2;
	}
	for (off_t i = 0; i < st.st_size; i++)
		lines += json[i] == '\n';

	ndb_default_config(&config);
	ndb_config_set_mapsize(&config, 1ULL << 40);
	ndb_config_set_flags(&config, flags);
	if (threads > 0)
		ndb_config_set_ingest_threads(&config, threads);
	if (!ndb_init(&ndb, argv[1], &config)) {
		fprintf(stderr, "ndb_init %s failed\n", argv[1]);
		return 2;
	}

	// One line at a time rather than ndb_process_events, which gives up
	// at the first line an ingester's full inbox refuses: wait and retry.
	t0 = now();
	for (const char *p = json, *end = json + st.st_size, *nl; p < end; p = nl + 1) {
		if (!(nl = memchr(p, '\n', end - p)))
			nl = end;
		if (nl == p)
			continue;
		while (!ndb_process_event(ndb, p, nl - p)) {
			waits++;
			usleep(100);
		}
	}
	t = now();
	printf("queued   %zu lines in %.3f s (%zu waits for a full inbox)\n",
	       lines, t - t0, waits);

	t = settle(ndb, 1.0, &notes) - t0;
	printf("ingest   %zu lines in %.3f s: %.0f ev/s; %zu notes stored, %.0f/s (%s)\n",
	       lines, t, lines / t, notes, notes / t,
	       flags ? "unverified" : "verified");

	ndb_filter_init(&filter);
	ndb_filter_start_field(&filter, NDB_FILTER_LIMIT);
	ndb_filter_add_int_element(&filter, notes);
	ndb_filter_end_field(&filter);
	ndb_filter_end(&filter);
	results = malloc(sizeof(*results) * (notes + 1));
	t = now();
	ndb_begin_query(ndb, &txn);
	ndb_query(&txn, &filter, 1, results, notes + 1, &count);
	ndb_end_query(&txn);
	t = now() - t;
	printf("query    %d notes in %.3f s: %.0f ev/s (read, no delete)\n",
	       count, t, count / t);

	fflush(stdout);
	_exit(0);
}
