# jepsen.bufstream

Tests for the Bufstream Kafka-compatible event log. Runs on a cluster of at
least three Debian Bookworm nodes. The first node is used for etcd, the second
for Minio (an S3-compatible storage engine), and the remainder run Bufstream.

## Usage

`lein run test` runs a single test, potentially multiple rounds, until a failure is detected. `lein run test-all` runs a full suite of tests with different nemeses and workloads. `lein run serve` runs a web server for browsing the `store/` directory. See [Jepsen](https://github.com/jepsen-io/jepsen)'s docs for more.

For running tests, you'll need a copy of the `bufstream` binary on this
machine, specified using `--bin FILE`. You'll also need a [Jepsen
environment](https://github.com/jepsen-io/jepsen?tab=readme-ov-file#setting-up-a-jepsen-environment). Then you can run something like:

```
lein run test-all --bin bufstream-0.1.3-rc6
```

There are lots of options to configure how tests run. Try `lein run test
--help` to see them. For example:

```
lein run test-all --username admin --nodes-file ~/nodes --bin bufstream-0.1.3-rc6 --concurrency 3n --time-limit 300 --test-count 30 --safe --sub-via subscribe --no-fetch-sync --ignore-queue-errors unseen
```

It looks like the best performance in 0.1.0 through 0.1.3-rc6 comes from
running with `--no-fetch-sync`.

For details of the queue analysis, see the [Kafka
workload](https://github.com/jepsen-io/jepsen/blob/main/jepsen/src/jepsen/tests/kafka.clj)
in Jepsen. For the actual implementation of the queue workload and client, see
the [Redpanda test
suite](https://github.com/jepsen-io/redpanda/blob/main/src/jepsen/redpanda/workload/queue.clj)---it
speaks the Kafka wire protocol, so we use it to test all three of Kafka,
Redpanda, and Bufstream. At some point this should probably get pulled out into
a `jepsen.kafka` library; this arrangement is sort of a historical accident.

### Faults

Because Bufstream has different subsystems--coordinator, storage, and bufstream
itself--we have faults that can target these roles independently. `--nemesis
kill` introduces crash faults across all processes. `kill-bufstream`,
`kill-storage`, and `kill-coordination` introduce crash faults on just those
subsystems. Likewise, `pause` and `clock` come in global and subsystem-scoped
variants. Partitions can be introduced randomly over the whole cluster
(`--nemesis partition`), between Bufstream nodes (`partition-bufstream`), or
between Bufstream and a specific subsystem (`partition-bufstream-coordination`,
`partition-bufstream-storage`).

We mix faults together randomly: see `jepsen.bufstream.nemesis/package-gen` for
this. Every few seconds we rotate to a new, randomly selected subset of nemesis
packages, including sometimes no packages at all, to allow for recovery. This
prevents us from driving the cluster completely offline with constant faults,
while still reaching interesting combinations of faults.

Because Bufstream likes to kill itself when startled by (e.g.) network hiccups,
we run a watchdog which continually polls to make sure Bufstream is running,
and restarts the process if it dies. You'll see log lines about `:watchdog`.

### Debugging

There are a few options specifically to help with gathering debugging
information. First, you'll find a copy of the etcd data dir in
`n1/data.tar.bz2`, if `n1` is your first node. The S3 storage data is dumped to
a tarball in `n2/bucket.tar.bz2`; note that this tarball's structure is the S3
structure, *not* the actual data directory you'll need to run a fresh instance
of `minio`. Logs for bufstream are in (e.g.) `n3/bufstream.log`, and the config file is `n3/bufstream.yaml`.

If you pass `--tcpdump`, the test will capture Kafka traffic to Bufstream
nodes, logging it to (e.g.) `n3/tcpdump.pcap`. `--bugstream-log-level DEBUG`
will get more detail from Bufstream's logs. Use `--no-archive` to disable
Bufstream archiving.

## What's Here

See `src/jepsen/bufstream/` for the test harness code. In there, you'll find:

- `cli.clj`: Main entry point. Parses CLI options, constructs and runs tests.
- `core.clj`: Common functions shared across the rest of the test.
- `db.clj`: DB automation. Stitches together bufstream, minio, etcd, tcpdump, and a watchdog into a Jepsen DB object.
- `db/bufstream.clj`: Runs Bufstream nodes
- `db/minio.clj`: Runs minio, our storage system
- `db/watchdog.clj`: Responsible for restarting bufstream when it crashes.
- `nemesis.clj`: Schedules and injects faults
- `workload/early_read.clj`: A (not very interesting) search for a specific G1c cycle.
- `workload/producer_fence.clj`: Demonstrates that producers throw a fenced exception on timeout
- `workload/producer_perf.clj`: Producer performance test--a WIP testbed for exploring some performance issues we've had in testing.
- `workload/queue.clj`: The main and most useful test. Verifies transactional and non-transactional sends, polls, and assigns.

## Interpreting Queue Results

Every Jepsen test produces a directory in `store/<test name>/<timestamp>`. See
the [Redpanda
docs](https://github.com/jepsen-io/redpanda?tab=readme-ov-file#understanding-results)
for what these files mean, and detailed examples for how to interpret them. 

## REPL

Run `lein repl` to start a REPL in this directory. You can pull in namespaces from the test suite and perform post-hoc analysis of tests like so:

```
=> (require '[jepsen.history :as h] '[jepsen.redpanda.workload.queue :as rq] '[jepsen.store :as store] '[jepsen.tests.kafka :as k])
=> (def t (store/test "/home/aphyr/bufstream/store/bufstream-0.1.2 queue rc assign acks=all retries=1000 aor=earliest auto-topics=false idem=true/20240820T082634.633-0500"))
=> (def h (:history t))
=> (count h)
48636
=> (->> h (k/around-key-value 5 140) pprint)
({:process 23,
  :type :ok,
  :f :send,
  :value ([:send 5 [272 140]]),
  :index 7797,
  :time 13620301650}
 {:process 187,
  :type :ok,
  :f :poll,
  :value ({5 [[272 140] [273 142] [277 144] [278 145]]}),
  :f :poll,
  :value [[:poll]],
  :poll-ms 1000,
  :index 24255,
  :time 128583906562}
  ...)
```

## License

Copyright © 2024 Jepsen, LLC

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
