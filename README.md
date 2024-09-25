# jepsen.bufstream

Tests for the Bufstream Kafka-compatible event log.

## Usage

You'll need a copy of the `bufstream` binary on this machine, specified using `--bin FILE`.

```
lein run test-all --username admin --nodes-file ~/nodes --bin bufstream-0.1.3-rc6 --concurrency 3n --time-limit 300 --test-count 30 --safe --sub-via subscribe --no-fetch-sync --ignore-queue-errors unseen
```

It looks like the best performance comes from running with `--no-fetch-sync`.

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

## REPL

Run `lein repl` to start a REPL in this directory. You can pull in namespaces from the test suite and perform post-hoc analysis of tests like so:

```
=> (require '[jepsen.history :as h] '[jepsen.redpanda.workload.queue :as rq] '[jepsen.store :as store] '[jespen.tests.kafka :as k])
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
