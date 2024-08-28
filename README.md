# jepsen.bufstream

Tests for the Bufstream Kafka-compatible event log.

## Usage

```
lein run test-all --bin bufstream-0.1.3-rc2 --concurrency 3n --time-limit 1000 --test-count 10 --safe
```

It looks like the best performance comes from running with --no-fetch-sync. 

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
