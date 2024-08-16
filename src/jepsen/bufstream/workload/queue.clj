(ns jepsen.bufstream.workload.queue
  (:require [clojure [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [role :as role]
                    [util :as util]]
            [jepsen.redpanda.workload.queue :as rq]
            [jepsen.tests.kafka :as k]))

(defn except-debug-partitions
  [op]
  (not= :debug-topic-partitions (:f op)))

(defn workload
  "Constructs a test workload given CLI options. Options are documented in the
  Redpanda client and jepsen.tests.kafka/workload docs."
  [opts]
  (-> (k/workload opts)
      (assoc :client (role/restrict-client :bufstream (rq/client)))
      ; We don't support debugging topic-partitions yet
      (update :generator (partial gen/filter except-debug-partitions))
      (update :final-generator (partial gen/filter except-debug-partitions))))
