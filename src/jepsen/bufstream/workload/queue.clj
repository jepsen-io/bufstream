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

(defn checker
  "We extend the normal queue checker to tag a specific kind of duplicate
  anomaly separately. A test has a :duplicate-nonzero if a value appears at
  more than one non-zero offset."
  [checker]
  (reify checker/Checker
    (check [this test history opts]
      (let [r (checker/check checker test history opts)
            bad-errors (:bad-error-types r)
            bad-errors (if (some (fn [err]
                                   (< 1 (count (remove #{0} (:offsets err)))))
                                 (:errs (:duplicate r)))
                         (conj bad-errors :duplicate-nonzero)
                         bad-errors)]
        (assoc r :bad-error-types bad-errors)))))

(defn workload
  "Constructs a test workload given CLI options. Options are documented in the
  Redpanda client and jepsen.tests.kafka/workload docs."
  [opts]
  (-> (k/workload opts)
      (assoc :client (role/restrict-client :bufstream (rq/client)))
      (update :checker checker)
      ; We don't support debugging topic-partitions yet
      (update :generator (partial gen/filter except-debug-partitions))
      (update :final-generator (partial gen/filter except-debug-partitions))))
