(ns jepsen.bufstream.workload.queue
  (:require [clojure [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [role :as role]
                    [util :as util]]
            [jepsen.redpanda.workload.queue :as rq]
            [jepsen.tests.kafka :as k]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (org.apache.kafka.clients.admin
             ListOffsetsOptions
             ListOffsetsResult$ListOffsetsResultInfo
             OffsetSpec)))

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

(defrecord Client [client] ; Redpanda client
  client/Client
  (open! [this test node]
    (assoc this :client (client/open! client test node)))

  (setup! [this test]
    (client/setup! client test))

  (invoke! [this test op]
    (case (:f op)
      :debug-topic-partitions
      (try+
        (let [res (-> (:admin client)
                      (.listOffsets
                        (zipmap (map rq/k->topic-partition (:value op))
                                (repeat (OffsetSpec/latest))))
                      .all
                      (deref 10000 nil)
                      (or (throw+ {:type :timeout})))]
          (->> res
               (map (fn [[topic-partition
                          ^ListOffsetsResult$ListOffsetsResultInfo r]]
                      [(rq/topic-partition->k topic-partition)
                       {:epoch (.orElse (.leaderEpoch r) nil)
                        :offset (.offset r)
                        :timestamp (.timestamp r)}]))
               (into (sorted-map))
               (assoc op :type :ok, :value)))
        (catch java.util.concurrent.ExecutionException e
          (throw (util/ex-root-cause e)))
        (catch [:type :clj-http.client/unexceptional-status] e
          (assoc op :type :fail, :error (:body e)))
        (catch java.net.SocketTimeoutException _
          (assoc op :type :fail, :error :timeout))
        (catch java.net.ConnectException _
          (assoc op :type :fail, :error :connection-refused)))

      (client/invoke! client test op)))

  (teardown! [this test]
    (client/teardown! client test))

  (close! [this test]
    (client/close! client test))

  client/Reusable
  (reusable? [this test]
             (client/reusable? client test)))

(defn client
  "Wraps a Redpanda client in one that has a different debug-topic-partitions
  implementation."
  [client]
  (Client. client))

(defn workload
  "Constructs a test workload given CLI options. Options are documented in the
  Redpanda client and jepsen.tests.kafka/workload docs."
  [opts]
  (-> (k/workload opts)
      (assoc :client (->> (rq/client)
                          client
                          (role/restrict-client :bufstream)))
      (update :checker checker)
      ; We don't support debugging topic-partitions yet
      ;(update :generator (partial gen/filter except-debug-partitions))
      ;(update :final-generator (partial gen/filter except-debug-partitions))
      ))
