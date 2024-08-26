(ns jepsen.bufstream.workload.producer-perf
  "We're seeing weird bimodal performance issues in the queue workload: the
  cluster's fine, then it abruptly flips into a mode where sends time out but
  polls are reasonably fast. That state persists for tens to thousands of
  seconds, then flips back. Intervals are random. Tests can start in either
  fast or slow mode.

  It happens on LXC containers. It happens on separate EC2 VMs. It happens with
  subscribes only, and assigns only. It does not require transactions. There's
  no obvious CPU, memory, GC, JVM, or disk bottleneck. Data volumes are small.
  It affects both Kafka and Bufstream. I'm starting to suspect a client-side
  behavior.

  This test tries to narrow that down. Our only ops are:

      {:f :send, :value 5}"
  (:require [clojure [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [loopr]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]
                    [util :as util]
                    [role :as role]]
            [jepsen.bufstream.workload.queue :as bq]
            [jepsen.checker.timeline :as timeline]
            [jepsen.generator.context :as context]
            [jepsen.redpanda.client :as rc]
            [jepsen.redpanda.workload.queue :as rq]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.util.concurrent ExecutionException)
           (org.apache.kafka.clients.admin Admin)
           (org.apache.kafka.clients.consumer ConsumerRecords
                                              ConsumerRecord
                                              KafkaConsumer)
           (org.apache.kafka.clients.producer KafkaProducer
                                              RecordMetadata)
           (org.apache.kafka.common KafkaException
                                    TopicPartition)
           (org.apache.kafka.common.errors AuthorizationException
                                           DisconnectException
                                           InterruptException
                                           InvalidProducerEpochException
                                           InvalidReplicationFactorException
                                           InvalidTopicException
                                           InvalidTxnStateException
                                           NetworkException
                                           NotControllerException
                                           NotLeaderOrFollowerException
                                           OutOfOrderSequenceException
                                           ProducerFencedException
                                           TimeoutException
                                           UnknownTopicOrPartitionException
                                           UnknownServerException
                                           )))

(def k
  "The key we use in this test."
  0)

(defrecord Client [; Our two Kafka clients.
                   admin
                   ^KafkaProducer producer
                   ; Atom: did we create the topic yet?
                   topic-created?
                   ]
  client/Client
  (open! [this test node]
    ; Only connect the admin client; producer is explicitly an invoke op.
    (let [admin (rc/admin test node)
          producer (rc/producer test node)]
      ; Create topic on first run
      (locking topic-created?
        (when-not @topic-created?
          (rc/create-topic! admin
                            (rq/k->topic k)
                            rq/partition-count
                            rq/replication-factor)
          (reset! topic-created? true)))
      (assoc this
             :admin    admin
             :producer producer)))

  (setup! [this test])

  (invoke! [this test {:keys [f value] :as op}]
    (case f
      :send
      (let [topic (rq/k->topic k)
            partition (rq/k->partition k)]
          (-> producer
              (.send (rc/producer-record topic partition nil value))
              (deref 10000 nil)
              (or (throw+ {:type :timeout})))
          (assoc op :type :ok))

      :crash
      (assoc op :type :info)))

  (teardown! [this test])

  (close! [this test]
    (rc/close! admin)
    (rc/close-producer! producer))

  client/Reusable
  (reusable? [this test]
    ; Just to be safe, we never let ourselves re-use a client once an exception
    ; is thrown
    false))

(defn client
  []
  (map->Client {:topic-created? (atom false)}))

(defn gen
  "Generator."
  []
  (->> (range)
       (map (fn [x]
              {:f :send, :value x}))))

;; OK, so THAT simple thing looks fine. Let's try building up to subsets of the
;; queue workload.

(defn wrap-gen
  "Transforms a queue generator."
  [gen]
  (->> gen
       (gen/filter (fn [op]
                     ; No give. Only throw.
                     ;(#{:send} (:f op))
                     (case (:f op)
                       :send true
                       :poll true
                       :assign (< (rand) 0.01))))))

(defn workload
  "Takes CLI opts and constructs a workload."
  [opts]
  ; Start with a queue workload
  (let [w (bq/workload opts)]
    (-> w
        (dissoc :final-generator)
        (update :generator wrap-gen))))
