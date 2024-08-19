(ns jepsen.bufstream.workload.early-read
  "This workload targets a particular behavior we found in the queue tests,
  where we found G1c cycles composed entirely of write-read edges. In short, it
  appears as if clients can poll values in a transaction before that
  transaction commits."
  (:require [clojure [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [loopr]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]
                    [util :as util]
                    [role :as role]]
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

(defn poll
  "Keeps polling until nothing comes back. Returns a map of {:values [...],
  :offsets [...].}"
 [consumer]
 (loop [offsets []
        values  []]
     (let [records    (.poll consumer (rc/ms->duration rq/poll-ms))
           k-records  (.records records (rq/k->topic-partition k))
           offsets    (into offsets  (mapv (fn [^ConsumerRecord record]
                                             (.offset record))
                                           k-records))
           values     (into values   (mapv (fn [^ConsumerRecord record]
                                             (.value record))
                                           k-records))]
       ; We use a random factor here so so we have a chance of *not* reading
       ; everything.
       (if (and (seq k-records)) ;(< (rand) 0.5))
         (recur offsets values)
         {:offsets offsets
          :values  values}))))

(defrecord Client [; Our three Kafka clients
                   admin
                   ^KafkaProducer producer
                   ^KafkaConsumer consumer
                   ; atom of whether topic exists
                   topic-created?
                   ; Do we intend to poll or no?
                   poll?]
  client/Client
  (open! [this test node]
    (let [tx-id    (rc/new-transactional-id)
          poll?    (< (rand) 0.5)
          admin    (rc/admin test node)
          producer (rc/producer
                         (assoc test
                                :client-id (str "jepsen-" tx-id)
                                :transactional-id tx-id)
                         node)
          consumer (rc/consumer test node)]
      (info "Transactional ID" tx-id)

      ; Create topic on first run
      (locking topic-created?
        (when-not @topic-created?
          (rc/create-topic! admin
                            (rq/k->topic k)
                            rq/partition-count
                            rq/replication-factor)
          (reset! topic-created? true)))

      ; Assign ourselves the topic if we're going to poll
      (when poll?
        (.assign consumer [(rq/k->topic-partition k)]))

      (info "Client created")
      ; Construct ready client
      (assoc this
             :admin    admin
             :producer producer
             :consumer consumer
             :poll?    poll?)))

  (setup! [this test])

  (invoke! [this test op]
    ; Start txn
    (.beginTransaction producer)
    (let [; Preliminaries
          topic           (rq/k->topic k)
          partition       (rq/k->partition k)
          topic-partition (rq/k->topic-partition k)

          ; Send the value of the op
          sent   (:value op)
          record (rc/producer-record topic partition nil sent)
          res    (-> producer
                     (.send record)
                     (deref 10000 nil)
                     (or (throw+ {:type :timeout})))
          send-offset (when (.hasOffset res)
                        (.offset res))
          ; Sleep a short while to make it easier to catch
          ;_ (Thread/sleep (rand-int 100))
          ; Then poll. We only poll sometimes; most txns just send to advance
          ; the partition.
          polled          (when poll?
                            (poll consumer))
          polled-offsets  (:offsets polled)
          polled-values   (:values polled)
          ; Send offsets to txn.
          offsets (when (seq polled-offsets)
                    ; Note that we actually have to send one *higher* than the
                    ; highest offset consumed
                    {topic-partition
                     (rc/offset+metadata (inc (reduce max -1 polled-offsets)))})
          _ (when (seq offsets)
              ; TODO: try (.groupMetadata consumer), the new API
              (.sendOffsetsToTransaction producer
                                         offsets
                                         rc/consumer-group))]
      ; Commit txn
      (.commitTransaction producer)
      ; Return completed op
      (assoc op
             :type :ok
             :value {:sent   sent
                     :polled polled-values})))

  (teardown! [this test]
    )

  (close! [this test]
    (rc/close! admin)
    (rc/close-producer! producer)
    (rc/close-consumer! consumer))

  client/Reusable
  (reusable? [this test]
    ; Just to be safe, we never let ourselves re-use a client once an exception
    ; is thrown
    false))

(defn client
  []
  (map->Client {:topic-created? (atom false)}))

(defn sent-index
  "Takes a set of txn ok ops and builds an index map of sent values to the
  completion ops which wrote them."
  [ops]
  (loopr [index (transient {})]
         [op ops]
         (recur (assoc! index (:sent (:value op)) op))
         (persistent! index)))

(defn checker
  "A checker that looks for a very small early read cycle: a pair of
  transactions such that T1 observes T2's sent value, and vice versa."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [ops        (->> history
                            h/oks
                            (h/filter (h/has-f? :txn)))
            sent-index (sent-index ops)]
        ; Loop over the history, and each polled value, and try to see if the
        ; antecedent op that wrote that polled value also depended on us. Note
        ; we only looks for cycles of length 2, not anything larger--we're
        ; going to miss a lot.
        (loopr [errors  []
                ignore  #{}] ; Ops we've already included in errors
               [op     ops
                polled (or (:polled (:value op)) [])]
               ; The opt that wrote the polled value we're looking at
               (if-let [writer (get sent-index polled)]
                 (let [sent (:sent (:value op))]
                   (if (and (not (ignore op))
                            (some #{sent} (:polled (:value writer))))
                     ; The writer polled our write!
                     (recur (conj errors
                                  [op writer])

                            (conj ignore op writer))
                     ; Fine
                     (recur errors ignore)))
                 ; Might have been an indefinite writer and we only index OKs,
                 ; whatever
                 (recur errors ignore))
               {:valid? (empty? errors)
                :count (count errors)
                :errors errors})))))

(defn workload
  "Takes CLI opts and constructs a workload."
  [opts]
  {:generator (->> (range)
                   (map (fn [x]
                          {:type  :invoke
                           :f     :txn
                           :value x})))
   :client  (role/restrict-client :bufstream (client))
   :checker (checker)})
