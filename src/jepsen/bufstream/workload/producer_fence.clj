(ns jepsen.bufstream.workload.producer-fence
  "Tries to narrow down the cause of a strange error message from
  freshly-created producers. They seem to log errors like:

      org.apache.kafka.common.errors.ProducerFencedException: There is a newer producer with the same transactionalId which fences the current one.

  This test is structured a little differently than most clients. Rather than
  focus on sending and polling, we have small, explicit operations for every
  stage of a client's operation. We do nothing on client/open! except open an
  admin client and ensure a topic exists. We make producer opening an explicit
  open! operation at the start of a process' operations with an
  explicitly-provided transactional ID. Then we begin transactions, send
  values, and commit. There are no polls, and no consumer.

      {:f :open,          :value {:transactional-id 1}}
      {:f :init-txns      :value nil}
      {:f :begin,         :value nil}
      {:f :send,          :value 5}
      {:f :commit         :value nil}

  To crash a process, we use {:f :crash}."
  (:require [clojure [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [loopr]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]
                    [util :as util]
                    [role :as role]]
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

(defrecord Client [; Our two Kafka clients. Admin is a regular KafkaAdmin.
                   admin
                   ; Producer is a promise; we fill it in later.
                   ^KafkaProducer producer
                   ; Atom: did we create the topic yet?
                   topic-created?
                   ; What node are we?
                   node
                   ]
  client/Client
  (open! [this test node]
    ; Only connect the admin client; producer is explicitly an invoke op.
    (let [admin (rc/admin test node)]
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
             :producer (promise)
             :node     node)))

  (setup! [this test])

  (invoke! [this test {:keys [f value] :as op}]
    (case f
      ; Open a fresh producer
      :open
      (if (realized? producer)
        (assoc op :type :fail, :error :already-open)
        (let [tx-id (:transactional-id value)]
          ; Note we use produce*, which does NOT initialize txns
          (deliver producer (rc/producer*
                              (assoc test
                                     :client-id         (str "jepsen-" tx-id)
                                     :transactional-id  tx-id
                                     :txn?              true)
                              node))
          (assoc op :type :ok)))

      :init-txns
      (do (.initTransactions @producer)
          (assoc op :type :ok))

      :begin
      (do (.beginTransaction @producer)
          (assoc op :type :ok))

      :send
      (let [topic (rq/k->topic k)
            partition (rq/k->partition k)]
          (-> @producer
              (.send (rc/producer-record topic partition nil value))
              (deref 10000 nil)
              (or (throw+ {:type :timeout})))
          (assoc op :type :ok))

      :commit
      (do (.commitTransaction @producer)
          (assoc op :type :ok))

      :crash
      (assoc op :type :info)))

  (teardown! [this test])

  (close! [this test]
    (rc/close! admin)
    (when-let [p (deref producer 0 nil)]
      (rc/close-producer! p)))

  client/Reusable
  (reusable? [this test]
    ; Just to be safe, we never let ourselves re-use a client once an exception
    ; is thrown
    false))

(defn client
  []
  (map->Client {:topic-created? (atom false)}))

; Checker
(defn fenced?
  "Did this op throw a ProducerFencedException?"
  [op]
  (when-let [e (:exception op)]
    (-> e
        :via
        first
        :type
        (= 'org.apache.kafka.common.errors.ProducerFencedException))))

(defn checker
  "Looks for ProducerFencedExceptions thrown by any op."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [errs (->> history
                      (h/filter fenced?)
                      (into []))]
        {:valid?        (empty? errs)
         :error-count   (count errs)
         :errors        errs}))))

; Generator

(defrecord EachProcess
  [; A fresh copy of the generator we start with for each process
   fresh-gen
   ; A promise of a map of threads to the context filters for those particular
   ; threads, lazily initialized.
   context-filters
   ; A map of thread -> process for processes that are currently initialized
   ; and running
   extant
   ; A map of threads to generators
   gens]

  gen/Generator
  (op [this test ctx]
    (gen/each-thread-ensure-context-filters! context-filters ctx)
    (let [{:keys [op gen' extant' thread] :as soonest}
          (->> (context/free-threads ctx)
               (keep (fn [thread]
                       (let [extant-process (get extant thread ::not-found)
                             new-process    (context/thread->process ctx thread)
                             ; Is this a process we haven't initialized yet?
                             new?           (not= extant-process new-process)
                             ; Maybe inefficient--we might discard most of these
                             extant'        (if new?
                                              (assoc extant thread new-process)
                                              extant)
                             gen            (if new?
                                              fresh-gen
                                              (get gens thread))
                             ; Give this generator a context *just* for one
                             ; thread
                             ctx     ((@context-filters thread) ctx)]
                         ; Generate an op
                         (when-let [[op gen'] (gen/op gen test ctx)]
                           {:op      op
                            :gen'    gen'
                            :thread  thread
                            :extant' extant'}))))
               (reduce gen/soonest-op-map nil))]
      (cond ; A free thread has an operation
            soonest [op (EachProcess. fresh-gen context-filters extant'
                                     (assoc gens thread gen'))]

            ; Some thread is busy; we can't tell what to do just yet
            (not= (context/free-thread-count ctx)
                  (context/all-thread-count ctx))
            [:pending this]

            ; Every thread is exhausted
            true
            nil)))

  (update [this test ctx event]
    (gen/each-thread-ensure-context-filters! context-filters ctx)
    (let [process (:process event)
          thread (context/process->thread ctx process)
          gen    (get gens thread fresh-gen)
          ctx    ((@context-filters thread) ctx)
          gen'   (gen/update gen test ctx event)]
      (EachProcess. fresh-gen context-filters extant
                    (assoc gens thread gen')))))

(defn each-process
  "Takes a generator. Constructs a generator which maintains independent copies
  of that generator for every process. Each generator sees exactly one thread &
  process in its free process list. Updates are propagated to the generator for
  the thread which emitted the operation."
  [gen]
  (EachProcess. gen (promise) {} {}))

(defrecord WithValueSeq [next-value gen]
  gen/Generator
  (op [this test ctx]
    (when-let [[op gen'] (gen/op gen test (assoc ctx ::next-value next-value))]
      [op (WithValueSeq. (if (= (:f op) :send)
                                 (inc next-value)
                                 next-value)
                         gen')]))

  (update [this test ctx event]
    (WithValueSeq. next-value (gen/update gen test ctx event))))

(defn with-value-seq
  "Wraps a generator. Keeps an auto-incrementing ::next-value in the generator
  context."
  ([gen]
   (with-value-seq gen 0))
  ([gen next-value]
   (WithValueSeq. next-value gen)))

(defn until-fenced
  "Wraps a generator, running until it hits a fenced error."
  [gen]
  (gen/on-update
    (fn [gen test ctx event]
      (let [gen' (gen/update gen test ctx event)]
        (if (fenced? event)
          (gen/limit 10 gen') ; We caught it, wind down
          (until-fenced gen'))))
    gen))

(defn txn-gen
  "Generates a single txn."
  []
  [{:f :begin}
   (gen/once (fn [test ctx]
               {:f :send, :value (::next-value ctx)}))
   {:f :commit}])

(defn open-gen
  "A generator that emits an :open op asking for a specific transactional ID."
  []
  (reify gen/Generator
    (update [this test ctx event] this)
    (op [this test ctx]
      (when-let [[op _] (gen/op {:f :open} test ctx)]
        (if (identical? :pending op)
          [:pending this]
          (let [process (:process op)
                thread (context/process->thread ctx process)]
            [(assoc op :value {:transactional-id (str "jt-" process)})]))))))

(defn gen
  "Generator."
  []
  (->> [(open-gen)
        {:f :init-txns}
        ; (txn-gen) ; One txn
        (repeatedly txn-gen) ; Many txns
        {:f :crash}]
       each-process
       with-value-seq
       until-fenced))

(defrecord OnGivenProcessThreads [ops]
  gen/Generator
  (op [this test ctx]
    (when-let [op (first ops)]
      (let [p (:process op)
            t (context/process->thread ctx p)]
        (if (context/thread-free? ctx t)
          [(gen/fill-in-op op ctx) (OnGivenProcessThreads. (next ops))]
          [:pending this]))))

  (update [this test ctx event]
    this))

(defn on-given-process-threads
  "Takes a sequence of maps with specific :process fields--e.g. one drawn from
  an existing history you'd like to reproduce. Emits exactly that sequence of
  events on the threads corresponding to those processes, blocking until those
  threads are free.

  If operations complete exactly as they did in the original history, this
  produces a history with identical :process fields. If a process crashed in
  the original history but not in this execution, or vice versa, the processes
  may differ, but the concurrency structure remains the same."
  [ops]
  (OnGivenProcessThreads. ops))

(def handwritten-gen-1
  "A very small hand-coded history we know reproduces this issue"
  (on-given-process-threads
    [
     {:process 1, :f :open, :value {:transactional-id "jt-1"}}
     {:process 1, :f :init-txns}
     {:process 0, :f :open, :value {:transactional-id "jt-0"}}
     {:process 1, :f :begin}
     {:process 0, :f :init-txns}
     {:process 1, :f :send, :value 0}
     {:process 0, :f :begin}
     {:process 1, :f :commit}
     ]))

(def handwritten-gen-2
  "A very small hand-coded history we think reproduces this issue"
  (on-given-process-threads
    [
     {:process 0, :f :open, :value {:transactional-id "jt-1"}}
     {:process 0, :f :init-txns}
     {:process 1, :f :open, :value {:transactional-id "jt-0"}}
     {:process 1, :f :init-txns}
     {:process 0, :f :begin}
     {:process 0, :f :send, :value 0}
     {:process 1, :f :begin}
     {:process 0, :f :commit}
     ]))

(defn workload
  "Takes CLI opts and constructs a workload."
  [opts]
  {;:generator (gen)
   :generator handwritten-gen-2
   :client  (role/restrict-client :bufstream (client))
   :checker (checker/compose
              {:fenced   (checker)
               :timeline (timeline/html)})})
