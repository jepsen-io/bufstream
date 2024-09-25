(ns jepsen.bufstream.cli
  "Command-line entry point for Bufstream tests"
  (:require [clojure [string :as str]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [generator :as gen]
                    [nemesis :as nemesis]
                    [os :as os]
                    [tests :as tests]
                    [util :as util]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.bufstream [core :as core]
                              [db :as db]
                              [nemesis :as bufstream.nemesis]]
            [jepsen.bufstream.workload [early-read :as early-read]
                                       [producer-fence :as producer-fence]
                                       [producer-perf :as producer-perf]
                                       [queue :as queue]]
            [jepsen.os.debian :as debian]))

(def workloads
  "A map of workload names to functions that take CLI options and return
  workload maps"
  {:none           (constantly tests/noop-test)
   :early-read     early-read/workload
   :producer-fence producer-fence/workload
   :producer-perf  producer-perf/workload
   :queue          queue/workload})

(def all-workloads
  "All the workloads we run by default."
  [:queue])

(def nemeses
  "Basic nemeses we have available."
  #{
    :kill
    :kill-bufstream
    :kill-storage
    :kill-coordination
    :pause
    :pause-bufstream
    :pause-storage
    :pause-coordination
    :partition
    :partition-bufstream
    :partition-bufstream-coordination
    :partition-bufstream-storage
    :clock
    :clock-bufstream
    :clock-storage
    :clock-coordination
    })

(def all-nemeses
  "Combinations of nemeses we run by default."
  [[]
   [:partition]
   [:kill]
   [:pause]
   [:clock]
   [:partition :pause :kill :clock]])

(def special-nemeses
  "A map of special nemesis names to collections of faults."
  {:none []
   :all [:partition :pause :kill]})

(defn parse-comma-kws
  "Takes a comma-separated string and returns a collection of keywords."
  [spec]
  (->> (str/split spec #",")
       (remove #{""})
       (map keyword)))

(defn parse-nemesis-spec
  "Takes a comma-separated nemesis string and returns a collection of keyword
  faults."
  [spec]
  (->> (parse-comma-kws spec)
       (mapcat #(get special-nemeses % [%]))
       set))

(def logging-overrides
  "New logging levels for various Kafka packages--otherwise this test is going
  to be NOISY"
  {"org.apache.kafka.clients.FetchSessionHandler"                    :warn
   ; This complains about invalid topics during partitions, too
   "org.apache.kafka.clients.Metadata"                               :off
   ; This is going to give us all kinds of NOT_CONTROLLER or
   ; UNKNOWN_SERVER_ERROR messages during partitions
   "org.apache.kafka.clients.NetworkClient"                          :error
   "org.apache.kafka.clients.admin.AdminClientConfig"                :warn
   "org.apache.kafka.clients.admin.KafkaAdminClient"                 :warn
   "org.apache.kafka.clients.admin.internals.AdminMetadataManager"   :warn
   "org.apache.kafka.common.telemetry.internals.KafkaMetricsCollector" :warn
   "org.apache.kafka.clients.consumer.ConsumerConfig"                :warn
   "org.apache.kafka.clients.consumer.internals.ConsumerCoordinator" :warn
   "org.apache.kafka.clients.consumer.internals.ConsumerUtils"       :warn
   "org.apache.kafka.clients.consumer.internals.LegacyKafkaConsumer" :warn
   ; This is also going to kvetch about unknown topic/partitions when listing
   ; offsets
   "org.apache.kafka.clients.consumer.internals.Fetcher"             :error
   "org.apache.kafka.clients.consumer.internals.SubscriptionState"   :warn
   "org.apache.kafka.clients.consumer.KafkaConsumer"                 :warn
   "org.apache.kafka.clients.producer.KafkaProducer"                 :warn
   ; Comment this to see the config opts for producers
   "org.apache.kafka.clients.producer.ProducerConfig"                :warn
   ; We're gonna get messages constantly about NOT_LEADER_OR_FOLLOWER whenever
   ; we create a topic, and it's also going to complain when trying to send to
   ; paused brokers that they're not available
   "org.apache.kafka.clients.producer.internals.Sender"              :off
   "org.apache.kafka.clients.producer.internals.TransactionManager"  :warn
   "org.apache.kafka.common.metrics.Metrics"                         :warn
   "org.apache.kafka.common.utils.AppInfoParser"                     :warn
   })

(defn stats-checker
  "A modified version of the stats checker which doesn't care if :crash or
  :debug-topic-partitions ops always crash."
  []
  (let [c (checker/stats)]
    (reify checker/Checker
      (check [this test history opts]
        (let [res (checker/check c test history opts)]
          (if (every? :valid? (vals (dissoc (:by-f res)
                                            :debug-topic-partitions
                                            :crash)))
            (assoc res :valid? true)
            res))))))

(defn ignore-some-errors-checker
  "Wraps the workload checker, ignoring specific errors like :unseen. Takes a
  set of errors and a checker to wrap."
  [ignore checker]
  (reify checker/Checker
    (check [this test history opts]
      (let [res (checker/check checker test history opts)]
        (if (seq (remove ignore (:bad-error-types res)))
          res
          (assoc res :valid? true))))))

(defn test-name
  "Takes CLI options and constructs a test name as a string."
  [opts]
  (str (:bin opts)
       " " (name (:workload opts))
       (when (:txn? opts) " txn")
       (when-let [i (:isolation-level opts)]
         (str " " (case i
           "read_committed" "rc"
           "read_uncommitted" "ru"
           i)))
       " "
       (->> opts :sub-via (map name) sort (str/join ","))
       (when-let [acks (:acks opts)] (str " acks=" acks))
       (when-let [r (:retries opts)] (str " retries=" r))
       (when-let [aor (:auto-offset-reset opts)]
         (str " aor=" aor))
       (when (contains?
               opts :enable-server-auto-create-topics)
         (str " auto-topics=" (:enable-server-auto-create-topics opts)))
       (when (contains? opts :idempotence)
         (str " idem=" (:idempotence opts)))
       (when-let [n (:nemesis opts)]
         (str " " (->> n (map name) sort (str/join ","))))))

(defn bufstream-test
  "Takes CLI options and constructs a Jepsen test map"
  [opts]
  (let [workload-name (:workload opts)
        workload      ((workloads workload-name) opts)
        db            (db/db)
        os            debian/os
        nemesis       (bufstream.nemesis/package
                        {:db db
                         :nodes (:nodes opts)
                         :faults (:nemesis opts)
                         :partition {:targets [:one :majority]}
                         :pause {:targets [:one :majority :all]}
                         :kill {:targets [:one :majority :all]}
                         :stable-period (:nemesis-stable-period opts)
                         :interval (:nemesis-interval opts)})
        ; Main workload
        generator (gen/phases
                    (->> (:generator workload)
                         (gen/stagger (/ (:rate opts)))
                         (gen/nemesis
                           (gen/phases
                             ; Causing faults early in the test seems to render
                             ; the cluster permanently unusable; we'll give it
                             ; a bit to run.
                             (gen/sleep 5)
                             (:generator nemesis)))
                         (gen/time-limit (:time-limit opts)))
                    ; We always run the nemesis final generator; it makes
                    ; it easier to do ad-hoc analysis of a running cluster
                    ; after the test
                    (gen/nemesis (:final-generator nemesis)))
        ; With final generator, if present
        generator (if-let [fg (:final-generator workload)]
                    (gen/phases
                      generator
                      (gen/log "Waiting for recovery")
                      (gen/sleep 10)
                      (gen/time-limit (:final-time-limit opts)
                                      (gen/clients fg)))
                    generator)]
    (merge tests/noop-test
           opts
           {:name     (test-name opts)
            :roles    (core/roles (:nodes opts))
            :os       os
            :db       db
            :plot     {:nemeses (:perf nemesis)}
            :checker  (checker/compose
                        {:perf       (checker/perf)
                         :clock      (checker/clock-plot)
                         :stats      (stats-checker)
                         :exceptions (checker/unhandled-exceptions)
                         :workload   (ignore-some-errors-checker
                                       (:ignore-queue-errors opts)
                                       (:checker workload))})
            :client    (:client workload)
            :nemesis   (:nemesis nemesis nemesis/noop)
            :generator generator
            :logging   {:overrides logging-overrides}
            })))

(def cli-opts
  "Command-line option specification"
  [[nil "--abort-p PROBABILITY" "Probability of aborting a transaction at any given step."
    :default 0.1
    :parse-fn read-string
    :validate [#(<= 0 % 1) "must be between 0 and 1, inclusive"]]

   [nil "--acks ACKS" "What level of acknowledgement should our producers use? Default is unset (uses client default); try 1 or 'all'."
    :default nil]

   [nil "--no-archive" "Disables bufstream archiving; helpful for debugging purposes."]

   [nil "--auto-offset-reset BEHAVIOR" "How should consumers handle it when there's no initial offset in Kafka?"
   :default nil]

   ["-b" "--bin BINARY" "The Bufstream binary to run."
    :default "bufstream"]

   [nil "--bufstream-log-level LEVEL" "The logging level to give Bufstream"
    :default "INFO"]

   [nil "--crash-clients" "If set, periodically crashes clients and forces them to set up fresh consumers/producers/etc."
    :id :crash-clients?
    :default false]

   [nil "--crash-client-interval" "Roughly how long in seconds does a single client get to run for before crashing?"
    :default 30
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "must be a positive number"]]

   [nil "--disable-auto-commit" "If set, disables automatic commits via Kafka consumers. If not provided, uses the client default."
    :assoc-fn (fn [m _ _] (assoc m :enable-auto-commit false))]

   [nil "--enable-auto-commit" "If set, disables automatic commits via Kafka consumers. If not provided, uses the client default."
    :default  nil
    :assoc-fn (fn [m _ _] (assoc m :enable-auto-commit true))]

   [nil "--etcd-version VERSION" "What version of etcd should we install?"
    :default "3.5.15"]

   [nil "--[no-]fetch-eager" "Should we enable kafka.fetch_eager in the Bufstream settings?"
    :default true]

   [nil "--[no-]fetch-sync" "Should we enable kafka.fetch_sync in the Bufstream settings?"
    :default true]

   [nil "--final-time-limit SECONDS" "How long should we run the final generator for, at most? In seconds."
    :default  200
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "must be a positive number"]]

   [nil "--[no-]idempotence" "If true, asks producers to enable idempotence. If omitted, uses client defaults."]

   [nil "--isolation-level NAME" "What isolation level should we request for consumers? e.g. read_committed"]

   [nil "--ignore-queue-errors TYPES" "A comma-separate list of errors to ignore from the queue workload checker. Helpful for ignoring :unseen, which is a constant problem right now."
    :default #{}
    :parse-fn (comp set parse-comma-kws)]

   [nil "--max-writes-per-key NUM" "Maximum number of writes to any given key."
    :default  1024
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer."]]

   [nil "--nemesis FAULTS" "A comma-separated list of nemesis faults to enable"
    :parse-fn parse-nemesis-spec
    :validate [(partial every? (into nemeses (keys special-nemeses)))
               (str (cli/one-of nemeses) " or the special nemeses, which " (cli/one-of special-nemeses))]]

   [nil "--nemesis-interval SECS" "Roughly how long between nemesis operations."
    :default  10
    :parse-fn read-string
    :validate [pos? "Must be a positive number."]]

   [nil "--nemesis-stable-period SECS" "If given, rotates the mixture of nemesis faults over time with roughly this period."
    :default 30
    :parse-fn parse-long
    :validate [pos? "Must be a positive number."]]

   ["-r" "--rate HZ" "Approximate request rate, in hz"
    :default 10000
    :parse-fn read-string
    :validate [pos? "Must be a positive number."]]

   [nil "--retries COUNT" "Producer retries. If omitted, uses client default."
    :parse-fn util/parse-long]

   ["-s" "--safe" "Runs with the safest settings: --disable-auto-commit, --disable-server-auto-create-topics, --acks all, --retries 1000, --idempotence, --isolation-level read_committed --auto-offset-reset earliest, --sub-via assign. You can override individual settings by following -s with additional arguments, like so: -s --acks 0"
    :assoc-fn (fn [m _ _]
                (assoc m
                       :acks "all"
                       :auto-offset-reset "earliest"
                       :enable-auto-commit false
                       :enable-server-auto-create-topics false
                       :idempotence true
                       :isolation-level "read_committed"
                       :retries 1000
                       :sub-via #{:assign}))]

   [nil "--sub-p PROBABILITY" "Probability any given op is an assign/subscribe operation."
    ; For completely inexplicable reasons, more frequent calls to assign or
    ; subscribe (e.g. if this is 1/32 or 1/8) can cause producers (yes,
    ; producers! not consumers!) to time out consistently for some period, then
    ; go back to being normal. If you're seeing periods on the graph where
    ; performance just TANKS for no reason, try adjusting this smaller.
    :default 1/64
    :parse-fn read-string
    :validate [#(< 0 % 1) "Must be between 0 and 1"]]

   [nil "--sub-via STRATEGIES" "A comma-separated list like `assign,subscribe`, which denotes how we ask clients to assign topics to themselves."
    :default #{:subscribe}
    :parse-fn (comp set parse-comma-kws)
    :validate [#(every? #{:assign :subscribe} %)
               "Can only be assign and/or subscribe"]]

   [nil "--[no-]txn" "Enables transactions for the queue workload."
    :id :txn?]

   [nil "--tcpdump" "Dumps traffic to a pcap file."]

   ["-v" "--version STRING" "What version of Datomic should we install?"
    :default "1.0.7075"]

   ["-w" "--workload NAME" "What workload should we run?"
    :parse-fn keyword
    :default  :queue
    :missing  (str "Must specify a workload: " (cli/one-of workloads))
    :validate [workloads (cli/one-of workloads)]]

   [nil "--[no-]ww-deps" "Enables or disables support for write-write dependency inference based on offsets in the queue workload. Redpanda allows write cycles everywhere. Disabling ww edges is helpful in hunting for pure wr cycles."
   :default true]
   ])

(defn all-tests
  "Turns CLI options into a sequence of tests."
  [opts]
  (let [nemeses   (if-let [n (:nemesis opts)]  [n] all-nemeses)
        workloads (if-let [w (:workload opts)] [w] all-workloads)
        txns      (if-let [t (:txn? opts)]     [t] [false true])]
    (for [i     (range (:test-count opts))
          n     nemeses
          w     workloads
          t     txns]
      (bufstream-test (assoc opts
                             :txn?     t
                             :nemesis  n
                             :workload w)))))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  bufstream-test
                                         :opt-spec cli-opts})
                   (cli/test-all-cmd {:tests-fn all-tests
                                      :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
