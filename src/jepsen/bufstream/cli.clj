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
            [jepsen.os.debian :as debian]))

(def workloads
  "A map of workload names to functions that take CLI options and return
  workload maps"
  {:none       (constantly tests/noop-test)})

(def all-workloads
  "All the workloads we run by default."
  [])

(def all-nemeses
  "Combinations of nemeses we run by default."
  [[]
   [:partition]])

(def special-nemeses
  "A map of special nemesis names to collections of faults."
  {:none []
   :all [:partition]})

(defn parse-nemesis-spec
  "Takes a comma-separated nemesis string and returns a collection of keyword
  faults."
  [spec]
  (->> (str/split spec #",")
       (map keyword)
       (mapcat #(get special-nemeses % [%]))))

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
                         :interval (:nemesis-interval opts)})]
    (merge tests/noop-test
           opts
           {:name (str (name workload-name)
                       " " (str/join "," (map name (:nemesis opts))))
            :roles    (core/roles (:nodes opts))
            :os       os
            :db       db
            :plot     {:nemeses (:perf nemesis)}
            :checker  (checker/compose
                        {:perf       (checker/perf)
                        :clock      (checker/clock-plot)
                        :stats      (checker/stats)
                        :exceptions (checker/unhandled-exceptions)
                        :workload   (:checker workload)
                        })
            :client    (:client workload)
            :nemesis   (:nemesis nemesis nemesis/noop)
            :generator (->> (:generator workload)
                            (gen/stagger (/ (:rate opts)))
                            (gen/nemesis (:generator nemesis))
                            (gen/time-limit (:time-limit opts)))})))

(def cli-opts
  "Command-line option specification"
  [["-b" "--bin BINARY" "The Bufstream binary to run."
   :default "bufstream"]

   [nil "--etcd-version VERSION" "What version of etcd should we install?"
    :default "3.5.15"]

   [nil "--max-txn-length NUM" "Maximum number of operations in a transaction."
    :default  4
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--max-writes-per-key NUM" "Maximum number of writes to any given key."
    :default  32
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer."]]

   [nil "--min-txn-length NUM" "Minumum number of operations in a transaction."
    :default  1
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--nemesis FAULTS" "A comma-separated list of nemesis faults to enable"
    :parse-fn parse-nemesis-spec
    :validate [(partial every? #{:pause
                                 :kill
                                 :partition
                                 :clock})
               "Faults must be pause, kill, partition, partition-storage, clock, or the special faults all or none."]]

   [nil "--nemesis-interval SECS" "Roughly how long between nemesis operations."
    :default  10
    :parse-fn read-string
    :validate [pos? "Must be a positive number."]]

   [nil "--nemesis-stable-period SECS" "If given, rotates the mixture of nemesis faults over time with roughly this period."
    :default nil
    :parse-fn parse-long
    :validate [pos? "Must be a positive number."]]

   ["-r" "--rate HZ" "Approximate request rate, in hz"
    :default 100
    :parse-fn read-string
    :validate [pos? "Must be a positive number."]]

   ["-v" "--version STRING" "What version of Datomic should we install?"
    :default "1.0.7075"]

   ["-w" "--workload NAME" "What workload should we run?"
    :parse-fn keyword
    :default  :none
    :missing  (str "Must specify a workload: " (cli/one-of workloads))
    :validate [workloads (cli/one-of workloads)]]
   ])

(defn all-tests
  "Turns CLI options into a sequence of tests."
  [opts]
  (let [nemeses   (if-let [n (:nemesis opts)]  [n] all-nemeses)
        workloads (if-let [w (:workload opts)] [w] all-workloads)]
    (for [n     nemeses
          w     workloads
          sync  [true false]
          i     (range (:test-count opts))]
      (bufstream-test (assoc opts
                           :nemesis n
                           :workload w
                           :sync sync)))))

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
