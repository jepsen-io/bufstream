(ns jepsen.bufstream.nemesis
  "Fault injection for Bufstream"
  (:require [clojure [set :as set]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [cheshire.core :as json]
            [dom-top.core :refer [real-pmap]]
            [jepsen [control :as c]
                    [nemesis :as n]
                    [generator :as gen]
                    [net :as net]
                    [util :as util]
                    [role :as role]]
            [jepsen.nemesis.combined :as nc]
            [slingshot.slingshot :refer [try+ throw+]]))

(defn package-gen
  "For long-running tests, it's nice to be able to have periods of no faults,
  periods with lots of faults, just one kind of fault, etc. This takes a time
  period in seconds, which is how long to emit nemesis operations for a
  particular subset of packages. Takes a collection of packages. Constructs a
  nemesis generator which emits faults for a shifting collection of packages
  over time."
  [period packages]
  ; We want a sequence of random subsets of packages
  (repeatedly
    (fn rand-pkgs []
      (let [; Start by picking the roles that we'll affect
            roles (set (util/random-nonempty-subset [:coordination
                                                     :storage
                                                     :bufstream]))
            ; Roughly 1/4 of the time, pick no roles
            roles (if (< (rand) 1/4)
                    #{}
                    roles)
            pkgs (->> packages
                      ; Just those with generators
                      (keep :generator)
                      ; And belonging to one of our roles
                      (filter (comp roles :role))
                      ; And pick a random subset of those
                      util/random-nonempty-subset
                      vec)
            ; Construct combined generators
            gen       (if (seq pkgs)
                        (apply gen/any (map :generator pkgs))
                        (gen/sleep period))
            final-gen (keep :final-generator pkgs)]
        ; Ops from the combined generator, followed by a final gen
        [(gen/log (str "Shifting to new mix of roles " (pr-str roles)
                       " and " (count pkgs) " nemeses:
                       " (pr-str (map :nemesis pkgs))))
         (gen/time-limit period gen)
         final-gen]))))

(defn package
  "Takes CLI opts. Constructs a nemesis and generator for the test."
  [opts]
  (let [opts (update opts :faults set)
        dep-opts (-> opts
                     ; There's no sense in causing partitions, corruption, or
                     ; clock skew on the single-node etcd/minio nodes; we're
                     ; only interested in their availability. We limit
                     ; ourselves to just pauses and kills for those.
                     (update :faults set/intersection #{:pause :kill})
                     ; Likewise, there's no sense in targeting anything other
                     ; than all nodes, since these are single-node subsystems
                     (assoc-in [:pause :targets] [:all])
                     (assoc-in [:kill :targets] [:all]))
        packages
        (->> (concat ; Standard faults, scoped only to bufstream
                     (map (partial role/restrict-nemesis-package :bufstream)
                          (nc/nemesis-packages opts))
                     ; Storage faults
                     (map (partial role/restrict-nemesis-package :storage)
                          (nc/nemesis-packages dep-opts))
                     ; Coordinator faults
                     (map (partial role/restrict-nemesis-package :coordination)
                          (nc/nemesis-packages dep-opts))
                     ; Custom packages
                     [])
             (filter :generator))
        nsp (:stable-period opts)]
    (cond-> (nc/compose-packages packages)
      nsp (assoc :generator (package-gen nsp packages)))))
