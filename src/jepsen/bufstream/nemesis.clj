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

(defn dep-part-package
  "A nemesis package for partitions between bufstream and dependencies.
  Supports two faults: :partition-bufstream-coordination, and
  :partition-bufstream-storage. Asking for a general :partition fault activates
  both."
  [{:keys [faults db] :as opts}]
  (let [needed? (or (:partition faults)
                    (:partition-bufstream-coordination faults)
                    (:partition-bufstream-storage faults))
        stop {:type :info, :f :stop-partition}
        ; Constructs a generator for start-partition operations, partitioning
        ; bufstream from the given role.
        start (fn [role]
                 (fn gen [test ctx]
                   {:type :info
                    :f    :start-partition
                    :value (n/complete-grudge
                             [(role/nodes test role)
                              (util/random-nonempty-subset
                                (role/nodes test :bufstream))])}))
        gen (->> [(gen/repeat stop)
                  (when (or (:partition faults)
                            (:partition-bufstream-coordination faults))
                    (start :coordination))
                  (when (or (:partition faults)
                            (:partition-bufstream-storage faults))
                    (start :storage))]
                 gen/mix
                 (gen/stagger (:interval opts nc/default-interval)))]
    {:generator       (when needed? gen)
     :final-generator (when needed? stop)
     :nemesis         (nc/partition-nemesis db)
     :perf            #{{:name  "partition"
                         :start #{:start-partition}
                         :stop  #{:stop-partition}
                         :color "#E9DCA0"}}
     ; We assume, in package-gen-helper, that every package has a role. Let's
     ; just call this bufstream.
     :role            :bufstream}))

(defn package-gen-helper
  "Helper for package-gen. Takes a collection of packages and draws a random
  nonempty subset of them."
  [packages]
  (when (seq packages)
    (let [; Start by picking the roles that we'll affect
          roles (set (util/random-nonempty-subset [:coordination
                                                   :storage
                                                   :bufstream]))
          pkgs (->> packages
                    ; Just those belonging to one of our roles
                    (filter (comp roles :role))
                    ; And pick a random subset of those
                    util/random-nonempty-subset
                    vec)]
      ; If we drew nothing, try again.
      (if (seq pkgs)
        pkgs
        (do ; (info "no draw, retrying")
            (recur packages))))))

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
      (let [; Pick packages
            pkgs (if (< (rand) 1/4)
                   ; Roughly 1/4 of the time, pick no pkgs
                    []
                    (package-gen-helper packages))
            ; Construct combined generators
            gen       (if (seq pkgs)
                        (apply gen/any (map :generator pkgs))
                        (gen/sleep period))
            final-gen (keep :final-generator pkgs)]
        ; Ops from the combined generator, followed by a final gen
        [(gen/log (str "Shifting to new mix of nemeses: "
                       (pr-str (map (comp n/fs :nemesis) pkgs))))
         (gen/time-limit period gen)
         final-gen]))))

(defn role-faults
  "Takes a set of faults like #{:pause-bufstream}, and returns faults for a
  specific role like #{:pause}.

  Some faults, like :partition, make no sense applied to single-node roles;
  they are omitted.

  General faults, like :pause or :kill, apply to every role.

  Role-specific faults are suffixed with -bufstream or -kill; :kill-storage
  maps to :kill iff the role is :storage."
  [faults role]
  (->> faults
       (keep (fn [fault]
               (if-let [[m fault role']
                        (re-find #"^(\w+)-(bufstream|coordination|storage)$"
                                 (name fault))]
                 ; This is a role-specific fault
                 (when (= (name role) role')
                   (keyword fault))
                 ; A general fault
                 (case role
                   ; Bufstream can do eveything
                   :bufstream fault
                   ; Coord and storage are single nodes and can't partition.
                   (:coordination, :storage)
                   (when (not= :partition fault)
                     fault)))))
       set))

(defn update-role-faults
  "Takes CLI options and updates :faults to be specific to the given role."
  [role opts]
  (update opts :faults role-faults role))

(defn package
  "Takes CLI opts. Constructs a nemesis and generator for the test."
  [opts]
  (let [opts (update opts :faults set)
        dep-opts (-> opts
                     ; There's no sense in targeting anything other
                     ; than all nodes, since these are single-node subsystems
                     (assoc-in [:pause :targets] [:all])
                     (assoc-in [:kill :targets] [:all]))
        packages
        (->> (concat
               ; Bufstream
               (->> opts
                    (update-role-faults :bufstream)
                    nc/nemesis-packages
                    (map (partial role/restrict-nemesis-package :bufstream)))
               ; Storage
               (->> dep-opts
                    (update-role-faults :storage)
                    nc/nemesis-packages
                    (map (partial role/restrict-nemesis-package :storage)))
               ; Coordinator
               (->> dep-opts
                    (update-role-faults :coordination)
                    nc/nemesis-packages
                    (map (partial role/restrict-nemesis-package :coordination)))
               ; Custom packages
               [(dep-part-package opts)])
             (filter :generator))

        nsp (:stable-period opts)]
    (info :packages (map (comp n/fs :nemesis) packages))

    (cond-> (nc/compose-packages packages)
      nsp (assoc :generator (package-gen nsp packages)))))
