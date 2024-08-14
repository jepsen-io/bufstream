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

(defn workload
  "Constructs a test workload given CLI options. Options are documented in the
  Redpanda client and jepsen.tests.kafka/workload docs."
  [opts]
  (assoc (k/workload opts)
         :client (role/restrict-client (rq/client) :bufstream)))
