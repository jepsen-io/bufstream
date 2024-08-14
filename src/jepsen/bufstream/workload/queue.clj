(ns jepsen.bufstream.workload.queue
  (:require [clojure [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [util :as util]]
            [jepsen.redpanda.workload.queue :as rq]
            [jepsen.tests.kafka :as k]))

(defn workload
  "Constructs a test workload given CLI options."
  [opts]
  (assoc (k/workload opts)
         :client (rq/client)))
