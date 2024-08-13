(ns jepsen.bufstream.db
  (:require [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [db :as db]
                    [control :as c]
                    [role :as role]
                    [util :as util]]
            [jepsen.bufstream [core :as core]]
            [jepsen.bufstream.db [bufstream :as db.bufstream]
                                 [minio :as db.minio]]
            [jepsen.control.util :as cu]
            [jepsen.etcd.db :as etcd]))

(defn db
  "Constructs a Jepsen DB for setting up and tearing down a Bufstream cluster."
  []
  (let [; A partial test map we'll merge into tests for etcd. This is in a
        ; promise so we can lazily initialize the stateful atoms it needs.
        etcd-test (promise)
        etcd-test-fn (fn [test]
                       ; Lazy init
                       (deliver etcd-test
                                {:version (:etcd-version test)
                                 :snapshot-count 100
                                 ; Tells etcd whether we've already run init
                                 :initialized? (atom false)
                                 ; Used for the membership state machine, which
                                 ; we ignore
                                 :members (atom (into (sorted-set) (:nodes test)))})
                       (merge test @etcd-test))]
    (role/db
      {:bufstream    (db.bufstream/db)
       :storage      (db.minio/db)
       :coordination (db/map-test etcd-test-fn (etcd/db {}))})))
