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

; A wrapper for the role-based DB. In particular, that DB supports primaries,
; which we don't actually care about here. Why bother disabling primaries?
; Because in rare situations the etcd DB's primaries implementation does
; network IO, and if it runs out of filehandles it can blow up the generator.
;
; The "right" thing to do here is to find the filehandle leak (if one exists?)
; and fix it, or add more robust recovery code to the primaries implementation,
; but hitting this took several hours, we don't actually want to ask for
; primaries *anyway*, a review of the code doesn't show any obvious leaks, and
; I'm on the clock here.
(defrecord DB [db]
  db/DB
  (setup!    [_ test node] (db/setup! db test node))
  (teardown! [_ test node] (db/teardown! db test node))

  db/LogFiles
  (log-files [_ test node] (db/log-files db test node))

  db/Kill
  (start! [_ test node] (db/start! db test node))
  (kill!  [_ test node] (db/kill! db test node))

  db/Pause
  (resume! [_ test node] (db/resume! db test node))
  (pause!  [_ test node] (db/pause! db test node)))

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
    (DB.
      (role/db
        {:bufstream    (db.bufstream/db)
         :storage      (db.minio/db)
         :coordination (db/map-test etcd-test-fn (etcd/db {}))}))))
