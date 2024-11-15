(ns jepsen.bufstream.db.bufstream
  "Sets up a Bufstream cluster."
  (:require [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [with-retry]]
            [jepsen [db :as db]
                    [control :as c]
                    [role :as role]
                    [util :as util]]
            [jepsen.bufstream [core :as core]]
            [jepsen.bufstream.db [minio :as minio]
                                 [watchdog :as watchdog]]
            [jepsen.control [net :as cn]
                            [util :as cu]]
            [jepsen.redpanda.client :as client]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (org.apache.kafka.clients.admin AlterConfigOp
                                           AlterConfigOp$OpType
                                           ConfigEntry)
           (org.apache.kafka.common.config ConfigResource
                                           ConfigResource$Type)))

(def dir
  "The top-level bufstream dir"
  "/opt/bufstream")

(def bin
  "The bufstream binary"
  (str dir "/bufstream"))

(def log-file
  "The log file for stdout/stderr"
  (str dir "/bufstream.log"))

(def pid-file
  "The PID file for bufstream's daemon"
  (str dir "/bufstream.pid"))

(def config-file
  "The yaml config file for bufstream"
  (str dir "/bufstream.yaml"))

(def kafka-port
  "The port for the Kafka interface."
  9092)

(def watchdog-interval
  "How often (ms) do we check to restart the bufstream process when it crashes"
  1000)

(defn install!
  "Uploads the minio binary and creates relevant directories."
  [test]
  (c/su
    (info "Installing Bufstream from local file" (:bin test))
    (c/exec :mkdir :-p dir)
    (c/upload (:bin test) bin)
    (c/exec :chmod :+x bin)))

(defn configure!
  "Sets up config files"
  [test]
  (let [etcd  (first (role/nodes test :coordination))
        minio (first (role/nodes test :storage))]
    (assert etcd)
    (assert minio)
    (c/su
      (-> (io/resource "bufstream.yaml")
          slurp
          (str/replace #"%ETCD_HOST%" etcd)
          (str/replace #"%S3_BUCKET%" minio/bucket)
          (str/replace #"%S3_ENDPOINT%"
                       (str "http://" minio ":" minio/port))
          ; Note: username and password *are* access keys!
          (str/replace #"%S3_ACCESS_KEY%" minio/user)
          (str/replace #"%S3_SECRET%" minio/password)
          (str/replace #"%FETCH_EAGER%" (str (:fetch-eager test)))
          (str/replace #"%FETCH_SYNC%"  (str (:fetch-sync test)))
          (str/replace #"%LOG_LEVEL%"    (:bufstream-log-level test))
          (str/replace #"%ARCHIVE%"
                       (if (:no-archive test)
                         "archive:\n  min_bytes: -1\n"
                         ""))
          (cu/write-file! config-file)))))

(defn post-configure!
  "After starting up, we have to set a few things via an admin client."
  [test node]
  (let [a (client/admin test node)]
    (.. a
        (incrementalAlterConfigs
          {; We have to work around a bug in Bufstream that means it ignores the
           ; config file's settings for group consumer session timeouts.
           ; This is possibly the most confusing API for settings I've ever seen
           (ConfigResource. ConfigResource$Type/BROKER "")
           [(AlterConfigOp. (ConfigEntry.
                              "group.consumer.min.session.timeout.ms"
                              "1000")
                            AlterConfigOp$OpType/SET)]})
        all
        get)))

(defn start!
  "Starts bufstream."
  [node]
  (c/su
    (cu/start-daemon!
      {:logfile log-file
       :pidfile pid-file
       :chdir dir
       :env {"EC2_PRIVATE_IP" (cn/local-ip)
             ; We run on big boxes and high core counts convince bufstream
             ; that it should assume absolutely huge memory sizes. Dropping
             ; this to 4 cores significantly improves throughput and latency.
             "GOMAXPROCS" 4}}
      bin
      :serve
      :-c                                   config-file
      :--config.name                        node
      :--config.kafka.public_address.host   (cn/local-ip)
      :--config.connect_public_address.host (cn/local-ip)
      )))

(defn running?
  "Is bufstream running?"
  [test node]
  (try+ (c/su (c/exec :pgrep :bufstream))
        true
        (catch [:type :jepsen.control/nonzero-exit] _
          false)))

(defrecord DB [tcpdump]
  db/DB
  (setup! [this test node]
    (when (:tcpdump test) (db/setup! tcpdump test node))
    (install! test)
    (configure! test)
    (db/start! this test node)
    (cu/await-tcp-port kafka-port)
    (post-configure! test node))

  (teardown! [this test node]
    (db/kill! this test node)
    (c/su (c/exec :rm :-rf dir))
    (when (:tcpdump test) (db/teardown! tcpdump test node)))

  db/Kill
  (start! [_ test node]
    (start! node))

  (kill! [_ test node]
    (c/su
      (cu/stop-daemon! bin pid-file)
      ; Just in case
      (cu/grepkill! :kill "bufstream")))

  db/Pause
  (pause! [_ test node]
    (c/su (cu/grepkill! :stop "bufstream")))

  (resume! [_ test node]
    (c/su (cu/grepkill! :cont "bufstream")))

  db/LogFiles
  (log-files [_ test node]
    (merge (when (:tcpdump test) (db/log-files tcpdump test node))
           {log-file "bufstream.log"
            config-file "bufstream.yaml"})))

(defn db
  "Constructs a fresh DB."
  []
  (->> (DB. (db/tcpdump {:ports [kafka-port]}))
       (watchdog/db {:running? running?})))
