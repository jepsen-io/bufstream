(ns jepsen.bufstream.db.bufstream
  "Sets up a Bufstream cluster."
  (:require [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [db :as db]
                    [control :as c]
                    [role :as role]
                    [util :as util]]
            [jepsen.bufstream [core :as core]]
            [jepsen.bufstream.db [minio :as minio]]
            [jepsen.control [net :as cn]
                            [util :as cu]]
            [jepsen.redpanda.client :as client])
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

(defrecord DB []
  db/DB
  (setup! [this test node]
    (install! test)
    (configure! test)
    (db/start! this test node)
    (cu/await-tcp-port kafka-port)
    (post-configure! test node))

  (teardown! [this test node]
    (db/kill! this test node)
    (c/su
      (c/exec :rm :-rf dir)
      ))

  db/Kill
  (start! [_ test node]
    (c/su
      (cu/start-daemon!
        {:logfile log-file
         :pidfile pid-file
         :chdir dir
         :env {"EC2_PRIVATE_IP" (cn/local-ip)}}
        bin
        :serve
        :-c                                   config-file
        :--config.name                        node
        :--config.kafka.public_address.host   (cn/local-ip)
        :--config.connect_public_address.host (cn/local-ip)
        )))

  (kill! [_ test node]
    (c/su
      (cu/stop-daemon! bin pid-file)))

  db/LogFiles
  (log-files [_ test node]
    {log-file "bufstream.log"
     config-file "bufstream.yaml"}))

(def db ->DB)
