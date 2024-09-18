(ns jepsen.bufstream.db.minio
  "Sets up a single-node Minio storage service"
  (:require [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [db :as db]
                    [control :as c]
                    [role :as role]
                    [util :as util :refer [meh]]]
            [jepsen.bufstream [core :as core]]
            [jepsen.control.util :as cu]))

(def dir
  "The top-level minio dir"
  "/opt/minio")

(def bin
  "The minio binary"
  (str dir "/minio"))

(def mc-bin
  "The minio control binary"
  (str dir "/mc"))

(def data-dir
  "The directory we store data in"
  (str dir "/data"))

(def user
  "The Minio admin username"
  "jepsen")

(def password
  "The Minio admin password"
  "jepsenpw")

(def log-file
  "The log file for stdout/stderr"
  (str dir "/minio.log"))

(def pid-file
  "The pidfile for the minio daemon"
  (str dir "/minio.pid"))

(def port
  "The port for the API"
  9000)

(def console-port
  "The port for the Minio console"
  9001)

(def mc-alias
  "The MC alias name."
  "jalias")

(def bucket
  "The bucket we use for storage"
  "jbucket")

(defn install!
  "Installs minio."
  []
  (c/su
    (info "Installing minio")
    (c/exec :mkdir :-p dir)
    (let [f (cu/cached-wget! "https://dl.min.io/server/minio/release/linux-amd64/minio")]
      (c/exec :cp f bin)
      (c/exec :chmod :+x bin))
    (let [f (cu/cached-wget! "https://dl.min.io/client/mc/release/linux-amd64/mc")]
      (c/exec :cp f mc-bin)
      (c/exec :chmod :+x mc-bin))

    (c/exec :mkdir :-p data-dir)))

(defn mc!
  "Runs an mc command."
  [& args]
  (c/su (c/cd dir (apply c/exec mc-bin args))))

(defn setup-mc!
  "Sets up mc with the initial alias and bucket."
  []
  (mc! :alias :set mc-alias (str "http://localhost:" port) user password)
  (mc! :mb (str mc-alias "/" bucket)))

(defrecord DB
  []
  db/DB
  (setup! [this test node]
    (install!)
    (db/start! this test node)
    (cu/await-tcp-port console-port)
    (setup-mc!)
    ;(info (mc! :admin :info mc-alias))
    )

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
         :env {"MINIO_ROOT_USER" user
               "MINIO_ROOT_PASSWORD" password}}
        bin
        :server
        data-dir
        :--console-address (str ":" console-port))))

  (kill! [_ test node]
    (c/su
      (cu/stop-daemon! bin pid-file)))

  db/Pause
  (pause! [_ test node]
    (c/su (cu/grepkill! :stop "minio")))

  (resume! [_ test node]
    (c/su (cu/grepkill! :cont "minio")))

  db/LogFiles
  (log-files [_ test node]
    ; (meh (c/su (c/cd dir (c/exec :tar :cjf "data.tar.bz2" data-dir))))
    (meh (c/su (c/cd dir
                     (mc! :cp :--recursive (str mc-alias "/" bucket)
                          bucket)
                     (c/exec :tar :cjf "bucket.tar.bz2" bucket))))
    {log-file                     "minio.log"
     (str dir "/data.tar.bz2") "data.tar.bz2"
     (str dir "/bucket.tar.bz2") "bucket.tar.bz2"}))

(def db ->DB)
