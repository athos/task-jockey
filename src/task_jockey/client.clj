(ns task-jockey.client
  (:refer-clojure :exclude [send])
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [java.io Closeable PushbackReader]
           [java.net Socket]))

(defrecord Client [^Socket socket in out]
  Closeable
  (close [_] (.close socket)))

(defn make-client [{:keys [host port]}]
  (let [socket (Socket. host port)
        in (PushbackReader. (io/reader (.getInputStream socket)))
        out (io/writer (.getOutputStream socket))]
    (->Client socket in out)))

(defn send-message [client msg]
  (binding [*out* (:out client)]
    (prn msg)))

(defn recv-message [client]
  (edn/read {:eof nil} (:in client)))

(defn send-and-recv [client type & fields]
  (let [msg (apply array-map :type type fields)]
    (send-message client msg)
    (recv-message client)))

(defn add [client cmd path after]
  (let [cmd' (if (coll? cmd)
               (str/join \space (map pr-str cmd))
               (str cmd))
        path (or path (System/getProperty "user.dir"))]
    (send-and-recv client :add
                   :command cmd'
                   :path (.getCanonicalPath (io/file path))
                   :dependencies (set after))))

(defn status [client]
  (send-and-recv client :status))

(defn clean [client]
  (send-and-recv client :clean))

(defn stash [client task-ids]
  (send-and-recv client :stash :task-ids task-ids))

(defn enqueue [client task-ids]
  (send-and-recv client :enqueue :task-ids task-ids))

(defn switch [client task-id1 task-id2]
  (send-and-recv client :switch
                 :task-id1 task-id1
                 :task-id2 task-id2))

(defn restart [client task-ids]
  (send-and-recv client :restart :task-ids task-ids))

(defn edit [client task-id command]
  (send-and-recv client :edit
                 :task-id task-id
                 :command command))

(defn log [client task-ids]
  (send-and-recv client :log-request :task-ids task-ids))

(defn send [client task-id input]
  (send-and-recv client :send
                 :task-id task-id
                 :input input))

(defn kill [client task-ids]
  (send-and-recv client :kill :task-ids task-ids))

(defn parallel [client group tasks]
  (send-and-recv client :parallel
                 :group (or group "default")
                 :parallel-tasks tasks))

(defn groups [client]
  (send-and-recv client :group-list))

(defn group-add [client name parallel-tasks]
  (send-and-recv client :group-add
                 :name name
                 :parallel-tasks parallel-tasks))

(defn group-rm [client name]
  (send-and-recv client :group-remove :name name))
