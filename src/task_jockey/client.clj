(ns task-jockey.client
  (:refer-clojure :exclude [send])
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [task-jockey.protocols :as proto]))

(defn- send-and-recv [client type & fields]
  (let [msg (apply array-map :type type fields)]
    (proto/send-message client msg)))

(defn- envs []
  (into {} (System/getenv)))

(defn add [client {:keys [command group dir after stashed label]}]
  (let [cmd (if (coll? command)
              (str/join \space (map pr-str command))
              (str command))
        dir (or dir (System/getProperty "user.dir"))]
    (send-and-recv client :add
                   :command cmd
                   :group (or (some-> group str) "default")
                   :dir (.getCanonicalPath (io/file dir))
                   :envs (envs)
                   :dependencies (set after)
                   :stashed stashed
                   :label (some-> label str))))

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

(defn follow [client task-id callback]
  (let [msg {:type :follow, :task-id task-id}]
    (proto/send-message-with-callback client msg callback)))

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
