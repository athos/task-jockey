(ns task-jockey.cli
  (:refer-clojure :exclude [send])
  (:require [task-jockey.client :as client]
            [task-jockey.log :as log]
            [task-jockey.server :as server]
            [task-jockey.state :as state]
            [task-jockey.system-state :as system]
            [task-jockey.task-handler :as handler]
            [task-jockey.transport :as transport]
            [task-jockey.utils :as utils]))

(defn- with-client
  [{:keys [host port] :or {host "localhost" port 5555}:as opts} f & args]
  (let [opts' (assoc opts :host host :port port)
        client (transport/make-socket-transport opts')]
    (apply f client args)))

(defn add [{:keys [cmd] :as opts}]
  (let [opts' (-> opts
                  (assoc :command cmd)
                  (dissoc :cmd))
        res (with-client opts' client/add opts')]
    (println (:message res))))

(defn status [{:keys [group] :as opts}]
  (let [res (with-client opts client/status)]
    (when (= (:type res) :status-response)
      (if group
        (state/print-single-group (:status res) group)
        (state/print-all-groups (:status res))))))

(defn clean [opts]
  (let [res (with-client opts client/clean)]
    (println (:message res))))

(defn stash [{:keys [tasks] :as opts}]
  (let [task-ids (utils/->coll tasks)
        res (with-client opts client/stash task-ids)]
    (println (:message res))))

(defn enqueue [{:keys [tasks] :as opts}]
  (let [task-ids (if (coll? tasks) (vec tasks) [tasks])
        res (with-client opts client/enqueue task-ids)]
    (println (:message res))))

(defn switch [{:keys [task1 task2] :as opts}]
  (let [res (with-client opts client/switch task1 task2)]
    (println (:message res))))

(defn restart [{:keys [tasks] :as opts}]
  (let [task-ids (utils/->coll tasks)
        res (with-client opts client/restart task-ids)]
    (println (:message res))))

(defn edit [{:keys [task cmd] :as opts}]
  (let [res (with-client opts client/edit task (str cmd))]
    (println (:message res))))

(defn log [{:keys [tasks] :as opts}]
  (let [task-ids (utils/->coll tasks)
        res (with-client opts client/log task-ids)]
    (log/print-logs (:tasks res) task-ids)))

(defn follow [{:keys [task] :as opts}]
  (with-client opts client/follow task
    (fn [{:keys [content]}]
      (print content)
      (flush))))

(defn send [{:keys [task input] :as opts}]
  (let [res (with-client opts client/send task (str input))]
    (println (:message res))))

(defn kill [{:keys [tasks] :as opts}]
  (let [task-ids (utils/->coll tasks)
        res (with-client opts client/kill task-ids)]
    (println (:message res))))

(defn parallel [{:keys [n group] :as opts}]
  (let [res (with-client opts client/parallel group n)]
    (println (:message res))))

(defn groups [opts]
  (let [res (with-client opts client/groups)]
    (doseq [[name group] (:groups res)]
      (state/print-group-summary name group)
      (newline))))

(defn group-add [{:keys [name parallel] :as opts}]
  (let [res (with-client opts client/group-add (str name) parallel)]
    (println (:message res))))

(defn group-rm [{:keys [name] :as opts}]
  (let [res (with-client opts client/group-rm (str name))]
    (println (:message res))))

(defn start-server [& {:keys [host] :or {host "localhost"} :as opts}]
  (let [opts' (assoc opts :host host)]
    (server/start-server opts')
    (handler/start-loop system/state system/message-queue)))
