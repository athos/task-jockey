(ns task-jockey.repl
  (:refer-clojure :exclude [send])
  (:require [clojure.java.io :as io]
            [task-jockey.client :as client]
            [task-jockey.log :as log]
            [task-jockey.server :as server]
            [task-jockey.state :as state]
            [task-jockey.system-state :as system]
            [task-jockey.task-handler :as handler]))

(def system nil)

(defn add [command & {:keys [work-dir after]}]
  (let [work-dir (or work-dir (System/getProperty "user.dir"))
        res (client/add (:client system)
                        (name command)
                        (.getCanonicalPath (io/file work-dir))
                        (set after))]
    (println (:message res))))

(defn status [& {:keys [group]}]
  (let [res (client/status (:client system))]
    (when (= (:type res) :status-response)
      (if group
        (state/print-single-group (:status res) group)
        (state/print-all-groups (:status res))))))

(defn clean []
  (let [res (client/clean (:client system))]
    (println (:message res))))

(defn stash [id-or-ids]
  (let [task-ids (if (coll? id-or-ids) (vec id-or-ids) [id-or-ids])
        res (client/stash (:client system) task-ids)]
    (println (:message res))))

(defn enqueue [id-or-ids]
  (let [task-ids (if (coll? id-or-ids) (vec id-or-ids) [id-or-ids])
        res (client/enqueue (:client system) task-ids)]
    (println (:message res))))

(defn switch [task-id1 task-id2]
  (let [res (client/switch (:client system) task-id1 task-id2)]
    (println (:message res))))

(defn restart [id-or-ids]
  (let [task-ids (if (coll? id-or-ids) (vec id-or-ids) [id-or-ids])
        res (client/restart (:client system) task-ids)]
    (println (:message res))))

(defn edit [task-id command]
  (let [res (client/edit (:client system) task-id command)]
    (println (:message res))))

(defn log
  ([] (log []))
  ([id-or-ids]
   (let [task-ids (if (coll? id-or-ids) (vec id-or-ids) [id-or-ids])
         res (client/log (:client system) task-ids)]
     (log/print-logs (:tasks res) task-ids))))

(defn follow [id]
  (log/follow-logs system/state id))

(defn send [task-id input]
  (let [res (client/send (:client system) task-id input)]
    (println (:message res))))

(defn kill [id-or-ids]
  (let [task-ids (if (coll? id-or-ids) (vec id-or-ids) [id-or-ids])
        res (client/send-and-recv (:client system) task-ids)]
    (println (:message res))))

(defn parallel [n & {:keys [group] :or {group "default"}}]
  (let [res (client/parallel (:client system) group n)]
    (println (:message res))))

(defn groups []
  (let [res (client/groups (:client system))]
    (doseq [[name group] (:groups res)]
      (state/print-group-summary name group)
      (newline))))

(defn group-add [name & {:keys [parallel-tasks]}]
  (let [res (client/group-add (:client system)
                              name
                              parallel-tasks)]
    (println (:message res))))

(defn group-remove [name]
  (let [res (client/send-and-recv (:client system) name)]
    (println (:message res))))

(defn start-system [& {:keys [host port]
                       :or {host "localhost" port 5555}
                       :as opts}]
  (let [opts' (assoc opts :host host :port port)
        fut (future (handler/start-loop system/state
                                        system/message-queue))
        server (server/start-server opts')
        client (client/make-client opts')]
    (alter-var-root #'system
                    (constantly {:loop fut :server server :client client}))
    nil))

(defn stop-system []
  (alter-var-root #'system
                  (fn [system]
                    (when system
                      (.close (:client system))
                      (server/stop-server (:server system))
                      (future-cancel (:loop system))
                      nil))))
