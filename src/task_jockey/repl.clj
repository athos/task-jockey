(ns task-jockey.repl
  (:refer-clojure :exclude [send])
  (:require [clojure.java.io :as io]
            [task-jockey.client :as client]
            [task-jockey.log :as log]
            [task-jockey.message-queue :as queue]
            [task-jockey.server :as server]
            [task-jockey.state :as state]
            [task-jockey.system-state :as system]
            [task-jockey.task-handler :as handler]))

(def system nil)

(defn add [command & {:keys [work-dir after]}]
  (let [work-dir (or work-dir (System/getProperty "user.dir"))
        msg {:type :add
             :command (name command)
             :path (.getCanonicalPath (io/file work-dir))
             :dependencies (set after)}
        res (client/send-and-recv (:client system) msg)]
    (println (:message res))))

(defn status [& {:keys [group]}]
  (let [res (client/send-and-recv (:client system) {:type :status})]
    (when (= (:type res) :status-response)
      (if group
        (state/print-single-group (:status res) group)
        (state/print-all-groups (:status res))))))

(defn clean []
  (let [res (client/send-and-recv (:client system) {:type :clean})]
    (println (:message res))))

(defn stash [id-or-ids]
  (let [task-ids (if (coll? id-or-ids) (vec id-or-ids) [id-or-ids])
        msg {:type :stash, :task-ids task-ids}
        res (client/send-and-recv (:client system) msg)]
    (println (:message res))))

(defn enqueue [id-or-ids]
  (let [task-ids (if (coll? id-or-ids) (vec id-or-ids) [id-or-ids])
        msg {:type :enqueue, :task-ids task-ids}
        res (client/send-and-recv (:client system) msg)]
    (println (:message res))))

(defn switch [task-id1 task-id2]
  (let [msg {:type :switch, :task-id1 task-id1, :task-id2 task-id2}
        res (client/send-and-recv (:client system) msg)]
    (println (:message res))))

(defn restart [id-or-ids]
  (let [task-ids (if (coll? id-or-ids) (vec id-or-ids) [id-or-ids])
        msg {:type :restart, :task-ids task-ids}
        res (client/send-and-recv (:client system) msg)]
    (println (:message res))))

(defn edit [task-id command]
  (let [msg {:type :edit, :task-id task-id, :command command}
        res (client/send-and-recv (:client system) msg)]
    (println (:message res))))

(defn log
  ([] (log #{}))
  ([id-or-ids]
   (let [task-ids (if (coll? id-or-ids) (vec id-or-ids) [id-or-ids])]
     (locking system/state
       (log/print-logs @system/state task-ids)))))

(defn follow [id]
  (log/follow-logs system/state id))

(defn send [task-id input]
  (let [msg {:type :send, :task-id task-id, :input input}
        res (client/send-and-recv (:client system) msg)]
    (println (:message res))))

(defn kill [id-or-ids]
  (let [task-ids (if (coll? id-or-ids) (vec id-or-ids) [id-or-ids])
        msg {:type :kill, :task-ids task-ids}
        res (client/send-and-recv (:client system) msg)]
    (println (:message res))))

(defn parallel [n & {:keys [group] :or {group "default"}}]
  (let [msg {:type :parallel, :group group, :parallel-tasks n}
        res (client/send-and-recv (:client system) msg)]
    (println (:message res))))

(defn groups []
  (let [res (client/send-and-recv (:client system)  {:type :group-list})]
    (doseq [[name group] (:groups res)]
      (state/print-group-summary name group)
      (newline))))

(defn group-add [name & {:keys [parallel-tasks]}]
  (let [msg {:type :group-add, :name name, :parallel-tasks parallel-tasks}
        res (client/send-and-recv (:client system) msg)]
    (println (:message res))))

(defn group-remove [name]
  (let [msg {:type :group-remove, :name name}
        res (client/send-and-recv (:client system) msg)]
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
