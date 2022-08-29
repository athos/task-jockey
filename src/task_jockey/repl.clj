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

(defn send [id input]
  (queue/push-message! system/message-queue
                       {:action :send :task-id id :input input}))

(defn kill [id-or-ids]
  (let [task-ids (if (coll? id-or-ids) (vec id-or-ids) [id-or-ids])
        msg {:action :kill :task-ids task-ids}]
    (queue/push-message! system/message-queue msg)))

(defn parallel [n & {:keys [group] :or {group "default"}}]
  (locking system/state
    (vswap! system/state assoc-in [:groups group :parallel-tasks] n)
    nil))

(defn group [& {:keys [action name parallel-tasks]
              :or {action :list parallel-tasks 1}}]
  (case action
    :list (doseq [[name group] (locking system/state
                                 (get @system/state :groups))]
            (state/print-group-summary name group)
            (newline))
    :add (let [msg {:action :group-add :name name
                    :parallel-tasks parallel-tasks}]
           (queue/push-message! system/message-queue msg))
    :remove (let [msg {:action :group-remove :name name}]
              (queue/push-message! system/message-queue msg))))

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
