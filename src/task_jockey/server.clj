(ns task-jockey.server
  (:require [clojure.core.server :as server]
            [clojure.edn :as edn]
            [task-jockey.state :as state]
            [task-jockey.system-state :as system]))

(defrecord Server [socket name])

(defmulti handle-message :type)

(defn accept []
  (loop []
    (when-let [msg (edn/read {:eof nil} *in*)]
      (let [resp (handle-message msg)]
        (prn resp)
        (recur)))))

(defn start-server [{:keys [host port] :as opts}]
  (let [name (format "task-jockey.%d" port)
        opts' (assoc opts
                     :name name
                     :accept `accept
                     :host host
                     :port port)]
    (->Server (server/start-server opts') name)))

(defn stop-server [server]
  (server/stop-server (:name server)))

(defn success [msg]
  {:type :success :message msg})

(defn failed [msg]
  {:type :failed :message msg})

(defmethod handle-message :add [{:keys [command path after]}]
  (let [task {:command command
              :status :queued
              :group "default"
              :path path
              :dependencies after}]
    (locking system/state
      (vswap! system/state state/add-task task))
    (success "New task added.")))

(defmethod handle-message :status [_]
  (let [state (locking system/state
                @system/state)]
    {:type :status-response
     :status state}))

(defmethod handle-message :clean [_]
  (locking system/state
    (vswap! system/state state/clean-tasks))
  (success "All finished tasks have been removed"))

(defmethod handle-message :stash [{:keys [task-ids]}]
  (locking system/state
    (vswap! system/state state/stash-tasks task-ids))
  (success "Tasks are stashed"))

(defmethod handle-message :enqueue [{:keys [task-ids]}]
  (locking system/state
    (vswap! system/state state/enqueue-tasks task-ids))
  (success "Tasks are enqueued"))

(defmethod handle-message :switch [{:keys [task-id1 task-id2]}]
  (locking system/state
    (vswap! system/state state/switch-tasks task-id1 task-id2))
  (success "Tasks have been switched"))

(defmethod handle-message :restart [{:keys [task-ids]}]
  (locking system/state
    (vswap! system/state state/restart-tasks task-ids))
  (success "Tasks restarted"))

(defmethod handle-message :edit [{:keys [task-id command]}]
  (locking system/state
    (vswap! system/state state/edit-task task-id command))
  (success "Command has been updated"))
