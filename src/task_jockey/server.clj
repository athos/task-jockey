(ns task-jockey.server
  (:require [clojure.core.server :as server]
            [clojure.edn :as edn]
            [task-jockey.message-queue :as queue]
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

(defmethod handle-message :parallel [{:keys [group parallel-tasks]}]
  (locking system/state
    (vswap! system/state assoc-in
            [:groups group :parallel-tasks] parallel-tasks))
  (success (format "Parallel tasks setting for group \"%s\" adjusted" group)))

(defmethod handle-message :send [{:keys [task-id input]}]
  (let [msg {:action :send :task-id task-id :input input}]
    (queue/push-message! system/message-queue msg)
    (success "Message is being send to the process")))

(defmethod handle-message :kill [{:keys [task-ids]}]
  (let [msg {:action :kill :task-ids task-ids}]
    (queue/push-message! system/message-queue msg)
    (success "Tasks are being killed")))

(defmethod handle-message :group-list [_]
  (let [groups (locking system/state
                 (get @system/state :groups))]
    {:type :group-response, :groups groups}))

(defmethod handle-message :group-add [{:keys [name parallel-tasks]}]
  (let [msg {:action :group-add, :name name,
             :parallel-tasks (or parallel-tasks 1)}]
    (queue/push-message! system/message-queue msg)
    (success (format "Group \"%s\" is being created" name))))

(defmethod handle-message :group-remove [{:keys [name]}]
  (let [msg {:action :group-remove :name name}]
    (queue/push-message! system/message-queue msg)
    (success (format "Group \"%s\" is being removed" name))))
