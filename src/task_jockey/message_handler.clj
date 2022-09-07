(ns task-jockey.message-handler
  (:require [clojure.java.io :as io]
            [task-jockey.log :as log]
            [task-jockey.message-queue :as queue]
            [task-jockey.state :as state]
            [task-jockey.system-state :as system]
            [task-jockey.task :as task])
  (:import [java.io Reader]))

(defmulti handle-message :type)

(defn success [msg & {:as opts}]
  (into {:type :success :message msg} opts))

(defn failed [msg]
  {:type :failed :message msg})

(defmethod handle-message :add
  [{:keys [command group dir envs after stashed label]}]
  (let [task {:command command
              :status (if stashed :stashed :queued)
              :group group
              :dir dir
              :envs envs
              :dependencies after
              :label label}
        state (locking system/state
                (vswap! system/state state/add-task task))
        task-id (ffirst (rseq (:tasks state)))]
    (success (format "New task added (id %d)" task-id)
             :task-id task-id)))

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

(defmethod handle-message :log-request [{:keys [task-ids]}]
  (let [task-ids (set task-ids)
        state (locking system/state @system/state)
        tasks (reduce-kv (fn [ret id task]
                           (if (or (empty? task-ids)
                                   (contains? task-ids id))
                             (let [output (log/read-log-file id)
                                   task-log {:task task
                                             :output output}]
                               (assoc ret id task-log))
                             ret))
                         {}
                         (:tasks state))]
    {:type :log-response, :tasks tasks}))

(defmethod handle-message :follow [{:keys [task-id]}]
  (letfn [(step [^Reader in ^chars buf]
            (if (.ready in)
              (let [size (.read in buf)
                    content (String. buf 0 size)]
                {:type :stream, :content content,
                 :cont #(step in buf)})
              (let [task (locking system/state
                           (get-in @system/state [:tasks task-id]))
                    done? (task/task-done? task)]
                (if done?
                 (do (.close in)
                     {:type :close})
                  (do (Thread/sleep 1000)
                      (recur in buf))))))]
    (step (io/reader (log/log-file-path task-id))
          (char-array 1024))))

(defmethod handle-message :send [{:keys [task-id input]}]
  (let [msg {:type :send :task-id task-id :input input}]
    (queue/push-message! system/message-queue msg)
    (success "Message is being send to the process")))

(defmethod handle-message :kill [{:keys [task-ids]}]
  (let [msg {:type :kill :task-ids task-ids}]
    (queue/push-message! system/message-queue msg)
    (success "Tasks are being killed")))

(defmethod handle-message :group-list [_]
  (let [groups (locking system/state
                 (get @system/state :groups))]
    {:type :group-response, :groups groups}))

(defmethod handle-message :group-add [{:keys [name parallel-tasks]}]
  (let [msg {:type :group-add, :name name,
             :parallel-tasks (or parallel-tasks 1)}]
    (queue/push-message! system/message-queue msg)
    (success (format "Group \"%s\" is being created" name))))

(defmethod handle-message :group-remove [{:keys [name]}]
  (let [msg {:type :group-remove :name name}]
    (queue/push-message! system/message-queue msg)
    (success (format "Group \"%s\" is being removed" name))))
