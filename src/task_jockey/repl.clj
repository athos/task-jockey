(ns task-jockey.repl
  (:refer-clojure :exclude [send])
  (:require [clojure.java.io :as io]
            [task-jockey.log :as log]
            [task-jockey.message-queue :as queue]
            [task-jockey.state :as state]
            [task-jockey.system-state :as system]
            [task-jockey.task-handler :as handler]))

(defn add [command & {:keys [work-dir after]}]
  (let [work-dir (or work-dir (System/getProperty "user.dir"))
        task {:command (name command)
              :status :queued
              :group "default"
              :path (.getCanonicalPath (io/file work-dir))
              :dependencies (set after)}]
    (locking system/state
      (vswap! system/state state/add-task task)
      nil)))

(defn edit [task-id command]
  (locking system/state
    (vswap! system/state state/edit-task task-id command)
    nil))

(defn stash [id-or-ids]
  (let [task-ids (if (coll? id-or-ids) (vec id-or-ids) [id-or-ids])]
    (locking system/state
      (vswap! system/state state/stash-tasks task-ids)
      nil)))

(defn enqueue [id-or-ids]
  (let [task-ids (if (coll? id-or-ids) (vec id-or-ids) [id-or-ids])]
    (locking system/state
      (vswap! system/state state/enqueue-tasks task-ids)
      nil)))

(defn switch [task-id1 task-id2]
  (locking system/state
    (vswap! system/state state/switch-tasks task-id1 task-id2)
    nil))

(defn restart [id-or-ids]
  (let [task-ids (if (coll? id-or-ids) (vec id-or-ids) [id-or-ids])]
    (locking system/state
      (vswap! system/state state/restart-tasks task-ids)
      nil)))

(defn status
  ([]
   (locking system/state
     (state/print-all-groups @system/state)))
  ([group]
   (locking system/state
     (state/print-single-group @system/state group))))

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

(defn clean []
  (locking system/state
    (vswap! system/state state/clean-tasks)
    nil))

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

(def running-loop nil)

(defn start-loop []
  (let [fut (future (handler/start-loop system/state
                                        system/message-queue))]
    (alter-var-root #'running-loop (constantly fut))))

(defn stop-loop []
  (alter-var-root #'running-loop
                  (fn [fut]
                    (when fut
                      (future-cancel fut)
                      nil))))
