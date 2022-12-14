(ns task-jockey.task-handler
  (:require [clojure.tools.logging :as logging]
            [task-jockey.child :as child]
            [task-jockey.children :as children]
            [task-jockey.log :as log]
            [task-jockey.message-queue :as queue]
            [task-jockey.protocols :as proto]
            [task-jockey.settings :as settings]
            [task-jockey.task :as task]
            [task-jockey.task-handler.messages :as messages]
            [task-jockey.utils :as utils])
  (:import [java.util Date]))

(defn make-task-handler [state queue]
  {:state state
   :queue queue
   :local (volatile! {:children {"default" (sorted-map)}})})

(defn- next-task-id [{:keys [state local]}]
  (->> (for [[task-id {:keys [group] :as task}] (:tasks @state)
             :when (= (:status task) :queued)
             :let [running-tasks (get-in @local [:children group])]
             :when (< (count running-tasks)
                      (get-in @state [:groups group :parallel-tasks]))
             :let [deps (:dependencies task)]
             :when (or (empty? deps)
                       (every? #(task/task-done? (get (:tasks @state) %))
                               deps))]
         task-id)
       first))

(defn- start-task [{:keys [state local]} id]
  (let [task (get-in @state [:tasks id])
        worker-id (children/next-group-worker (:children @local) (:group task))]
    (try
      (let [child (child/start-task task worker-id)]
        (vswap! state update-in [:tasks id]
                assoc :status :running :start (utils/now))
        (vswap! local update :children
                children/add-child (:group task) worker-id id child))
      (catch Exception e
        (vswap! state update-in [:tasks id]
                assoc :status :failed-to-spawn :reason (ex-message e)
                :start (utils/now) :end (utils/now))))))

(defn- spawn-new [task-handler]
  (locking (:state task-handler)
    (loop []
      (when-let [id (next-task-id task-handler)]
        (start-task task-handler id)
        (recur)))))

(defn- handle-messages [{:keys [queue] :as task-handler}]
  (when-let [msg (queue/pop-message! queue)]
    (messages/handle-message task-handler msg)
    true))

(defn- handle-finished-tasks [{:keys [state local]}]
  (let [finished (for [[group pool] (:children @local)
                       [worker {:keys [task child]}] pool
                       :when (proto/done? child)
                       :let [res (proto/result child)]]
                   [group worker task res (proto/succeeded? child res)])]
    (when (seq finished)
      (locking state
        (->> finished
             (reduce (fn [state [_ _ task res ok?]]
                       (cond-> state
                         (not= (:status (get-in state [:tasks task]))
                               :killed)
                         (update-in [:tasks task] assoc
                                    :status (if ok? :success :failed)
                                    :result res
                                    :end (utils/now))))
                     @state)
             (vreset! state)))
      (doseq [[group worker task _ _] finished]
        (vswap! local update-in [:children group] dissoc worker)
        (when (:full-reset? @local)
          (log/clean-log-file task))))))

(defn- enqueue-delayed-tasks [{:keys [state]}]
  (locking state
    (doseq [[id task] (:tasks @state)
            :when (and (= (:status task) :stashed)
                       (when-let [^Date t (:enqueue-at task)]
                         (.before t (utils/now))))]
      (vswap! state assoc-in [:tasks id :status] :queued))))

(defn- check-failed-dependencies [{:keys [state]}]
  (locking state
    (doseq [[id task] (:tasks @state)
            :when (and (= (:status task) :queued)
                       (some #(task/task-failed? (get-in @state [:tasks %]))
                             (:dependencies task)))]
      (vswap! state update-in [:tasks id] assoc
              :status :dependency-failed :start (utils/now) :end (utils/now)))))

(defn- handle-reset [{:keys [state local]}]
  (when (children/has-active-tasks? (:children @local))
    (locking state
      (vswap! state assoc :tasks (sorted-map)))
    (log/reset-log-dir)
    (vswap! local assoc :full-reset? false)))

(defn- step [{:keys [local] :as handler}]
  (let [ret (handle-messages handler)]
    (doto handler
      (handle-finished-tasks)
      (enqueue-delayed-tasks)
      (check-failed-dependencies))
    (if (:full-reset? @local)
      (handle-reset handler)
      (spawn-new handler))
    ret))

(defn stop-handler [{:keys [local loop]}]
  (logging/debug "Stopped message loop")
  (vswap! local assoc :stopped true)
  (future-cancel loop))

(defn restart-handler [{:keys [local] :as handler} {:keys [sync?] :as opts}]
  (let [f (fn []
            (settings/with-settings opts
              (try
                (loop []
                  (when-not (step handler)
                    (Thread/sleep 200))
                  (recur))
                (catch Throwable t
                  (when-not (:stopped @local)
                    (logging/error t "Message loop terminated abruptly"))))))]
    (when (:loop handler)
      (stop-handler handler))
    (vswap! local assoc :stopped false)
    (logging/debug "Started message loop")
    (if sync?
      (f)
      (assoc handler :loop (future (f))))))

(defn start-handler [state queue opts]
  (let [handler (make-task-handler state queue)]
    (restart-handler handler opts)))
