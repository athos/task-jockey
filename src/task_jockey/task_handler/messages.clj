(ns task-jockey.task-handler.messages
  (:require [task-jockey.children :as children]
            [task-jockey.utils :as utils]))

(defmulti handle-message (fn [_task-handler msg] (:type msg)))

(defmethod handle-message :group-add
  [{:keys [state local]} {:keys [name parallel-tasks]}]
  (locking state
    (vswap! state assoc-in [:groups name]
            {:parallel-tasks parallel-tasks, :status :running}))
  (vswap! local assoc-in [:children name] (sorted-map)))

(defmethod handle-message :group-rm [{:keys [state local]} {:keys [name]}]
  (locking state
    (vswap! state update :groups dissoc name))
  (vswap! local update :children dissoc name))

(defmethod handle-message :send [{:keys [local]} {:keys [task-id input]}]
  (let [child (children/get-child (:children @local) task-id)]
    (doto (.getOutputStream ^Process child)
      (.write (.getBytes ^String input))
      (.flush))))

(defmethod handle-message :kill [{:keys [state local]} {:keys [group task-ids]}]
  (locking state
    (let [task-ids (cond group
                         (for [task (vals (:tasks @state))
                               :when (and (= (:group task) group)
                                          (= (:status task) :running))]
                           (:id task))

                         (seq task-ids)
                         task-ids

                         :else
                         (for [task (vals (:tasks @state))
                               :when (= (:status task) :running)]
                           (:id task)))]
      (doseq [task-id task-ids
              :let [child (children/get-child (:children @local) task-id)]]
        (vswap! state update-in [:tasks task-id] assoc
                :status :killed :end (utils/now))
        (.destroy ^Process child)))))

(defmethod handle-message :reset [{:keys [local] :as task-handler} _]
  (vswap! local assoc :full-reset? true)
  (handle-message task-handler {:type :kill :task-ids #{}}))
