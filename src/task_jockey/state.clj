(ns task-jockey.state
  (:require [clojure.pprint :as pp]
            [task-jockey.task :as task]
            [task-jockey.utils :as utils]))

(defn make-state []
  {:tasks (sorted-map)
   :groups {"default" {:parallel-tasks 1 :status :running}}})

(defn add-task [state task]
  (let [next-id (or (some-> (rseq (:tasks state)) ffirst inc) 0)]
    (assoc-in state [:tasks next-id] (assoc task :id next-id))))

(defn edit-task [state task-id command]
  (assoc-in state [:tasks task-id :command] command))

(defn stash-tasks [state task-ids]
  (reduce (fn [state task-id]
            (assoc-in state [:tasks task-id :status] :stashed))
          state
          task-ids))

(defn enqueue-tasks [state task-ids]
  (reduce (fn [state task-id]
            (assoc-in state [:tasks task-id :status] :queued))
          state
          task-ids))

(defn switch-tasks [state task-id1 task-id2]
  (let [task1 (get-in state [:tasks task-id1])
        task2 (get-in state [:tasks task-id2])]
    (-> state
        (assoc-in [:tasks task-id1] (assoc task2 :id task-id1))
        (assoc-in [:tasks task-id2] (assoc task1 :id task-id2)))))

(defn restart-tasks [state task-ids]
  (reduce (fn [state task-id]
            (let [task (get-in state [:tasks task-id])
                  new-task (-> task
                               (assoc :status :queued)
                               (dissoc :id :start :end))]
              (add-task state new-task)))
          state
          task-ids))

(defn task-removable? [{:keys [tasks] :as state} to-be-deleted task-id]
  (let [dependants
        (into #{}
              (keep (fn [[id task']]
                      (when (and (contains? (:dependencies task')
                                            task-id)
                                 (not (task/task-done? (get tasks id))))
                        id)))
              tasks)]
    (or (empty? dependants)
        (and (empty? (apply disj dependants to-be-deleted))
             (every? (partial task-removable? state to-be-deleted) dependants)))))

(defn clean-tasks [state]
  (reduce-kv (fn [state id task]
               (cond-> state
                 (and (task/task-done? task)
                      (task-removable? state #{} id))
                 (update :tasks dissoc id)))
             state
             (:tasks state)))

(defn print-group-summary [group-name {:keys [status parallel-tasks]}]
  (printf "Group \"%s\" (%d parallel): %s"
          group-name
          parallel-tasks
          (name status)))

(defn print-single-group [state group-name]
  (let [group (get-in state [:groups group-name])
        tasks (->> (:tasks state)
                   (keep (fn [[_ task]]
                           (when (= (:group task) group-name)
                             (cond-> task
                               (:start task)
                               (update :start utils/stringify-date)
                               (:end task)
                               (update :end utils/stringify-date)))))
                   (sort-by :id))]
    (print-group-summary group-name group)
    (if (empty? tasks)
      (newline)
      (pp/print-table [:id :status :command :path :start :end] tasks))))

(defn print-all-groups [state]
  (->> (:groups state)
       keys
       (run! (partial print-single-group state))))
