(ns task-jockey.task-jockey)

(defn make-state []
  {:tasks (sorted-map)
   :groups {"default" {:parallel-tasks 1}}})

(def state (volatile! (make-state)))

(defn make-task-handler []
  {:children {"default" (sorted-map)}})

(def task-handler (volatile! (make-task-handler)))

(defn add-task [state task]
  (let [next-id (or (some-> (rseq (:tasks state)) ffirst inc) 0)]
    (assoc-in state [:tasks next-id] (assoc task :id next-id))))

(defn next-task-id [{:keys [children]} locked-state]
  (->> (:tasks @locked-state)
       (filter (fn [[_ {:keys [group] :as task}]]
                 (and (= (:status task) :queued)
                      (let [running-tasks (get children group)]
                        (< (count running-tasks)
                           (get-in @locked-state
                                   [:groups group :parallel-tasks]))))))
       ffirst))

(defn now [] (java.util.Date.))

(defn start-process [task-handler locked-state id]
  (let [task (get-in @locked-state [:tasks id])
        pb (ProcessBuilder. (:command task))
        child (.start pb)]
    (vswap! locked-state update-in [:tasks id]
            assoc :status :running :start (now))
    (assoc-in task-handler [:children (:group task) id] child)))

(defn spawn-new [task-handler state]
  (locking state
    (if-let [id (next-task-id task-handler state)]
      (start-process task-handler state id)
      task-handler)))
