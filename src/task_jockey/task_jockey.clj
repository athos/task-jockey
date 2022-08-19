(ns task-jockey.task-jockey)

(def state (volatile! {:tasks (sorted-map)}))
(def task-handler
  (volatile! {:children (sorted-map)}))

(defn add-task [state task]
  (let [next-id (or (some-> (rseq (:tasks state)) ffirst inc) 0)]
    (assoc-in state [:tasks next-id] (assoc task :id next-id))))

(defn next-task-id [{:keys [children]} locked-state]
  (when-not (seq children)
    (->> (:tasks @locked-state)
         (filter #(= (:status (val %)) :queued))
         ffirst)))

(defn now [] (java.util.Date.))

(defn start-process [task-handler locked-state id]
  (let [task (get-in @locked-state [:tasks id])
        pb (ProcessBuilder. (:command task))
        child (.start pb)]
    (vswap! locked-state update-in [:tasks id]
            assoc :status :running :start (now))
    (assoc-in task-handler [:children id] child)))

(defn spawn-new [task-handler state]
  (locking state
    (if-let [id (next-task-id task-handler state)]
      (start-process task-handler state id)
      task-handler)))
