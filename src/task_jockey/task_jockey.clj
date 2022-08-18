(ns task-jockey.task-jockey)

(def state (atom {:tasks (sorted-map)}))
(def children (atom (sorted-map)))

(defn add-task [state task]
  (let [next-id (or (some-> (rseq (:tasks state)) ffirst inc) 0)]
    (assoc-in state [:tasks next-id] (assoc task :id next-id))))

(defn next-task-id [state]
  (->> (:tasks state)
       (filter #(= (:status (val %)) :queued))
       ffirst))

(defn now [] (java.util.Date.))

(defn start-process [state id]
  (let [task (get-in state [:tasks id])
        pb (ProcessBuilder. (:command task))
        child (.start pb)]
    (swap! children assoc id child)
    (update-in state [:tasks id]
               assoc :status :running :start (now))))

(defn spawn-new [state]
  (if-let [id (next-task-id state)]
    (start-process state id)
    state))
