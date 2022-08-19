(ns task-jockey.task-jockey)

(defn make-state []
  {:tasks (sorted-map)
   :groups {"default" {:parallel-tasks 1}}})

(def state (volatile! (make-state)))

(defn make-task-handler [state]
  {:state state
   :children {"default" (sorted-map)}})

(def task-handler (volatile! (make-task-handler state)))

(defn add-task [state task]
  (let [next-id (or (some-> (rseq (:tasks state)) ffirst inc) 0)]
    (assoc-in state [:tasks next-id] (assoc task :id next-id))))

(defn next-task-id [{:keys [children state]}]
  (->> (:tasks @state)
       (filter (fn [[_ {:keys [group] :as task}]]
                 (and (= (:status task) :queued)
                      (let [running-tasks (get children group)]
                        (< (count running-tasks)
                           (get-in @state [:groups group :parallel-tasks]))))))
       ffirst))

(defn now [] (java.util.Date.))

(defn start-process [task-handler id]
  (let [task (get-in @(:state task-handler) [:tasks id])
        pb (ProcessBuilder. (:command task))
        child (.start pb)]
    (vswap! (:state task-handler) update-in [:tasks id]
            assoc :status :running :start (now))
    (assoc-in task-handler [:children (:group task) id] child)))

(defn spawn-new [task-handler]
  (locking (:state task-handler)
    (if-let [id (next-task-id task-handler)]
      (start-process task-handler id)
      task-handler)))
