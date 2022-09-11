(ns task-jockey.task-handler
  (:require [clojure.java.io :as io]
            [task-jockey.log :as log]
            [task-jockey.message-queue :as queue]
            [task-jockey.task :as task])
  (:import [java.util Date]))

(defn make-task-handler [state queue]
  {:state state
   :queue queue
   :children (volatile! {"default" (sorted-map)})})

(defn- next-task-id [{:keys [children state]}]
  (->> (:tasks @state)
       (filter (fn [[_ {:keys [group] :as task}]]
                 (and (= (:status task) :queued)
                      (let [running-tasks (get @children group)]
                        (< (count running-tasks)
                           (get-in @state [:groups group :parallel-tasks])))
                      (let [deps (:dependencies task)]
                        (or (empty? deps)
                            (every? #(task/task-done? (get (:tasks @state) %)) deps))))))
       ffirst))

(defn- next-group-worker [{:keys [children]} group]
  (let [pool (get @children group)]
    (or (->> pool
             (keep-indexed (fn [i [worker-id _]]
                             (when (not= i worker-id)
                               i)))
             first)
        (count pool))))

(defn- now ^Date [] (Date.))

(defn- start-process [task-handler id]
  (let [task (get-in @(:state task-handler) [:tasks id])
        worker-id (next-group-worker task-handler (:group task))
        log-file (log/log-file-path id)
        command (into-array String ["sh" "-c" (:command task)])
        pb (ProcessBuilder. ^"[Ljava.lang.String;" command)
        _ (doto (.environment pb)
            (.clear)
            (.putAll (:envs task))
            (.put "TASK_JOCKEY_GROUP" (:group task))
            (.put "TASK_JOCKEY_WORKER_ID" (str worker-id)))
        child (-> pb
                  (.redirectOutput log-file)
                  (.redirectError log-file)
                  (.directory (io/file (:dir task)))
                  (.start))]
    (vswap! (:state task-handler) update-in [:tasks id]
            assoc :status :running :start (now))
    (vswap! (:children task-handler) assoc-in
            [(:group task) worker-id]
            {:task id :child child})))

(defn- spawn-new [task-handler]
  (locking (:state task-handler)
    (loop []
      (when-let [id (next-task-id task-handler)]
        (start-process task-handler id)
        (recur)))))

(defn- handle-messages [{:keys [state queue children]}]
  (when-let [msg (queue/pop-message! queue)]
    (case (:type msg)
      :group-add
      (do (locking state
            (vswap! state assoc-in [:groups (:name msg)]
                    {:parallel-tasks (:parallel-tasks msg)
                     :status :running}))
          (vswap! children assoc (:name msg) (sorted-map)))
      :group-remove
      (do (locking state
            (vswap! state update :groups dissoc (:name msg)))
          (vswap! children dissoc (:name msg)))
      :send
      (let [{:keys [task-id input]} msg
            child (->> (for [[_ pool] @children
                             [_ {:keys [task child]}] pool
                             :when (= task task-id)]
                         child)
                       first)]
        (doto (.getOutputStream ^Process child)
          (.write (.getBytes ^String input))
          (.flush)))
      :kill
      (locking state
        (doseq [task-id (:task-ids msg)
                :let [child (->> (for [[_ pool] @children
                                       [_ {:keys [task child]}] pool
                                       :when (= task-id task)]
                                   child)
                                 first)]]
          (vswap! state update-in [:tasks task-id] assoc
                  :status :killed :end (now))
          (.destroy ^Process child))))
    true))

(defn- handle-finished-tasks [{:keys [state children]}]
  (let [finished (for [[group pool] @children
                       [worker {:keys [task ^Process child]}] pool
                       :when (not (.isAlive child))]
                   [group worker task (.exitValue child)])]
    (when (seq finished)
      (locking state
        (->> finished
             (reduce (fn [state [_ _ task code]]
                       (cond-> state
                         (not= (:status (get-in state [:tasks task]))
                               :killed)
                         (update-in [:tasks task] assoc
                                    :status (if (= code 0) :success :failed)
                                    :code code
                                    :end (now))))
                     @state)
             (vreset! state)))
      (doseq [[group worker _ _] finished]
        (vswap! children update group dissoc worker)))))

(defn- enqueue-delayed-tasks [{:keys [state]}]
  (locking state
    (doseq [[id task] (:tasks @state)
            :when (and (= (:status task) :stashed)
                       (when-let [^Date t (:enqueue-at task)]
                         (.before t (now))))]
      (vswap! state assoc-in [:tasks id :status] :queued))))

(defn- step [handler]
  (let [ret (handle-messages handler)]
    (doto handler
      (handle-finished-tasks)
      (enqueue-delayed-tasks)
      (spawn-new))
    ret))

(defn restart-handler [handler & {:keys [sync?]}]
  (let [f (fn []
            (when-not (step handler)
              (Thread/sleep 200))
            (recur))]
    (if sync?
      (f)
      (assoc handler :loop (future (f))))))

(defn start-handler [state queue & {:keys [sync?]}]
  (let [handler (make-task-handler state queue)]
    (restart-handler handler :sync? sync?)))

(defn stop-handler [handler]
  (future-cancel (:loop handler)))
