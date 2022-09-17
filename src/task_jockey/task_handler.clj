(ns task-jockey.task-handler
  (:require [clojure.java.io :as io]
            [task-jockey.children :as children]
            [task-jockey.log :as log]
            [task-jockey.message-queue :as queue]
            [task-jockey.settings :as settings]
            [task-jockey.task :as task])
  (:import [java.util Date]))

(defn make-task-handler [state queue]
  {:state state
   :queue queue
   :local (volatile! {:children {"default" (sorted-map)}})})

(defn- next-task-id [{:keys [state local]}]
  (->> (:tasks @state)
       (filter (fn [[_ {:keys [group] :as task}]]
                 (and (= (:status task) :queued)
                      (let [running-tasks (get-in @local [:children group])]
                        (< (count running-tasks)
                           (get-in @state [:groups group :parallel-tasks])))
                      (let [deps (:dependencies task)]
                        (or (empty? deps)
                            (every? #(task/task-done? (get (:tasks @state) %)) deps))))))
       ffirst))

(defn- now ^Date [] (Date.))

(defn- start-process [{:keys [state local]} id]
  (let [task (get-in @state [:tasks id])
        worker-id (children/next-group-worker (:children @local) (:group task))
        log-file (log/log-file-path id)
        command (into-array String ["sh" "-c" (:command task)])
        pb (doto (ProcessBuilder. ^"[Ljava.lang.String;" command)
             (.redirectOutput log-file)
             (.redirectError log-file)
             (.directory (io/file (:dir task))))]
    (doto (.environment pb)
      (.clear)
      (.putAll (:envs task))
      (.put "TASK_JOCKEY_GROUP" (:group task))
      (.put "TASK_JOCKEY_WORKER_ID" (str worker-id)))
    (try
      (let [child (.start pb)]
        (vswap! state update-in [:tasks id]
                assoc :status :running :start (now))
        (vswap! local update :children
                children/add-child (:group task) worker-id id child))
      (catch Exception e
        (vswap! state update-in [:tasks id]
                assoc :status :failed-to-spawn :reason (ex-message e)
                :start (now) :end (now))))))

(defn- spawn-new [task-handler]
  (locking (:state task-handler)
    (loop []
      (when-let [id (next-task-id task-handler)]
        (start-process task-handler id)
        (recur)))))

(defn- handle-messages [{:keys [state queue local]}]
  (when-let [msg (queue/pop-message! queue)]
    (case (:type msg)
      :group-add
      (do (locking state
            (vswap! state assoc-in [:groups (:name msg)]
                    {:parallel-tasks (:parallel-tasks msg)
                     :status :running}))
          (vswap! local assoc-in [:children (:name msg)] (sorted-map)))
      :group-remove
      (do (locking state
            (vswap! state update :groups dissoc (:name msg)))
          (vswap! local update :children dissoc (:name msg)))
      :send
      (let [{:keys [task-id input]} msg
            child (children/get-child (:children @local) task-id)]
        (doto (.getOutputStream ^Process child)
          (.write (.getBytes ^String input))
          (.flush)))
      :kill
      (locking state
        (let [task-ids (cond (:group msg)
                             (for [task (vals (:tasks @state))
                                   :when (and (= (:group task) (:group msg))
                                              (= (:status task) :running))]
                               (:id task))

                             (seq (:task-ids msg))
                             (:task-ids msg)

                             :else
                             (for [task (vals (:tasks @state))
                                   :when (= (:status task) :running)]
                               (:id task)))]
          (doseq [task-id task-ids
                  :let [child (children/get-child (:children @local) task-id)]]
            (vswap! state update-in [:tasks task-id] assoc
                    :status :killed :end (now))
            (.destroy ^Process child)))))
    true))

(defn- handle-finished-tasks [{:keys [state local]}]
  (let [finished (for [[group pool] (:children @local)
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
        (vswap! local update-in [:children group] dissoc worker)))))

(defn- enqueue-delayed-tasks [{:keys [state]}]
  (locking state
    (doseq [[id task] (:tasks @state)
            :when (and (= (:status task) :stashed)
                       (when-let [^Date t (:enqueue-at task)]
                         (.before t (now))))]
      (vswap! state assoc-in [:tasks id :status] :queued))))

(defn- check-failed-dependencies [{:keys [state]}]
  (locking state
    (doseq [[id task] (:tasks @state)
            :when (and (= (:status task) :queued)
                       (some #(task/task-failed? (get-in @state [:tasks %]))
                             (:dependencies task)))]
      (vswap! state update-in [:tasks id] assoc
              :status :dependency-failed :start (now) :end (now)))))

(defn- step [handler]
  (let [ret (handle-messages handler)]
    (doto handler
      (handle-finished-tasks)
      (enqueue-delayed-tasks)
      (check-failed-dependencies)
      (spawn-new))
    ret))

(defn restart-handler [handler {:keys [sync?] :as opts}]
  (let [f (fn []
            (settings/with-settings opts
              (loop []
                (when-not (step handler)
                  (Thread/sleep 200))
                (recur))))]
    (if sync?
      (f)
      (assoc handler :loop (future (f))))))

(defn start-handler [state queue opts]
  (let [handler (make-task-handler state queue)]
    (restart-handler handler opts)))

(defn stop-handler [handler]
  (future-cancel (:loop handler)))
