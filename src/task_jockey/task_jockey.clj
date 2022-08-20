(ns task-jockey.task-jockey
  (:require [clojure.java.io :as io]
            [clojure.pprint :as pp])
  (:import [java.io File]
           [java.text SimpleDateFormat]
           [java.util Date]))

(def task-log-directory "task_logs")

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

(defn task-done? [task]
  (#{:success :failed} (:status task)))

(defn clean-tasks [state]
  (reduce-kv (fn [state id task]
               (cond-> state
                 (task-done? task)
                 (update :tasks dissoc id)))
             state
             (:tasks state)))

(defn log-file-path ^File [task-id]
  (io/file task-log-directory (str task-id ".log")))

(defn read-log-file [task-id]
  (slurp (log-file-path task-id)))

(defn stringify-date [^Date date]
  (let [formatter (SimpleDateFormat. "HH:mm:ss")]
    (.format formatter date)))

(defn print-log [task]
  (printf "--- Task %d: %s ---\n" (:id task) (name (:status task)))
  (println "Command:" (pr-str (:command task)))
  (println "  Start:" (stringify-date (:start task)))
  (println "    End:" (stringify-date (:end task)))
  (newline)
  (println "output:")
  (print (read-log-file (:id task)))
  (flush))

(defn print-logs [state task-ids]
  (doseq [[_ task] (:tasks state)
          :when (or (empty? task-ids)
                    (contains? task-ids (:id task)))]
    (print-log task)
    (newline)))

(defn follow-logs [state task-id]
  (with-open [r (io/reader (log-file-path task-id))]
    (let [buf (char-array 1024)]
      (loop []
        (if (.ready r)
          (let [size (.read r buf)]
            (.write *out* buf 0 size)
            (recur))
          (let [task (locking state
                       (get-in @state [:tasks task-id]))]
            (.flush *out*)
            (when-not (task-done? task)
              (Thread/sleep 1000)
              (recur))))))))

(defn print-single-group [state group-name]
  (let [group (get-in state [:groups group-name])
        tasks (->> (:tasks state)
                   (keep (fn [[_ task]]
                           (when (= (:group task) group-name)
                             (cond-> (update task :command pr-str)
                               (:start task)
                               (update :start stringify-date)
                               (:end task)
                               (update :end stringify-date)))))
                   (sort-by :id))]
    (printf "Group \"%s\" (%d parallel): " group-name (:parallel-tasks group))
    (pp/print-table [:id :status :command :start :end] tasks)))

(defn print-all-groups [state]
  (->> (:groups state)
       keys
       (run! (partial print-single-group state))))

(defn next-task-id [{:keys [children state]}]
  (->> (:tasks @state)
       (filter (fn [[_ {:keys [group] :as task}]]
                 (and (= (:status task) :queued)
                      (let [running-tasks (get children group)]
                        (< (count running-tasks)
                           (get-in @state [:groups group :parallel-tasks]))))))
       ffirst))

(defn next-group-worker [{:keys [children]} group]
  (let [pool (get children group)]
    (or (->> pool
             (keep-indexed (fn [i [worker-id _]]
                             (when (not= i worker-id)
                               i)))
             first)
        (count pool))))

(defn now [] (Date.))

(defn start-process [task-handler id]
  (let [task (get-in @(:state task-handler) [:tasks id])
        worker-id (next-group-worker task-handler (:group task))
        log-file (log-file-path id)
        command (into-array String ["sh" "-c" (:command task)])
        child (-> (ProcessBuilder. command)
                  (.redirectOutput log-file)
                  (.redirectError log-file)
                  (.start))]
    (vswap! (:state task-handler) update-in [:tasks id]
            assoc :status :running :start (now))
    (assoc-in task-handler [:children (:group task) worker-id]
              {:task id :child child})))

(defn spawn-new [task-handler]
  (locking (:state task-handler)
    (loop [handler task-handler]
      (if-let [id (next-task-id handler)]
        (recur (start-process handler id))
        handler))))

(defn handle-finished-tasks [task-handler]
  (let [finished (for [[group pool] (:children task-handler)
                       [worker {:keys [task ^Process child]}] pool
                       :when (not (.isAlive child))]
                   [group worker task (.exitValue child)])]
    (if (seq finished)
      (do (locking (:state task-handler)
            (->> finished
                 (reduce (fn [state [_ _ task code]]
                           (update-in state [:tasks task] assoc
                                      :status (if (= code 0) :success :failed)
                                      :code code
                                      :end (now)))
                         @state)
                 (vreset! state)))
          (reduce (fn [handler [group worker _ _]]
                    (update-in handler [:children group] dissoc worker))
                  task-handler
                  finished))
      task-handler)))

(defn start-loop [state]
  (future
    (loop [handler (make-task-handler state)]
      (Thread/sleep 200)
      (-> handler
          (handle-finished-tasks)
          (spawn-new)
          (recur)))))
