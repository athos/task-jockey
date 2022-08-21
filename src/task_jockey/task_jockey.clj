(ns task-jockey.task-jockey
  (:require [clojure.java.io :as io]
            [clojure.pprint :as pp])
  (:import [java.io File]
           [java.text SimpleDateFormat]
           [java.util Date]))

(def task-log-directory "task_logs")

(def message-queue
  (atom (clojure.lang.PersistentQueue/EMPTY)))

(defn push-message! [queue msg]
  (swap! queue conj msg)
  nil)

(defn pop-message! [queue]
  (let [msg (volatile! nil)]
    (swap! queue
           (fn [queue]
             (vreset! msg (first queue))
             (pop queue)))
    @msg))

(defn make-state []
  {:tasks (sorted-map)
   :groups {"default" {:parallel-tasks 1 :status :running}}})

(def state (volatile! (make-state)))

(defn make-task-handler [state queue]
  {:state state
   :queue queue
   :children {"default" (sorted-map)}})

(def task-handler (volatile! (make-task-handler state message-queue)))

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

(defn stringify-date [^Date date]
  (let [formatter (SimpleDateFormat. "HH:mm:ss")]
    (.format formatter date)))

(defn print-log [task]
  (let [log-file (log-file-path (:id task))]
    (when (.exists log-file)
      (printf "--- Task %d: %s ---\n" (:id task) (name (:status task)))
      (println "Command:" (pr-str (:command task)))
      (println "  Start:" (stringify-date (:start task)))
      (when (:end task)
        (println "    End:" (stringify-date (:end task))))
      (newline)
      (println "output:")
      (print (slurp log-file))
      (flush))))

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
                             (cond-> (update task :command pr-str)
                               (:start task)
                               (update :start stringify-date)
                               (:end task)
                               (update :end stringify-date)))))
                   (sort-by :id))]
    (print-group-summary group-name group)
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

(defn handle-messages [{:keys [state] :as task-handler}]
  (if-let [msg (pop-message! (:queue task-handler))]
    (case (:action msg)
      :group-add
      (do (locking state
            (vswap! state assoc-in [:groups (:name msg)]
                    {:parallel-tasks (:parallel-tasks msg)
                     :status :running}))
          (assoc-in task-handler [:children (:name msg)] (sorted-map)))
      :group-remove
      (do (locking state
            (vswap! state update :groups dissoc (:name msg)))
          (update task-handler :children dissoc (:name msg))))
    task-handler))

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

(defn step [handler]
  (-> handler
      (handle-messages)
      (handle-finished-tasks)
      (spawn-new)))

(defn start-loop [state queue]
  (future
    (loop [handler (make-task-handler state queue)]
      (Thread/sleep 200)
      (recur (step handler)))))

(defn group [& {:keys [action name parallel-tasks]
              :or {action :list parallel-tasks 1}}]
  (case action
    :list (doseq [[name group] (locking state
                                 (get @state :groups))]
            (print-group-summary name group)
            (newline))
    :add (let [msg {:action :group-add :name name
                    :parallel-tasks parallel-tasks}]
           (push-message! message-queue msg))
    :remove (let [msg {:action :group-remove :name name}]
              (push-message! message-queue msg))))
