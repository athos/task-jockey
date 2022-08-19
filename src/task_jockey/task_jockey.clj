(ns task-jockey.task-jockey
  (:require [clojure.pprint :as pp]
            [clojure.string :as str])
  (:import [java.text SimpleDateFormat]
           [java.util Date]))

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

(defn stringify-date [^Date date]
  (let [formatter (SimpleDateFormat. "HH:mm:ss")]
    (.format formatter date)))

(defn print-single-group [state group-name]
  (let [group (get-in state [:groups group-name])
        tasks (->> (:tasks state)
                   (keep (fn [[_ task]]
                           (when (= (:group task) group-name)
                             (cond-> (update task :command str/join)
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
        pb (ProcessBuilder. (:command task))
        child (.start pb)]
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
