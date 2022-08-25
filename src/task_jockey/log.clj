(ns task-jockey.log
  (:require [clojure.java.io :as io]
            [task-jockey.task :as task]
            [task-jockey.utils :as utils])
  (:import [java.io File]))

(def task-log-directory "task_logs")

(defn log-file-path ^File [task-id]
  (io/file task-log-directory (str task-id ".log")))

(defn print-log [task]
  (let [log-file (log-file-path (:id task))]
    (when (.exists log-file)
      (printf "--- Task %d: %s ---\n" (:id task) (name (:status task)))
      (println "Command:" (:command task))
      (println "   Path:" (:path task))
      (when (:start task)
        (println "  Start:" (utils/stringify-date (:start task))))
      (when (:end task)
        (println "    End:" (utils/stringify-date (:end task))))
      (newline)
      (println "output:")
      (print (slurp log-file))
      (flush))))

(defn print-logs [state task-ids]
  (let [ids (set task-ids)]
    (doseq [[_ task] (:tasks state)
            :when (or (empty? ids)
                      (contains? ids (:id task)))]
      (print-log task)
      (newline))))

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
            (when-not (task/task-done? task)
              (Thread/sleep 1000)
              (recur))))))))
