(ns task-jockey.task)

(defn task-done? [task]
  (#{:success :failed :killed} (:status task)))
