(ns task-jockey.task)

(defn task-done? [task]
  (#{:success :failed :failed-to-spawn :killed} (:status task)))
