(ns task-jockey.task)

(defn task-done? [task]
  (#{:success :failed :failed-to-spawn :dependency-failed :killed}
   (:status task)))

(defn task-failed? [task]
  (#{:failed :failed-to-spawn :dependency-failed :killed} (:status task)))
