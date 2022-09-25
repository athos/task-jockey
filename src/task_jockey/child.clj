(ns task-jockey.child
  (:require [clojure.java.io :as io]
            [task-jockey.log :as log]
            [task-jockey.protocols :as proto]))

(extend-protocol proto/IChild
  Process
  (done? [this] (not (.isAlive this)))
  (result [this] (.exitValue this))
  (kill [this] (.destroy this))
  (write-input [this input]
    (doto (.getOutputStream this)
      (.write (.getBytes ^String input))
      (.flush))))

(defmulti start-task (fn [task _worker-id] (:task/type task)))

(defmethod start-task :process [task worker-id]
  (let [log-file (log/log-file-path (:id task))
        ^java.util.List command ["sh" "-c" (:command task)]
        pb (doto (ProcessBuilder. command)
             (.redirectOutput log-file)
             (.redirectError log-file)
             (.directory (io/file (:dir task))))]
    (doto (.environment pb)
      (.clear)
      (.putAll (:envs task))
      (.put "TASK_JOCKEY_GROUP" (:group task))
      (.put "TASK_JOCKEY_WORKER_ID" (str worker-id)))
    (.start pb)))
