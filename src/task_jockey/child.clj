(ns task-jockey.child
  (:require [clojure.java.io :as io]
            [task-jockey.log :as log]
            [task-jockey.protocols :as proto])
  (:import [java.io PrintWriter Writer]
           [java.util List]
           [java.util.concurrent Future]))

(extend-protocol proto/IChild
  Process
  (done? [this] (not (.isAlive this)))
  (result [this] (.exitValue this))
  (succeeded? [_ res] (= res 0))
  (kill [this] (.destroy this))
  (write-input [this input]
    (doto (.getOutputStream this)
      (.write (.getBytes ^String input))
      (.flush)))

  Future
  (done? [this] (future-done? this))
  (result [this]
    (try
      (deref this)
      nil
      (catch Throwable t t)))
  (succeeded? [_ res] (not (instance? Throwable res)))
  (kill [this] (future-cancel this))
  (write-input [_ _]))

(defmulti start-task (fn [task _worker-id] (:task/type task)))

(defmethod start-task :process [task worker-id]
  (let [log-file (log/log-file-path (:id task))
        ^List command ["sh" "-c" (:command task)]
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

(defmethod start-task :thread [task _]
  (let [log-file (log/log-file-path (:id task))]
    (future
      (with-open [w (io/writer log-file)]
        (binding [*ns* (the-ns 'user)
                  *out* w
                  *err* w]
          (try
            (eval (:command task))
            (catch Throwable t
              (.printStackTrace t (PrintWriter. ^Writer *err*))
              (throw t))))))))
