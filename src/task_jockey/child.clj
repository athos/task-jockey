(ns task-jockey.child
  (:require [task-jockey.protocols :as proto]))

(extend-protocol proto/IChild
  Process
  (done? [this] (not (.isAlive this)))
  (result [this] (.exitValue this))
  (kill [this] (.destroy this))
  (write-input [this input]
    (doto (.getOutputStream this)
      (.write (.getBytes ^String input))
      (.flush))))
