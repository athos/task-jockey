(ns task-jockey.utils
  (:import [java.text SimpleDateFormat]
           [java.util Date]))

(defn stringify-date [^Date date]
  (let [formatter (SimpleDateFormat. "HH:mm:ss")]
    (.format formatter date)))
