(ns task-jockey.utils
  (:import [java.text SimpleDateFormat]
           [java.util Date]))

(defn ->coll [x]
  (if (or (nil? x) (coll? x))
    (vec x)
    [x]))

(defn stringify-date [^Date date]
  (let [formatter (SimpleDateFormat. "HH:mm:ss")]
    (.format formatter date)))

(defn now ^Date []
  (Date.))
