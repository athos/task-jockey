(ns task-jockey.settings)

(def ^:dynamic *settings*)

(defn settings []
  (assert (bound? #'*settings*)
          "task-jockey.settings/*settings* must be bound")
  *settings*)

(defmacro with-settings [settings & body]
  `(binding [*settings* ~settings]
     ~@body))

(def default-settings
  {:base-dir ".task-jockey"
   :host "localhost"})

(defn load-settings [opts]
  (merge default-settings opts))
