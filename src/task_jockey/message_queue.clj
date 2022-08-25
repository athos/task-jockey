(ns task-jockey.message-queue)

(defn make-message-queue []
  (atom (clojure.lang.PersistentQueue/EMPTY)))

(defn push-message! [queue msg]
  (swap! queue conj msg)
  nil)

(defn pop-message! [queue]
  (let [msg (volatile! nil)]
    (swap! queue
           (fn [queue]
             (vreset! msg (first queue))
             (pop queue)))
    @msg))

