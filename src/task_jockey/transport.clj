(ns task-jockey.transport
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [task-jockey.protocols :as proto])
  (:import [java.io Closeable PushbackReader]
           [java.net Socket]))

(defn- send-message* [out msg]
  (binding [*out* out]
    (prn msg)))

(defn- recv-message* [in]
  (edn/read {:eof nil} in))

(defrecord SocketTransport [^Socket socket in out]
  proto/ITransport
  (send-message [_ msg]
    (send-message* out msg)
    (recv-message* in))
  (send-message-with-callback [_ msg callback]
    (send-message* out msg)
    (loop []
      (let [res (recv-message* in)]
        (if (:continue? res)
          (do (callback (dissoc res :continue?))
              (recur))
          res))))
  Closeable
  (close [_]
    (.close socket)))

(defn make-socket-transport [{:keys [host port]}]
  (let [socket (Socket. host port)
        in (PushbackReader. (io/reader (.getInputStream socket)))
        out (io/writer (.getOutputStream socket))]
    (->SocketTransport socket in out)))

(defrecord FnTransport [handler]
  proto/ITransport
  (send-message [_ msg]
    (handler msg))
  (send-message-with-callback [_ msg callback]
    (loop [res (handler msg)]
      (if-let [cont (:cont res)]
        (do (callback (dissoc res :cont))
            (recur (cont)))
        res)))
  Closeable
  (close [_]))

(defn make-fn-transport [handler]
  (->FnTransport handler))
