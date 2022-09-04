(ns task-jockey.transport
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [task-jockey.protocols :as proto])
  (:import [java.io Closeable PushbackReader]
           [java.net Socket]))

(defrecord SocketTransport [^Socket socket in out]
  proto/ITransport
  (send-message [_ msg]
    (binding [*out* out]
      (prn msg))
    (edn/read {:eof nil} in))
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
  Closeable
  (close [_]))

(defn make-fn-transport [handler]
  (->FnTransport handler))
