(ns task-jockey.client
  (:refer-clojure :exclude [send])
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io])
  (:import [java.io Closeable PushbackReader]
           [java.net Socket]))

(defrecord Client [^Socket socket in out]
  Closeable
  (close [_] (.close socket)))

(defn make-client [{:keys [host port]}]
  (let [socket (Socket. host port)
        in (PushbackReader. (io/reader (.getInputStream socket)))
        out (io/writer (.getOutputStream socket))]
    (->Client socket in out)))

(defn send [client msg]
  (binding [*out* (:out client)]
    (prn msg)))

(defn recv [client]
  (edn/read {:eof nil} (:in client)))

(defn send-and-recv [client msg]
  (send client msg)
  (recv client))
