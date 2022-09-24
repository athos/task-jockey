(ns task-jockey.server
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [task-jockey.message-handler :as message]
            [task-jockey.settings :as settings])
  (:import [java.io PushbackReader]
           [java.net InetAddress Socket ServerSocket SocketException]))

(defrecord Server [socket host port])

(defn- send-message [out msg]
  (binding [*out* out]
    (prn msg)))

(defn- recv-message [in]
  (edn/read {:eof nil} in))

(defn- accept [^Socket conn in out opts]
  (settings/with-settings opts
    (try
      (loop []
        (when-let [msg (recv-message in)]
          (let [res (message/handle-message msg)
                res (loop [{:keys [cont] :as res} res]
                      (if cont
                        (let [res' (-> res
                                       (assoc :continue? true)
                                       (dissoc :cont))]
                          (send-message out res')
                          (recur (cont)))
                        res))]
            (send-message out res)
            (recur))))
      (finally
        (.close conn)))))

(defn start-server [{:keys [host port] :as opts}]
  (let [address (InetAddress/getByName host)
        socket (ServerSocket. port 0 address)]
    (future
      (loop []
        (when (not (.isClosed socket))
          (try
            (let [conn (.accept socket)
                  in (PushbackReader. (io/reader (.getInputStream conn)))
                  out (io/writer (.getOutputStream conn))]
              (future
                (accept conn in out opts)))
            (catch SocketException _disconnect))
          (recur))))
    (->Server socket host port)))

(defn stop-server [{:keys [^ServerSocket socket]}]
  (when-not (.isClosed socket)
    (.close socket)))
