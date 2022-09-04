(ns task-jockey.server
  (:require [clojure.core.server :as server]
            [clojure.edn :as edn]
            [task-jockey.message-handler :as message]))

(defrecord Server [socket name])

(def send-message prn)
(defn recv-message []
  (edn/read {:eof nil} *in*))

(defn accept []
  (loop []
    (when-let [msg (recv-message)]
      (let [resp (message/handle-message msg)]
        (send-message resp)
        (recur)))))

(defn start-server [{:keys [host port] :as opts}]
  (let [name (format "task-jockey.%d" port)
        opts' (assoc opts
                     :name name
                     :accept `accept
                     :host host
                     :port port)]
    (->Server (server/start-server opts') name)))

(defn stop-server [server]
  (server/stop-server (:name server)))
