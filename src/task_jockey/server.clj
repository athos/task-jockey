(ns task-jockey.server
  (:require [clojure.core.server :as server]
            [clojure.edn :as edn]
            [task-jockey.message-handler :as message]
            [task-jockey.settings :as settings]))

(defrecord Server [socket name])

(def send-message prn)
(defn recv-message []
  (edn/read {:eof nil} *in*))

(defn accept [opts]
  (settings/with-settings opts
    (loop []
      (when-let [msg (recv-message)]
        (let [res (message/handle-message msg)
              res (loop [{:keys [cont] :as res} res]
                    (if cont
                      (let [res' (-> res
                                     (assoc :continue? true)
                                     (dissoc :cont))]
                        (send-message res')
                        (recur (cont)))
                      res))]
          (send-message res)
          (recur))))))

(defn start-server [{:keys [host port] :as opts}]
  (let [name (format "task-jockey.%d" port)
        opts' (assoc opts
                     :name name
                     :accept `accept
                     :args [opts]
                     :host host
                     :port port)]
    (->Server (server/start-server opts') name)))

(defn stop-server [server]
  (server/stop-server (:name server)))
