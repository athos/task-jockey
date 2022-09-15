(ns task-jockey.system
  (:require [task-jockey.message-handler :as message]
            [task-jockey.server :as server]
            [task-jockey.system-state :as system]
            [task-jockey.task-handler :as handler]
            [task-jockey.transport :as transport]))

(defn stop-system [{:keys [handler server]}]
  (when server
    (server/stop-server server))
  (when handler
    (handler/stop-handler handler))
  nil)

(defn restart-system [system {:keys [port] :as opts}]
  (stop-system system)
  (let [handler (handler/start-handler system/state system/message-queue)]
    (cond-> {:handler handler}
      port (assoc :server (when port (server/start-server opts))))))

(defn start-system [opts]
  (restart-system nil opts))

(defn make-socket-client [opts]
  (transport/make-socket-transport opts))

(defn make-client [{:keys [port] :as opts}]
  (if port
    (transport/make-socket-transport opts)
    (transport/make-fn-transport message/handle-message)))
