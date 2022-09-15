(ns task-jockey.system
  (:require [task-jockey.server :as server]
            [task-jockey.system-state :as system]
            [task-jockey.task-handler :as handler]))

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
