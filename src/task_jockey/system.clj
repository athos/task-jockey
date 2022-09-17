(ns task-jockey.system
  (:require [task-jockey.client :as client]
            [task-jockey.log :as log]
            [task-jockey.message-handler :as message]
            [task-jockey.server :as server]
            [task-jockey.settings :as settings]
            [task-jockey.system-state :as system]
            [task-jockey.task-handler :as handler]
            [task-jockey.transport :as transport])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn stop-system [{:keys [handler server]}]
  (when server
    (server/stop-server server))
  (when handler
    (handler/stop-handler handler))
  nil)

(defn start-system
  ([opts] (start-system nil opts))
  ([system {:keys [port] :as opts}]
   (let [opts' (settings/load-settings opts)
         logs-dir (settings/with-settings opts'
                    (.toPath (log/logs-dir)))]
     (Files/createDirectories logs-dir (into-array FileAttribute []))
     (stop-system system)
     (cond-> {}
       port (assoc :server (when port (server/start-server opts')))
       ;; handler should be started because it might be sync'ed
       true (assoc :handler
                   (if system
                     (handler/restart-handler (:handler system) opts')
                     (handler/start-handler system/state
                                            system/message-queue
                                            opts')))))))

(defn make-socket-client [opts]
  (let [opts' (settings/load-settings opts)]
    (client/->Client (transport/make-socket-transport opts) opts')))

(defn make-client [{:keys [port] :as opts}]
  (let [opts' (settings/load-settings opts)
        transport (if port
                    (transport/make-socket-transport opts')
                    (transport/make-fn-transport message/handle-message opts'))]
    (client/->Client transport opts')))
