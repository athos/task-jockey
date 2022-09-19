(ns task-jockey.system
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [task-jockey.client :as client]
            [task-jockey.log :as log]
            [task-jockey.message-handler :as message]
            [task-jockey.server :as server]
            [task-jockey.settings :as settings]
            [task-jockey.system-state :as system]
            [task-jockey.task-handler :as handler]
            [task-jockey.transport :as transport])
  (:import [java.io File PushbackReader]))

(defn- port-file ^File [{:keys [base-dir]}]
  (io/file base-dir "task-jockey.port"))

(defn- save-port-file [settings port]
  (with-open [w (io/writer (port-file settings))]
    (binding [*out* w]
      (prn port))))

(defn- load-port-file [settings]
  (with-open [r (PushbackReader. (io/reader (port-file settings)))]
    (edn/read r)))

(defn- delete-port-file [settings]
  (let [port-file (port-file settings)]
    (when (.exists port-file)
      (.delete port-file))))

(defn stop-system [{:keys [handler server settings]}]
  (when server
    (server/stop-server server))
  (when handler
    (handler/stop-handler handler))
  (delete-port-file settings)
  nil)

(defn start-system
  ([opts] (start-system nil opts))
  ([system opts]
   (let [{:keys [port] :as settings} (settings/load-settings opts)
         logs-dir (settings/with-settings settings
                    (log/logs-dir))]
     (stop-system system)
     (.mkdir logs-dir)
     (let [server (when port (server/start-server settings))]
       (save-port-file settings (or port :local))
       (cond-> {:settings settings}
         server (assoc :server server)
         ;; handler should be started because it might be sync'ed
         true (assoc :handler
                     (if system
                       (handler/restart-handler (:handler system) settings)
                       (handler/start-handler system/state
                                              system/message-queue
                                              settings))))))))

(defn- load-settings-with-port-resolved [opts]
  (let [{:keys [port] :as settings} (settings/load-settings opts)
        port' (or port (load-port-file settings))]
    (cond-> settings
      (not= port' :local)
      (assoc :port port'))))

(defn make-socket-client [opts]
  (let [settings (load-settings-with-port-resolved opts)]
    (client/->Client (transport/make-socket-transport settings) settings)))

(defn make-client [{:keys [port] :as opts}]
  (let [settings (load-settings-with-port-resolved opts)
        transport (if port
                    (transport/make-socket-transport settings)
                    (transport/make-fn-transport message/handle-message
                                                 settings))]
    (client/->Client transport settings)))
