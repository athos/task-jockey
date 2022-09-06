(ns task-jockey.repl
  (:refer-clojure :exclude [send])
  (:require [task-jockey.client :as client]
            [task-jockey.log :as log]
            [task-jockey.message-handler :as message]
            [task-jockey.server :as server]
            [task-jockey.state :as state]
            [task-jockey.transport :as transport]
            [task-jockey.system-state :as system]
            [task-jockey.task-handler :as handler]
            [task-jockey.utils :as utils]))

(def ^:private system nil)

(defn- current-client []
  (or (:client system)
      (throw (ex-info "Call task-jockey.repl/start-system! first" {}))))

(defn add [command & {:as opts}]
  (let [res (client/add (current-client) (assoc opts :command command))]
    (select-keys res [:task-id])))

(defn status [& {:keys [group edn]}]
  (let [res (client/status (current-client))]
    (when (= (:type res) :status-response)
      (cond edn (:status res)
            group (state/print-single-group (:status res) group)
            :else (state/print-all-groups (:status res))))))

(defn clean []
  (client/clean (current-client))
  nil)

(defn stash [id-or-ids]
  (let [task-ids (utils/->coll id-or-ids)
        res (client/stash (current-client) task-ids)]
    (println (:message res))))

(defn enqueue [id-or-ids]
  (let [task-ids (utils/->coll id-or-ids)
        res (client/enqueue (current-client) task-ids)]
    (println (:message res))))

(defn switch [task-id1 task-id2]
  (let [res (client/switch (current-client) task-id1 task-id2)]
    (println (:message res))))

(defn restart [id-or-ids]
  (let [task-ids (utils/->coll id-or-ids)
        res (client/restart (current-client) task-ids)]
    (println (:message res))))

(defn edit [task-id command]
  (client/edit (current-client) task-id command)
  nil)

(defn log
  ([] (log []))
  ([id-or-ids]
   (let [task-ids (utils/->coll id-or-ids)
         res (client/log (current-client) task-ids)]
     (log/print-logs (:tasks res) task-ids))))

(defn follow [task-id]
  (client/follow (current-client)
                 task-id
                 (fn [{:keys [content]}]
                   (print content)
                   (flush)))
  nil)

(defn send [task-id input]
  (client/send (current-client) task-id input)
  nil)

(defn kill [id-or-ids]
  (let [task-ids (utils/->coll id-or-ids)
        res (client/kill (current-client) task-ids)]
    (println (:message res))))

(defn parallel [n & {:keys [group]}]
  (let [res (client/parallel (current-client) group n)]
    (println (:message res))))

(defn groups []
  (let [res (client/groups (current-client))]
    (doseq [[name group] (:groups res)]
      (state/print-group-summary name group)
      (newline))))

(defn group-add [name & {:keys [parallel]}]
  (let [res (client/group-add (current-client) name parallel)]
    (println (:message res))))

(defn group-rm [name]
  (let [res (client/group-rm (current-client) name)]
    (println (:message res))))

(defn- stop-system* [system]
  (.close (current-client))
  (when-let [server (:server system)]
    (server/stop-server server))
  (when-let [fut (:loop system)]
    (future-cancel fut))
  nil)

(defn stop-system! []
  (alter-var-root #'system stop-system*)
  :stopped)

(defn- ensure-stopped [system]
  (when system
    (stop-system* system)))

(defn start-system! [& {:keys [host port] :or {host "localhost"} :as opts}]
  (letfn [(start! [system]
            (ensure-stopped system)
            (let [fut (future
                        (handler/start-loop system/state system/message-queue))
                  opts' (assoc opts :host host :port port)
                  [server client]
                  (if port
                    [(server/start-server opts')
                     (transport/make-socket-transport opts')]
                    [nil (transport/make-fn-transport message/handle-message)])]
              {:loop fut :server server :client client}))]
    (alter-var-root #'system start!)
    :started))

(defn connect! [& {:keys [host] :or {host "localhost"} :as opts}]
  (alter-var-root #'system
                  (fn [system]
                    (ensure-stopped system)
                    (let [opts' (assoc opts :host host)]
                      {:client (transport/make-socket-transport opts')})))
  :connected)

(defn disconnect! []
  (stop-system!)
  :disconnected)
