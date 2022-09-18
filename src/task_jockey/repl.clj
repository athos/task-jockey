(ns task-jockey.repl
  (:refer-clojure :exclude [send])
  (:require [task-jockey.client :as client]
            [task-jockey.log :as log]
            [task-jockey.state :as state]
            [task-jockey.system :as system]
            [task-jockey.utils :as utils]))

(def ^:private system nil)

(defn- current-client []
  (or (:client system)
      (throw (ex-info "Call task-jockey.repl/start-system! first" {}))))

(defn add [command & {:as opts}]
  (let [opts' (cond-> (assoc opts :command command)
                (:after opts)
                (update :after utils/->coll))
        res (client/add (current-client) opts')]
    (if (:print-task-id opts)
      (prn (:task-id res))
      (select-keys res [:task-id :enqueue-at]))))

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

(defn enqueue [id-or-ids & {:as opts}]
  (let [task-ids (utils/->coll id-or-ids)
        res (client/enqueue (current-client) task-ids opts)]
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

(defn kill
  ([] (kill nil))
  ([ids-or-group]
   (let [res (client/kill (current-client)
                          (when (string? ids-or-group) ids-or-group)
                          (when (or (int? ids-or-group) (coll? ids-or-group))
                            (utils/->coll ids-or-group)))]
     (println (:message res)))))

(defn wait
  ([] (wait nil))
  ([ids-or-group & {:keys [callback]}]
   (letfn [(callback' [id prev curr added?]
             (callback {:id id :old prev :new curr :added? added?}))]
     (client/wait (current-client)
                  (when (string? ids-or-group) ids-or-group)
                  (when (or (int? ids-or-group) (coll? ids-or-group))
                    (utils/->coll ids-or-group))
                  (if callback callback' (constantly nil))))))

(defn reset []
  (client/reset (current-client))
  nil)

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
  (system/stop-system system))

(defn stop-system! []
  (alter-var-root #'system stop-system*)
  :stopped)

(defn- ensure-stopped [system]
  (when system
    (stop-system* system)))

(defn start-system! [& {:keys [host] :or {host "localhost"} :as opts}]
  (letfn [(start! [system]
            (ensure-stopped system)
            (let [opts' (assoc opts :host host)
                  system' (system/start-system system opts')]
              (assoc system' :client (system/make-client opts'))))]
    (alter-var-root #'system start!)
    :started))

(defn connect! [& {:keys [host] :or {host "localhost"} :as opts}]
  (alter-var-root #'system
                  (fn [system]
                    (ensure-stopped system)
                    (let [opts' (assoc opts :host host)]
                      {:client (system/make-socket-client opts')})))
  :connected)

(defn disconnect! []
  (stop-system!)
  :disconnected)
