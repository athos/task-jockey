(ns task-jockey.client
  (:refer-clojure :exclude [send])
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [task-jockey.protocols :as proto]
            [task-jockey.task :as task]))

(defn- send-and-recv [client type & fields]
  (let [msg (apply array-map :type type fields)]
    (proto/send-message client msg)))

(defn- envs []
  (into {} (System/getenv)))

(defn add [client {:keys [command group dir after stashed delay label]}]
  (let [cmd (if (coll? command)
              (str/join \space (map pr-str command))
              (str command))
        dir (or dir (System/getProperty "user.dir"))]
    (send-and-recv client :add
                   :command cmd
                   :group (or (some-> group str) "default")
                   :dir (.getCanonicalPath (io/file dir))
                   :envs (envs)
                   :dependencies (set after)
                   :stashed stashed
                   :enqueue-at delay
                   :label (some-> label str))))

(defn status [client]
  (send-and-recv client :status))

(defn clean [client]
  (send-and-recv client :clean))

(defn stash [client task-ids]
  (send-and-recv client :stash :task-ids task-ids))

(defn enqueue [client task-ids {:keys [delay]}]
  (send-and-recv client :enqueue :task-ids task-ids :enqueue-at delay))

(defn switch [client task-id1 task-id2]
  (send-and-recv client :switch
                 :task-id1 task-id1
                 :task-id2 task-id2))

(defn restart [client task-ids]
  (send-and-recv client :restart :task-ids task-ids))

(defn edit [client task-id command]
  (send-and-recv client :edit
                 :task-id task-id
                 :command command))

(defn log [client task-ids]
  (send-and-recv client :log-request :task-ids task-ids))

(defn follow [client task-id callback]
  (let [msg {:type :follow, :task-id task-id}]
    (proto/send-message-with-callback client msg callback)))

(defn send [client task-id input]
  (send-and-recv client :send
                 :task-id task-id
                 :input input))

(defn kill [client group task-ids]
  (send-and-recv client :kill :group group :task-ids task-ids))

(defn wait [client group task-ids callback]
  (loop [first? true, previous-statuses {}]
    (let [res (send-and-recv client :status)
          tasks (get-in res [:status :tasks])
          target-ids (if group
                       (into #{} (filter #(= (:group %) group))
                             (vals tasks))
                       (if (seq task-ids)
                         (set task-ids)
                         (into #{} (map :id) (vals tasks))))
          [finished? changed]
          (reduce (fn [[finished? changed] task-id]
                    (let [task (get tasks task-id)]
                      [(and finished? (task/task-done? task))
                       (cond-> changed
                         (let [prev (get previous-statuses (:id task))
                               curr (:status task)]
                           (and (not= prev curr)
                                (or prev (= curr :running))))
                         (assoc (:id task) (:status task)))]))
                  [true (sorted-map)]
                  target-ids)]
      (doseq [[id status] changed]
        (callback id (get previous-statuses id) status first?))
      (when-not finished?
        (Thread/sleep 2000)
        (recur false (into previous-statuses changed))))))

(defn parallel [client group tasks]
  (send-and-recv client :parallel
                 :group (or group "default")
                 :parallel-tasks tasks))

(defn groups [client]
  (send-and-recv client :group-list))

(defn group-add [client name parallel-tasks]
  (send-and-recv client :group-add
                 :name name
                 :parallel-tasks parallel-tasks))

(defn group-rm [client name]
  (send-and-recv client :group-remove :name name))
