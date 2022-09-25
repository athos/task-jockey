(ns task-jockey.task
  (:require [clojure.java.io :as io]
            [clojure.string :as str]))

(defn task-done? [task]
  (#{:success :failed :failed-to-spawn :dependency-failed :killed}
   (:status task)))

(defn task-failed? [task]
  (#{:failed :failed-to-spawn :dependency-failed :killed} (:status task)))

(defmulti prepare-task (fn [task] (type (:command task))))

(defn- cwd []
  (System/getProperty "user.dir"))

(defn- envs []
  (into {} (System/getenv)))

(defmethod prepare-task String [task]
  (let [dir (or (:dir task) (cwd))]
    (assoc task
           :task/type :process
           :dir (.getCanonicalPath (io/file dir))
           :envs (envs))))

(defmethod prepare-task clojure.lang.Symbol [task]
  (prepare-task (update task :command str)))

(defmethod prepare-task clojure.lang.IPersistentVector [task]
  (let [cmd (str/join \space (map pr-str (:command task)))]
    (prepare-task (assoc task :command cmd))))
