(ns task-jockey.system-state
  (:require [task-jockey.message-queue :as queue]
            [task-jockey.state :as state]))

(def state (volatile! (state/make-state)))

(def message-queue (queue/make-message-queue))
