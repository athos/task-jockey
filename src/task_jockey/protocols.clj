(ns task-jockey.protocols)

(defprotocol ITransport
  (send-message [this msg]))
