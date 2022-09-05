(ns task-jockey.protocols)

(defprotocol ITransport
  (send-message [this msg])
  (send-message-with-callback [this msg callback]))
