(ns task-jockey.protocols)

(defprotocol ICloseable
  (close [this]))

(defprotocol ITransport
  (send-message [this msg])
  (send-message-with-callback [this msg callback]))

(defprotocol IChild
  (done? [this])
  (result [this])
  (succeeded? [this result])
  (kill [this])
  (write-input [this input]))
