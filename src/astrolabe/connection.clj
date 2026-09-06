(ns astrolabe.connection
  "How an SSE connection is born, held open, and torn down.

  This is deliberately separate from [[astrolabe.hub]]. A hub stores
  connections by topic, and how it stores them varies -- an atom today, a
  database later. The machinery here does not vary: every backend builds a
  queue, drains it, detects the client going away, and tears the connection
  down the same way."
  (:require [astrolabe.queue :as queue]
            [astrolabe.sse :as sse]
            [starfederation.datastar.clojure.api :as d*]))

(defrecord Connection [sse-gen queue topic data])

(defn data
  "The app-supplied data attached to a connection at `connect!` time."
  [conn]
  (:data conn))

(defprotocol Connector
  "Spawns connections. Implement this to replace the connection machinery
  wholesale; to vary the queue or the drain loop, pass `:queue-fn` / `:drain`
  to [[connector]] instead."
  (-open [connector sse-gen topic data]
    "Build a Connection with its queue. Does not subscribe and does not block.")
  (-drain! [connector conn]
    "Block, draining `conn`'s queue and writing frames, until the queue closes.")
  (-close! [connector conn]
    "Close the queue, then the SSE generator."))

(defn default-drain
  "Take frames until the queue closes, writing each one."
  [_conn q write!]
  (loop []
    (when-let [frame (queue/take! q)]
      (write! frame)
      (recur))))

(defrecord DefaultConnector [queue-fn drain]
  Connector
  (-open [_ sse-gen topic data]
    (->Connection sse-gen (queue-fn {:topic topic :data data}) topic data))

  (-drain! [_ conn]
    (let [{:keys [sse-gen queue]} conn
          write! (fn [frame]
                   (when-not (sse/apply! sse-gen frame)
                     ;; The client is gone. The SDK never throws for this -- the
                     ;; adapter catches the IOException, closes the generator,
                     ;; and every later write silently returns false -- so this
                     ;; verdict is the only signal we get. Closing the queue
                     ;; ends the drain, which lets the caller clean up.
                     (queue/close! queue)))]
      (drain conn queue write!)))

  (-close! [_ conn]
    (queue/close! (:queue conn))
    (d*/close-sse! (:sse-gen conn))))

(defn connector
  "Build the default connector.

  Opts:
  - `:queue-fn` a keyword (`:bounded` `:latest` `:unbounded`) or a function of
    `{:topic :data}` returning a queue; defaults to `:bounded`
  - `:drain` `(fn [conn queue write!])` that blocks until the queue closes;
    defaults to [[default-drain]]"
  ([] (connector {}))
  ([{:keys [queue-fn drain]}]
   (->DefaultConnector (queue/->queue-fn (or queue-fn :bounded))
                       (or drain default-drain))))
