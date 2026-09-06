(ns astrolabe.hub
  "A topic-keyed registry of open SSE connections.

  A hub stores connections and nothing else: add one to a topic, remove it,
  list a topic's connections. That is what varies between backends -- an atom
  here, a database elsewhere. Everything about how a connection is built, held
  open and torn down lives in [[astrolabe.connection]], so a new backend
  implements three methods and inherits the rest."
  (:require [astrolabe.connection :as conn]
            [astrolabe.queue :as queue]
            [astrolabe.sse :as sse]))

(defprotocol Hub
  (-subscribe!   [hub topic conn])
  (-unsubscribe! [hub topic conn])
  (-conns        [hub topic]))

(defrecord InMemoryHub [!topics]
  Hub
  (-subscribe! [_ topic conn]
    (swap! !topics update topic (fnil conj #{}) conn)
    nil)
  (-unsubscribe! [_ topic conn]
    (swap! !topics (fn [topics]
                     (let [remaining (disj (get topics topic #{}) conn)]
                       (if (seq remaining)
                         (assoc topics topic remaining)
                         (dissoc topics topic)))))
    nil)
  (-conns [_ topic]
    (vec (get @!topics topic))))

(defn subscribe!
  "Register `conn` on `topic`. A connection belongs to exactly one topic --
  `send!` unsubscribes a refusing connection using its own `:topic` -- so do
  not use this to put one connection on a second topic."
  [hub topic conn]
  (-subscribe! hub topic conn))

(defn unsubscribe! [hub topic conn] (-unsubscribe! hub topic conn))

(defn conns
  "The connections currently on `topic`, as a stable snapshot safe to iterate
  while other threads subscribe and unsubscribe."
  [hub topic]
  (-conns hub topic))

(defn in-memory
  "A single-node hub. Connections live in this process and are lost on restart;
  browsers reconnect via Datastar's SSE retry."
  []
  (->InMemoryHub (atom {})))

(defn- close-conn!
  "Unsubscribe a connection and close its queue, ending its drain loop. The
  drain's own cleanup closes the generator."
  [hub conn]
  (-unsubscribe! hub (:topic conn) conn)
  (queue/close! (:queue conn))
  nil)

(defn send!
  "Enqueue one frame for one connection. Never writes and never blocks. If the
  connection's queue refuses the frame, the connection is closed."
  [hub conn frame]
  (let [accepted (boolean (queue/offer! (:queue conn) frame))]
    (when-not accepted
      (close-conn! hub conn))
    accepted))

(defn broadcast!
  "Send one already-rendered frame to every connection on `topic`. Use when
  every client sees the same HTML -- render once with `astrolabe.sse/frame`
  and the same frame reaches everyone."
  [hub topic frame]
  (doseq [conn (conns hub topic)]
    (send! hub conn frame))
  nil)

(defn broadcast-each!
  "Call `f` with each connection on `topic` and send it the frame `f` returns.
  Use when clients see different HTML; you pay one render per connection.

  A render that throws for one connection aborts the fan-out -- unlike a dead
  client, which cannot."
  [hub topic f]
  (doseq [conn (conns hub topic)]
    (send! hub conn (f conn)))
  nil)

(defn connect!
  "Return SSE response data that spawns a connection with `connector`,
  registers it on `topic`, drains it until the client disconnects, and cleans
  up on the way out.

  This is the glue between a hub and a connection and holds no policy of its
  own: the queue, the drain loop and disconnect detection all belong to the
  connector, and storage belongs to the hub.

  Opts:
  - `:data`         app data attached to the connection, readable with
                    [[astrolabe.connection/data]]
  - `:on-close`     SDK on-close callback, passed through to the response
  - `:on-exception` SDK on-exception callback, passed through to the response"
  ([hub connector topic] (connect! hub connector topic {}))
  ([hub connector topic {:keys [data on-close on-exception]}]
   (sse/response
    (cond-> {:on-open
             (fn [sse-gen]
               (let [c (conn/-open connector sse-gen topic data)]
                 (-subscribe! hub topic c)
                 (try
                   (conn/-drain! connector c)
                   (catch Exception _
                     nil)   ; a dead client is normal; fall through to cleanup
                   (finally
                     (-unsubscribe! hub topic c)
                     (conn/-close! connector c)))))}
      on-close     (assoc :on-close on-close)
      on-exception (assoc :on-exception on-exception)))))
