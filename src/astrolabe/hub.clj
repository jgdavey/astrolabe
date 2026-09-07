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
  (-conns        [hub topic])
  (-topics       [hub]))

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
    (vec (get @!topics topic)))
  (-topics [_]
    (vec (keys @!topics))))

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

(defn topics
  "The topics currently holding at least one connection. A topic whose last
  connection has left is gone, so this never reports an empty topic."
  [hub]
  (-topics hub))

(defn in-memory
  "A single-node hub. Connections live in this process and are lost on restart;
  browsers reconnect via Datastar's SSE retry."
  []
  (->InMemoryHub (atom {})))

(defn disconnect!
  "Drop one connection: record why it is going away, unsubscribe it, and close
  its queue.

  Closing the queue is what ends the drain loop, so the thread inside
  [[connect!]] that is holding the connection open returns and runs its own
  cleanup -- which closes the SSE generator. Nothing here touches the
  generator directly, and a connection not held by `connect!` simply leaves
  the registry with its queue closed.

  `reason` defaults to `:disconnected` and reaches `:on-disconnect`; pass your
  own -- `:idle-timeout`, `:unauthorized` -- when you know it. A connection
  whose reason is already set keeps it, so dropping a client that has in fact
  already gone away still reports `:client-gone`."
  ([hub conn] (disconnect! hub conn :disconnected))
  ([hub conn reason]
   (conn/closing! conn reason)
   (-unsubscribe! hub (:topic conn) conn)
   (queue/close! (:queue conn))
   nil))

(defn shutdown!
  "Disconnect every connection on every topic, leaving the registry empty.

  Use this on server shutdown so held connections are released rather than
  left blocking their threads. Clients reconnect on their own via Datastar's
  SSE retry."
  [hub]
  (doseq [topic (topics hub)
          conn  (conns hub topic)]
    (disconnect! hub conn :shutdown))
  nil)

(defn send!
  "Enqueue one frame for one connection. Never writes and never blocks. If the
  connection's queue refuses the frame, the connection is closed."
  [hub conn frame]
  (let [accepted (boolean (queue/offer! (:queue conn) frame))]
    (when-not accepted
      (disconnect! hub conn :queue-full))
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
  - `:data`          app data attached to the connection, readable with
                     [[astrolabe.connection/data]]
  - `:on-connect`    `(fn [conn])` run once the connection is subscribed and
                     before the drain starts -- the place to send an initial
                     render. Subscribing first means a broadcast racing the
                     initial render is queued behind it rather than lost.
                     Unlike a dead client, an exception here is not swallowed:
                     the connection is torn down and the exception reaches the
                     adapter, and so `:on-exception`.
  - `:on-disconnect` `(fn [conn info])` run after the connection is
                     unsubscribed and its generator closed. `info` is
                     `{:reason ... :exception ...}` -- see
                     [[astrolabe.connection/disconnect-info]] -- where `:reason`
                     is one of `:client-gone` (the write that revealed it),
                     `:queue-full` (the client could not keep up),
                     `:shutdown`, `:disconnected` or whatever reason was passed
                     to [[disconnect!]], `:error` (`:on-connect` or the drain
                     threw, and `:exception` is the throwable), or `:unknown`
                     (the queue closed and nothing named a reason)
  - `:on-close`      SDK on-close callback, passed through to the response
  - `:on-exception`  SDK on-exception callback, passed through to the response

  `:on-connect`/`:on-disconnect` see the [[astrolabe.connection/Connection]];
  the SDK's `:on-close`/`:on-exception` see the raw sse-gen."
  ([hub connector topic] (connect! hub connector topic {}))
  ([hub connector topic {:keys [data on-connect on-disconnect on-close on-exception]}]
   (sse/response
    (cond-> {:on-open
             (fn [sse-gen]
               (let [c (conn/-open connector sse-gen topic data)]
                 (-subscribe! hub topic c)
                 (try
                   (when on-connect (on-connect c))
                   (try
                     (conn/-drain! connector c)
                     ;; The SDK reports a dead client as a false write verdict,
                     ;; never as a throw, so the drain thread has already named
                     ;; that -- an exception here is a bug, and swallowing it
                     ;; without recording it is what used to hide it.
                     (catch Exception e
                       (conn/closing! c :error e)))
                   (catch Exception e
                     (conn/closing! c :error e)
                     (throw e))
                   (finally
                     ;; A no-op unless every path above stayed silent, which
                     ;; only a custom drain or a queue closed behind the hub's
                     ;; back can manage. Settled here so -close! sees it too.
                     (conn/closing! c :unknown)
                     (-unsubscribe! hub topic c)
                     (conn/-close! connector c)
                     (when on-disconnect
                       (on-disconnect c (conn/disconnect-info c)))))))}
      on-close     (assoc :on-close on-close)
      on-exception (assoc :on-exception on-exception)))))
