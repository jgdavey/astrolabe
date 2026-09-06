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

(defn- start-heartbeat!
  "Offer a keepalive frame whenever the connection has gone `ms` without a write.

  Runs on its own virtual thread and only ever touches the queue -- the drain
  thread stays the sole writer to the generator, so no locking is involved.
  Because it fires only when idle, the queue is empty whenever it offers: a
  keepalive can never evict a pending frame from a coalescing queue, nor add
  work to a connection that is already writing.

  The loop ends when the queue refuses the frame, which is what a closed queue
  does, so it also winds itself up if it is never interrupted."
  [q !last-write ^long ms]
  (Thread/startVirtualThread
   (fn []
     (try
       (loop []
         (Thread/sleep ms)
         (let [idle-ms (quot (- (System/nanoTime) ^long @!last-write) 1000000)]
           (if (< idle-ms ms)
             (recur)
             (when (queue/offer! q sse/heartbeat-frame)
               (recur)))))
       (catch InterruptedException _ nil)))))

(defrecord DefaultConnector [queue-fn drain heartbeat-ms]
  Connector
  (-open [_ sse-gen topic data]
    (->Connection sse-gen (queue-fn {:topic topic :data data}) topic data))

  (-drain! [_ conn]
    (let [{:keys [sse-gen queue]} conn
          !last-write (atom (System/nanoTime))
          write! (fn [frame]
                   (reset! !last-write (System/nanoTime))
                   (when-not (sse/apply! sse-gen frame)
                     ;; The client is gone. The SDK never throws for this -- the
                     ;; adapter catches the IOException, closes the generator,
                     ;; and every later write silently returns false -- so this
                     ;; verdict is the only signal we get. Closing the queue
                     ;; ends the drain, which lets the caller clean up.
                     (queue/close! queue)))
          ;; Started here rather than in -open so it brackets whatever `drain`
          ;; is configured: a custom drain loop cannot silently lose keepalives.
          hb (when heartbeat-ms (start-heartbeat! queue !last-write heartbeat-ms))]
      (try
        (drain conn queue write!)
        (finally
          (when hb (.interrupt ^Thread hb))))))

  (-close! [_ conn]
    (queue/close! (:queue conn))
    (d*/close-sse! (:sse-gen conn))))

(defn connector
  "Build the default connector.

  Opts:
  - `:queue-fn` a keyword (`:bounded` `:latest` `:unbounded`) or a function of
    `{:topic :data}` returning a queue; defaults to `:bounded`
  - `:drain` `(fn [conn queue write!])` that blocks until the queue closes;
    defaults to [[default-drain]]
  - `:heartbeat-ms` milliseconds of write inactivity after which a no-op
    keepalive frame is queued. Off by default -- when absent, no keepalive
    thread is started and a connection costs exactly what it always did. Turn
    it on for topics that can be idle for long stretches, where nothing else
    would ever attempt the write that reveals a client is gone."
  ([] (connector {}))
  ([{:keys [queue-fn drain heartbeat-ms]}]
   (->DefaultConnector (queue/->queue-fn (or queue-fn :bounded))
                       (or drain default-drain)
                       heartbeat-ms)))
