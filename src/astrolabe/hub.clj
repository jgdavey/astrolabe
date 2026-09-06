(ns astrolabe.hub
  "A topic-keyed registry of open SSE connections."
  (:refer-clojure :exclude [meta])
  (:require [astrolabe.queue :as queue]
            [astrolabe.sse :as sse]
            [starfederation.datastar.clojure.api :as d*]))

(defrecord Connection [sse-gen queue topic meta])

(defn meta
  "The app-supplied data attached to a connection at `connect!` time."
  [conn]
  (:meta conn))

(defprotocol Hub
  (-subscribe!   [hub topic conn])
  (-unsubscribe! [hub topic conn])
  (-conns        [hub topic]))

(defrecord InMemoryHub [!topics interpreter queue-fn drain]
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

(defn subscribe!   [hub topic conn] (-subscribe! hub topic conn))
(defn unsubscribe! [hub topic conn] (-unsubscribe! hub topic conn))
(defn conns        [hub topic]      (-conns hub topic))

(declare default-drain)

(defn in-memory
  "A single-node hub. Connections live in this process and are lost on restart;
  browsers reconnect via Datastar's SSE retry."
  [{:keys [interpreter queue-fn drain]}]
  (when (nil? interpreter)
    (throw (ex-info ":interpreter is required" {})))
  (->InMemoryHub (atom {})
                 interpreter
                 (queue/->queue-fn (or queue-fn :bounded))
                 (or drain default-drain)))

(defn default-drain
  "Take frames until the queue closes, writing each one."
  [_conn q write!]
  (loop []
    (when-let [frame (queue/take! q)]
      (write! frame)
      (recur))))

(defn frame
  "Normalize and render `events` once, producing a connection-independent frame."
  [hub events]
  (sse/frame (:interpreter hub) events))

(defn- close-conn!
  "Unsubscribe a connection and close its queue, ending its drain loop."
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
  "Render `events` once and send the resulting frame to every connection on
  `topic`. Use when every client sees the same HTML."
  [hub topic events]
  (let [f (frame hub events)]
    (doseq [conn (conns hub topic)]
      (send! hub conn f)))
  nil)

(defn broadcast-each!
  "Call `f` with each connection on `topic` and send that connection its own
  frame. Use when clients see different HTML."
  [hub topic f]
  (doseq [conn (conns hub topic)]
    (send! hub conn (frame hub (f conn))))
  nil)

(defn connect!
  "Return SSE response data that subscribes to `topic`, holds the connection
  open by draining its queue, and unsubscribes when the client disconnects.

  Disconnect detection lives here. The SDK never throws when a client goes
  away -- the adapter catches the IOException, closes the generator, and every
  later write silently returns false -- so `connect!`'s `write!` closes the
  queue as soon as `sse/apply!` reports a closed connection. That ends the
  drain loop, and the cleanup below unsubscribes and closes the generator.
  Cleanup is owned by `connect!`, not by the drain, so a custom `:drain`
  inherits all of it.

  Opts:
  - `:meta`         app data attached to the connection, readable with [[meta]]
  - `:on-close`     SDK on-close callback, passed through to the response
  - `:on-exception` SDK on-exception callback, passed through to the response"
  ([hub topic] (connect! hub topic {}))
  ([hub topic {m :meta :keys [on-close on-exception]}]
   (sse/response
    (cond-> {:on-open
             (fn [sse-gen]
               ;; the queue-fn sees the connection, so build it in two steps
               (let [proto (->Connection sse-gen nil topic m)
                     q     ((:queue-fn hub) proto)
                     conn  (assoc proto :queue q)
                     write! (fn [frame]
                              (when-not (sse/apply! (:interpreter hub) sse-gen frame)
                                ;; the client is gone; end the drain loop
                                (queue/close! q)))]
                 (-subscribe! hub topic conn)
                 (try
                   ((:drain hub) conn q write!)
                   (catch Exception _
                     nil)   ; a dead client is normal; fall through to cleanup
                   (finally
                     (-unsubscribe! hub topic conn)
                     (queue/close! q)
                     (d*/close-sse! sse-gen)))))}
      on-close     (assoc :on-close on-close)
      on-exception (assoc :on-exception on-exception)))))
