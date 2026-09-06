(ns astrolabe.hub
  "A topic-keyed registry of open SSE connections."
  (:refer-clojure :exclude [meta])
  (:require [astrolabe.queue :as queue]
            [astrolabe.sse :as sse]))

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
