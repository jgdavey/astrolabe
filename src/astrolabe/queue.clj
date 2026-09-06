(ns astrolabe.queue
  "Per-connection frame queues. Overflow policy is `offer!`'s return value:
  a queue that returns false is asking for its connection to be closed."
  (:import [java.util.concurrent LinkedBlockingQueue ArrayBlockingQueue]))

(defprotocol Queue
  (offer! [q frame]
    "Accept a frame. Returns false if the connection should close.")
  (take! [q]
    "Block until a frame is available. Returns nil when the queue is closed.")
  (close! [q]
    "Close the queue, waking any blocked `take!` with nil."))

(def ^:private CLOSED ::closed)

(deftype BlockingFrameQueue [^LinkedBlockingQueue q ^:volatile-mutable closed?]
  Queue
  (offer! [_ frame]
    (if closed?
      false
      (.offer q frame)))
  (take! [_]
    (let [v (.take q)]
      (if (identical? v CLOSED)
        ;; Put the sentinel back on the queue in case of other readers
        (do (while (not (.offer q CLOSED)) (.poll q))
            nil)
        v)))
  (close! [_]
    (set! closed? true)
    (.clear q)
    (while (not (.offer q CLOSED)) (.poll q))
    nil))

(defn bounded
  "A queue holding at most `n` frames. Refuses further frames when full, which
  closes the connection so the client reconnects and resyncs. Use for
  event-based topics, where dropping a delta would diverge the client silently."
  [n]
  (->BlockingFrameQueue (LinkedBlockingQueue. (int n)) false))

(defn unbounded
  "A queue that never refuses and grows without limit. Useful in tests."
  []
  (->BlockingFrameQueue (LinkedBlockingQueue.) false))

(deftype SlidingFrameQueue [^ArrayBlockingQueue q ^:volatile-mutable closed?]
  Queue
  (offer! [_ frame]
    (if closed?
      false
      (do
        (while (not (.offer q frame))
          ;; drop head of queue until there's room
          (.poll q))
        true)))
  (take! [_]
    (let [v (.take q)]
      (if (identical? v CLOSED)
        ;; Put the sentinel back on the queue in case of other readers
        (do (while (not (.offer q CLOSED)) (.poll q))
            nil)
        v)))
  (close! [_]
    (set! closed? true)
    (.clear q)
    (while (not (.offer q CLOSED)) (.poll q))
    nil))

(defn sliding [n]
  (->SlidingFrameQueue (ArrayBlockingQueue. n) false))

(defn latest []
  (sliding 1))

(def ^:private built-ins
  {:bounded   #(bounded 64)
   :latest    latest
   :unbounded unbounded})

(defn ->queue-fn
  "Resolve a `:queue-fn` config value into a 1-arg function of a connection."
  [x]
  (cond
    (keyword? x) (if-let [ctor (get built-ins x)]
                   (fn [_conn] (ctor))
                   (throw (ex-info "Unknown :queue-fn keyword"
                                   {:queue-fn x :known (set (keys built-ins))})))
    (ifn? x) x
    :else (throw (ex-info ":queue-fn must be a keyword or a function of one connection"
                          {:queue-fn x}))))
