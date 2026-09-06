(ns astrolabe.queue
  "Per-connection frame queues. Overflow policy is `offer!`'s return value:
  a queue that returns false is asking for its connection to be closed."
  (:import [java.util.concurrent LinkedBlockingQueue]
           [java.util.concurrent.locks ReentrantLock Condition]))

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
  (offer! [_ frame] (if closed? false (.offer q frame)))
  (take!  [_] (let [v (.take q)]
                (if (identical? v CLOSED)
                  (do (.offer q CLOSED) nil)   ; stay closed for any other taker
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

(deftype LatestFrameQueue [^ReentrantLock lock
                           ^Condition ready
                           ^:volatile-mutable slot
                           ^:volatile-mutable closed?]
  Queue
  (offer! [_ frame]
    (.lock lock)
    (try
      (when-not closed?
        (set! slot frame)
        (.signalAll ready))
      true
      (finally (.unlock lock))))

  (take! [_]
    (.lock lock)
    (try
      (loop []
        (cond
          (some? slot) (let [v slot] (set! slot nil) v)
          closed?      nil
          :else        (do (.await ready) (recur))))
      (finally (.unlock lock))))

  (close! [_]
    (.lock lock)
    (try
      (set! closed? true)
      (set! slot nil)
      (.signalAll ready)
      nil
      (finally (.unlock lock)))))

(defn latest
  "A depth-1 queue where a newer frame replaces the pending one. Never refuses.
  Use for state-based topics, where each frame is a complete snapshot and a
  superseded one is worthless."
  []
  (let [lock (ReentrantLock.)]
    (->LatestFrameQueue lock (.newCondition lock) nil false)))

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
