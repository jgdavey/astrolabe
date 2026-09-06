(ns astrolabe.hub-test
  (:require [clojure.test :refer [deftest is testing]]
            [astrolabe.connection :as conn]
            [astrolabe.hub :as hub]
            [astrolabe.queue :as queue]
            [astrolabe.sse :as sse]
            [starfederation.datastar.clojure.adapter.test :as adapter.test]
            [starfederation.datastar.clojure.protocols :as p])
  (:import [java.util.concurrent.locks ReentrantLock]))

(def itp (sse/interpreter {:render str :write-json pr-str}))

(defn- frame [events] (sse/frame itp events))

(defn- test-conn
  ([topic] (test-conn topic nil))
  ([topic data]
   (conn/->Connection (adapter.test/->sse-recorder) (queue/unbounded) topic data)))

(defrecord FailingGen [lock !writes !closed? max-writes]
  p/SSEGenerator
  (send-event! [_ _ _ _] (<= (swap! !writes inc) max-writes))
  (get-lock [_] lock)
  (close-sse! [_] (reset! !closed? true))
  (sse-gen? [_] true))

(defn- ->failing-gen
  "An SSEGenerator that reports a closed connection -- `false` out of
  `send-event!`, with no exception -- after `max-writes` successful writes.

  That is what the SDK actually does when a client goes away: the adapter
  catches the IOException internally, `on-exception` closes the generator, and
  every later write silently returns false. `adapter.test/->sse-recorder` can
  never fail, so a connection leak on this path is invisible to it."
  [max-writes]
  (->FailingGen (ReentrantLock.) (atom 0) (atom false) max-writes))

(defn- wait-for
  "Poll `pred` for up to ~2s, returning its last value."
  [pred]
  (loop [n 0]
    (let [v (pred)]
      (if (or v (>= n 200))
        v
        (do (Thread/sleep 10) (recur (inc n)))))))

(defn- drain-one
  "Take one frame from a connection's queue without blocking forever."
  [c]
  (let [p (promise)]
    (Thread/startVirtualThread #(deliver p (queue/take! (:queue c))))
    (deref p 1000 ::timeout)))

;; -----------------------------------------------------------------------------
;; Registry

(deftest subscribe-and-unsubscribe
  (let [h (hub/in-memory)
        a (test-conn :room)
        b (test-conn :room)]
    (is (= [] (hub/conns h :room)) "unknown topics have no connections")

    (hub/subscribe! h :room a)
    (hub/subscribe! h :room b)
    (is (= #{a b} (set (hub/conns h :room))))

    (hub/unsubscribe! h :room a)
    (is (= [b] (hub/conns h :room)))

    (hub/unsubscribe! h :room b)
    (is (= [] (hub/conns h :room)) "the topic is empty once its last conn leaves")))

(deftest topics-are-isolated
  (let [h (hub/in-memory)
        a (test-conn :one)
        b (test-conn :two)]
    (hub/subscribe! h :one a)
    (hub/subscribe! h :two b)
    (is (= [a] (hub/conns h :one)))
    (is (= [b] (hub/conns h :two)))))

(deftest unsubscribe-is-idempotent
  (let [h (hub/in-memory)
        a (test-conn :room)]
    (hub/subscribe! h :room a)
    (hub/unsubscribe! h :room a)
    (is (nil? (hub/unsubscribe! h :room a)) "unsubscribing twice is not an error")
    (is (= [] (hub/conns h :room)))))

;; -----------------------------------------------------------------------------
;; Delivery

(deftest send!-enqueues-rather-than-writing
  (let [h (hub/in-memory)
        c (test-conn :room)
        f (frame [[:remove-element "#x"]])]
    (hub/send! h c f)
    (is (empty? @(:!rec (:sse-gen c)))
        "send! must not touch the generator; the drain loop does the writing")
    (is (= f (drain-one c)) "the frame is waiting on the queue")))

(deftest broadcast!-sends-one-frame-to-every-connection
  (let [h (hub/in-memory)
        a (test-conn :room) b (test-conn :room) c (test-conn :room)
        f (frame [[:patch-elements "<div/>"]])]
    (doseq [x [a b c]] (hub/subscribe! h :room x))

    (hub/broadcast! h :room f)

    ;; broadcast! takes an already-rendered frame, so "renders once" is now a
    ;; property of the signature rather than something to assert. What is worth
    ;; asserting is that every connection got that same frame.
    (is (= [f f f] (mapv drain-one [a b c])))))

(deftest broadcast-each!-sends-a-per-connection-frame
  (let [h (hub/in-memory)
        a (test-conn :room {:uid :a})
        b (test-conn :room {:uid :b})]
    (doseq [x [a b]] (hub/subscribe! h :room x))

    (hub/broadcast-each! h :room
                         (fn [c]
                           (frame [[:patch-elements
                                    (str "<p>" (name (:uid (conn/data c))) "</p>")]])))

    (is (= [{:op :patch-elements :elements "<p>a</p>"}] (drain-one a)))
    (is (= [{:op :patch-elements :elements "<p>b</p>"}] (drain-one b)))))

(deftest a-refusing-queue-closes-its-connection
  (let [h    (hub/in-memory)
        full (conn/->Connection (adapter.test/->sse-recorder) (queue/bounded 1) :room nil)
        ok   (test-conn :room)]
    (doseq [c [full ok]] (hub/subscribe! h :room c))

    (hub/broadcast! h :room (frame [[:remove-element "#a"]]))   ; fills `full`
    (hub/broadcast! h :room (frame [[:remove-element "#b"]]))   ; `full` refuses

    (is (= [ok] (hub/conns h :room))
        "the refusing connection is unsubscribed, the healthy one is untouched")))

(deftest broadcast!-is-not-stalled-by-a-slow-client
  (let [h    (hub/in-memory)
        slow (conn/->Connection (adapter.test/->sse-recorder) (queue/latest) :room nil)
        fast (test-conn :room)
        f    (frame [[:remove-element "#x"]])]
    (doseq [c [slow fast]] (hub/subscribe! h :room c))
    ;; nobody is draining `slow`; broadcast must still return promptly. Run the
    ;; loop under a deadline so a stalling broadcast! fails instead of hanging
    ;; the whole suite.
    (let [done (promise)]
      (Thread/startVirtualThread
       #(do (dotimes [_ 100] (hub/broadcast! h :room f))
            (deliver done true)))
      (is (true? (deref done 5000 ::timeout))
          "100 broadcasts to an undrained connection finish well inside 5s"))
    (is (= 2 (count (hub/conns h :room))) "a latest-queue never refuses, so nobody is dropped")))

;; -----------------------------------------------------------------------------
;; connect!

(deftest connect!-holds-open-drains-and-cleans-up
  (let [h    (hub/in-memory)
        resp (hub/connect! h (conn/connector) :room {:data {:uid 7}})
        gen  (adapter.test/->sse-recorder)
        done (promise)]
    (is (sse/response? resp))

    ;; connect! blocks, so run its on-open on its own virtual thread
    (let [t (Thread/startVirtualThread #(do ((:on-open resp) gen) (deliver done true)))]
      (is (wait-for #(seq (hub/conns h :room))) "connect! subscribed")
      (let [c (first (hub/conns h :room))]
        (is (= {:uid 7} (conn/data c)) "connect! attached :data")

        (hub/broadcast! h :room (frame [[:remove-element "#x"]]))
        (is (wait-for #(seq @(:!rec gen))) "the drain loop wrote the broadcast frame")
        (is (= 1 (count @(:!rec gen))))

        (queue/close! (:queue c)))

      (is (true? (deref done 1000 ::timeout)) "closing the queue ends the drain loop")
      (.join t)
      (is (= [] (hub/conns h :room)) "connect! unsubscribes on the way out")
      (is (false? @(:!open? gen)) "connect! closes the SSE generator on the way out"))))

(defrecord StubConnector [!calls]
  conn/Connector
  (-open [_ sse-gen topic data]
    (swap! !calls conj :open)
    (conn/->Connection sse-gen (queue/unbounded) topic data))
  (-drain! [_ c]
    (swap! !calls conj :drain)
    (loop [] (when (queue/take! (:queue c)) (recur))))
  (-close! [_ c]
    (swap! !calls conj :close)
    (queue/close! (:queue c))))

(deftest connect!-depends-only-on-the-Connector-protocol
  ;; StubConnector is not a DefaultConnector and never touches the sse-gen. If
  ;; connect! reached past the protocol -- for a queue-fn, a drain, an
  ;; interpreter -- this test would fail rather than silently pass the way it
  ;; would against the default connector.
  (let [!calls (atom [])
        h      (hub/in-memory)
        resp   (hub/connect! h (->StubConnector !calls) :room {:data {:uid 1}})
        done   (promise)]
    (Thread/startVirtualThread #(do ((:on-open resp) ::not-a-generator)
                                    (deliver done true)))
    (is (wait-for #(seq (hub/conns h :room))) "the stub's connection was subscribed")
    (let [c (first (hub/conns h :room))]
      (is (= {:uid 1} (conn/data c)))
      (is (= ::not-a-generator (:sse-gen c))
          "connect! passes the generator through without touching it")
      (queue/close! (:queue c)))

    (is (true? (deref done 2000 ::timeout)))
    (is (= [:open :drain :close] @!calls) "spawn, drain, then tear down")
    (is (= [] (hub/conns h :room)) "connect! unsubscribed on the way out")))

(deftest a-custom-drain-loop-can-dedupe
  (let [h    (hub/in-memory)
        c    (conn/connector
              {:queue-fn :latest
               :drain (fn [_conn q write!]
                        (loop [prev nil]
                          (when-let [f (queue/take! q)]
                            (when (not= f prev) (write! f))
                            (recur f))))})
        resp (hub/connect! h c :room)
        gen  (adapter.test/->sse-recorder)]
    (Thread/startVirtualThread #((:on-open resp) gen))
    (is (wait-for #(seq (hub/conns h :room))))
    (let [cn (first (hub/conns h :room))]
      (dotimes [_ 5]
        (hub/send! h cn (frame [[:remove-element "#same"]]))
        (Thread/sleep 20))
      (is (= 1 (count @(:!rec gen))) "identical frames are written once")
      (queue/close! (:queue cn)))))

(deftest a-closed-connection-is-unsubscribed-by-its-drain-loop
  (let [h    (hub/in-memory)
        resp (hub/connect! h (conn/connector) :room)
        gen  (->failing-gen 0)   ; every write reports a closed connection
        done (promise)]
    (Thread/startVirtualThread #(do ((:on-open resp) gen) (deliver done true)))
    (is (wait-for #(seq (hub/conns h :room))) "connect! subscribed")

    (hub/broadcast! h :room (frame [[:remove-element "#x"]]))

    (is (wait-for #(pos? @(:!writes gen))) "the drain loop attempted the write")
    (is (true? (deref done 2000 ::timeout))
        "a write reporting a closed connection ends the drain loop")
    (is (= [] (hub/conns h :room))
        "the dead connection is unsubscribed, not left on the topic forever")
    (is (true? @(:!closed? gen))
        "connect! closes the SSE generator on the way out")))

(deftest every-event-in-a-frame-is-attempted-even-after-a-failure
  (let [h    (hub/in-memory)
        resp (hub/connect! h (conn/connector) :room)
        gen  (->failing-gen 0)
        done (promise)]
    (Thread/startVirtualThread #(do ((:on-open resp) gen) (deliver done true)))
    (is (wait-for #(seq (hub/conns h :room))))

    (hub/broadcast! h :room (frame [[:remove-element "#a"]
                                    [:remove-element "#b"]
                                    [:remove-element "#c"]]))

    (is (true? (deref done 2000 ::timeout)) "the drain loop ended")
    (is (= 3 @(:!writes gen))
        "a failing write does not skip the rest of the frame")))
