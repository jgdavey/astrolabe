(ns astrolabe.connection-test
  (:require [clojure.test :refer [deftest is testing]]
            [astrolabe.connection :as conn]
            [astrolabe.queue :as queue]
            [astrolabe.sse :as sse]
            [starfederation.datastar.clojure.adapter.test :as adapter.test]
            [starfederation.datastar.clojure.protocols :as p])
  (:import [java.util.concurrent.locks ReentrantLock]))

(def itp (sse/interpreter {:render str}))

(defn- a-frame [] (sse/frame itp [[:remove-element "#x"]]))

(defrecord FailingGen [lock !writes !closed? max-writes]
  p/SSEGenerator
  (send-event! [_ _ _ _] (<= (swap! !writes inc) max-writes))
  (get-lock [_] lock)
  (close-sse! [_] (reset! !closed? true))
  (sse-gen? [_] true))

(defn- ->failing-gen
  "An SSEGenerator reporting a closed connection -- `false` out of `send-event!`,
  with no exception -- after `max-writes` successful writes. That is what the
  SDK does when a client goes away."
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

(deftest open-builds-a-connection-with-its-queue
  (let [seen (atom nil)
        c    (conn/connector {:queue-fn (fn [x] (reset! seen x) (queue/unbounded))})
        gen  (adapter.test/->sse-recorder)
        conn (conn/-open c gen :room {:uid 7})]
    (is (identical? gen (:sse-gen conn)))
    (is (= :room (:topic conn)))
    (is (satisfies? queue/Queue (:queue conn)) "the connection arrives with its queue")
    (testing "queue-fn sees the topic and data, not a half-built connection"
      (is (= {:topic :room :data {:uid 7}} @seen)))))

(deftest data-exposes-the-app-supplied-payload
  (let [conn (conn/-open (conn/connector) (adapter.test/->sse-recorder) :room {:uid 42})]
    (is (= {:uid 42} (conn/data conn))))
  (testing "absent data is nil, not an error"
    (is (nil? (conn/data (conn/-open (conn/connector)
                                     (adapter.test/->sse-recorder) :room nil))))))

(deftest connector-defaults-to-a-bounded-queue-of-64
  (let [conn (conn/-open (conn/connector) (adapter.test/->sse-recorder) :room nil)
        q    (:queue conn)]
    (dotimes [_ 64] (queue/offer! q :frame))
    (is (false? (queue/offer! q :frame))
        "the default queue is bounded at 64, so it refuses the 65th frame")))

(deftest drain!-writes-queued-frames-until-the-queue-closes
  (let [c    (conn/connector {:queue-fn (fn [_] (queue/unbounded))})
        gen  (adapter.test/->sse-recorder)
        conn (conn/-open c gen :room nil)
        done (promise)]
    (Thread/startVirtualThread #(do (conn/-drain! c conn) (deliver done true)))

    (queue/offer! (:queue conn) (a-frame))
    (is (wait-for #(seq @(:!rec gen))) "the drain loop wrote the queued frame")

    (queue/close! (:queue conn))
    (is (true? (deref done 2000 ::timeout)) "closing the queue ends the drain")))

(deftest drain!-ends-when-a-write-reports-the-client-gone
  (let [c    (conn/connector {:queue-fn (fn [_] (queue/unbounded))})
        gen  (->failing-gen 0)      ; every write reports a closed connection
        conn (conn/-open c gen :room nil)
        done (promise)]
    (Thread/startVirtualThread #(do (conn/-drain! c conn) (deliver done true)))

    (queue/offer! (:queue conn) (a-frame))

    (is (true? (deref done 2000 ::timeout))
        "a false write verdict closes the queue, which ends the drain loop")
    (is (pos? @(:!writes gen)) "the write was actually attempted")))

(deftest drain!-uses-the-configured-drain
  (let [calls (atom 0)
        c     (conn/connector
               {:queue-fn (fn [_] (queue/unbounded))
                :drain    (fn [_conn q write!]
                            (swap! calls inc)
                            (loop [] (when-let [f (queue/take! q)] (write! f) (recur))))})
        gen   (adapter.test/->sse-recorder)
        conn  (conn/-open c gen :room nil)
        done  (promise)]
    (Thread/startVirtualThread #(do (conn/-drain! c conn) (deliver done true)))
    (queue/offer! (:queue conn) (a-frame))
    (is (wait-for #(seq @(:!rec gen))))
    (queue/close! (:queue conn))
    (is (true? (deref done 2000 ::timeout)))
    (is (= 1 @calls) "the supplied drain ran, not the default")))

(deftest close!-closes-the-queue-then-the-generator
  (let [c    (conn/connector)
        gen  (->failing-gen 5)
        conn (conn/-open c gen :room nil)]
    (conn/-close! c conn)
    (is (false? (queue/offer! (:queue conn) :frame)) "the queue is closed")
    (is (true? @(:!closed? gen)) "the SSE generator is closed")))
