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

;; -----------------------------------------------------------------------------
;; Heartbeat

(defn- heartbeat? [s] (re-find #"datastar-patch-signals" s))

(defn- drain-on-thread
  "Run `-drain!` on its own virtual thread, returning a promise of its return."
  [c conn]
  (let [done (promise)]
    (Thread/startVirtualThread #(do (conn/-drain! c conn) (deliver done true)))
    done))

(deftest heartbeat-is-off-unless-asked-for
  (let [c    (conn/connector {:queue-fn (fn [_] (queue/unbounded))})
        gen  (adapter.test/->sse-recorder)
        conn (conn/-open c gen :room nil)
        done (drain-on-thread c conn)]
    (Thread/sleep 300)
    (is (= [] @(:!rec gen))
        "no :heartbeat-ms means no keepalive thread and no traffic at all")
    (queue/close! (:queue conn))
    (is (true? (deref done 2000 ::timeout)))))

(deftest an-idle-connection-is-heartbeaten
  (let [c    (conn/connector {:queue-fn    (fn [_] (queue/unbounded))
                              :heartbeat-ms 50})
        gen  (adapter.test/->sse-recorder)
        conn (conn/-open c gen :room nil)
        done (drain-on-thread c conn)]
    (is (wait-for #(seq @(:!rec gen))) "an idle connection gets a keepalive write")
    (is (heartbeat? (first @(:!rec gen)))
        "the keepalive is an empty signals patch -- a no-op on the client")
    (queue/close! (:queue conn))
    (is (true? (deref done 2000 ::timeout)))))

(deftest a-busy-connection-is-not-heartbeaten
  (let [c    (conn/connector {:queue-fn    (fn [_] (queue/unbounded))
                              :heartbeat-ms 100})
        gen  (adapter.test/->sse-recorder)
        conn (conn/-open c gen :room nil)
        done (drain-on-thread c conn)]
    ;; keep real frames flowing for well over the heartbeat interval
    (dotimes [_ 30]
      (queue/offer! (:queue conn) (a-frame))
      (Thread/sleep 10))
    (Thread/sleep 20)
    (is (seq @(:!rec gen)) "the real frames were written")
    (is (not-any? heartbeat? @(:!rec gen))
        "a connection that is already writing needs no keepalive")
    (queue/close! (:queue conn))
    (is (true? (deref done 2000 ::timeout)))))

(deftest a-heartbeat-detects-a-client-that-went-away-silently
  ;; The whole point: with no application traffic, nothing else would ever
  ;; attempt a write, so a half-open connection would sit in the registry
  ;; forever. The keepalive is what turns it into a false write verdict.
  (let [c    (conn/connector {:queue-fn    (fn [_] (queue/unbounded))
                              :heartbeat-ms 50})
        gen  (->failing-gen 0)
        conn (conn/-open c gen :room nil)
        done (drain-on-thread c conn)]
    (is (true? (deref done 3000 ::timeout))
        "the keepalive write reports the connection closed, which ends the drain")
    (is (pos? @(:!writes gen)) "a write was actually attempted")))

(deftest the-heartbeat-outlives-a-custom-drain-loop
  ;; The keepalive is started by -drain! around whatever drain is configured,
  ;; so replacing the drain does not silently disable it.
  (let [c    (conn/connector {:queue-fn    (fn [_] (queue/unbounded))
                              :heartbeat-ms 50
                              :drain       (fn [_conn q write!]
                                             (loop []
                                               (when-let [f (queue/take! q)]
                                                 (write! f)
                                                 (recur))))})
        gen  (adapter.test/->sse-recorder)
        conn (conn/-open c gen :room nil)
        done (drain-on-thread c conn)]
    (is (wait-for #(some heartbeat? @(:!rec gen)))
        "a custom drain still gets keepalives")
    (queue/close! (:queue conn))
    (is (true? (deref done 2000 ::timeout)))))

(deftest the-heartbeat-stops-when-the-drain-ends
  (let [c    (conn/connector {:queue-fn    (fn [_] (queue/unbounded))
                              :heartbeat-ms 50})
        gen  (adapter.test/->sse-recorder)
        conn (conn/-open c gen :room nil)
        done (drain-on-thread c conn)]
    (is (wait-for #(seq @(:!rec gen))))
    (queue/close! (:queue conn))
    (is (true? (deref done 2000 ::timeout)))
    (let [n @(:!rec gen)]
      (Thread/sleep 200)
      (is (= (count n) (count @(:!rec gen)))
          "no keepalive thread is left running after the connection ends"))))

;; -----------------------------------------------------------------------------
;; Disconnect reason

(deftest a-live-connection-has-no-disconnect-info
  (let [conn (conn/-open (conn/connector) (adapter.test/->sse-recorder) :room nil)]
    (is (nil? (conn/disconnect-info conn))
        "nil is what separates a live connection from one that is going away")))

(deftest connection-builds-a-connection-that-can-report-why-it-closed
  ;; The factory exists so a custom Connector cannot forget the closing cell.
  (let [conn (conn/connection (adapter.test/->sse-recorder) (queue/unbounded) :room {:uid 1})]
    (is (= :room (:topic conn)))
    (is (= {:uid 1} (conn/data conn)))
    (conn/closing! conn :disconnected)
    (is (= {:reason :disconnected :exception nil} (conn/disconnect-info conn)))))

(deftest closing!-keeps-the-first-reason-it-is-given
  (let [conn (conn/-open (conn/connector) (adapter.test/->sse-recorder) :room nil)]
    (conn/closing! conn :client-gone)
    (conn/closing! conn :shutdown)
    (is (= {:reason :client-gone :exception nil} (conn/disconnect-info conn))
        "the cause wins over the consequence: a teardown racing another cannot
         overwrite why the connection is actually going away")))

(deftest closing!-carries-an-exception-when-given-one
  (let [conn (conn/-open (conn/connector) (adapter.test/->sse-recorder) :room nil)
        boom (ex-info "boom" {})]
    (conn/closing! conn :error boom)
    (is (= {:reason :error :exception boom} (conn/disconnect-info conn)))))

(deftest a-failed-write-records-client-gone
  (let [c    (conn/connector {:queue-fn (fn [_] (queue/unbounded))})
        gen  (->failing-gen 0)
        conn (conn/-open c gen :room nil)
        done (drain-on-thread c conn)]
    (queue/offer! (:queue conn) (a-frame))
    (is (true? (deref done 2000 ::timeout)) "the failed write ends the drain")
    (is (= {:reason :client-gone :exception nil} (conn/disconnect-info conn))
        "the drain names the client going away, the normal end of a connection")))
