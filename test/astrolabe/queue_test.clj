(ns astrolabe.queue-test
  (:require [clojure.test :refer [deftest is testing]]
            [astrolabe.queue :as queue])
  (:import [java.lang Thread$State]))

(defn- await-parked!
  "Spin until thread `t` has genuinely parked (WAITING or TIMED_WAITING),
  so a caller can be sure `t` is blocked before acting on shared state.
  Fails loudly (rather than hanging the suite) if `t` never parks within
  `timeout-ms`."
  [^Thread t timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)
        parked?  #{Thread$State/WAITING Thread$State/TIMED_WAITING}]
    (loop []
      (cond
        (parked? (.getState t)) true
        (> (System/currentTimeMillis) deadline)
        (throw (ex-info "thread never parked before timeout"
                         {:state (.getState t) :timeout-ms timeout-ms}))
        :else (do (Thread/sleep 1) (recur))))))

(deftest bounded-refuses-when-full
  (let [q (queue/bounded 2)]
    (is (true? (queue/offer! q :a)))
    (is (true? (queue/offer! q :b)))
    (is (false? (queue/offer! q :c)) "a full bounded queue refuses, asking for a disconnect")
    (is (= :a (queue/take! q)))
    (is (= :b (queue/take! q)))
    (is (true? (queue/offer! q :d)) "space frees up after a take")))

(deftest latest-coalesces
  (let [q (queue/latest)]
    (is (true? (queue/offer! q :a)))
    (is (true? (queue/offer! q :b)) "never refuses")
    (is (true? (queue/offer! q :c)))
    (is (= :c (queue/take! q)) "only the newest frame survives")))

(deftest unbounded-never-refuses
  (let [q (queue/unbounded)]
    (dotimes [i 1000] (is (true? (queue/offer! q i))))
    (is (= 0 (queue/take! q)))))

(deftest take!-blocks-until-a-frame-arrives
  (doseq [[label q] [["bounded" (queue/bounded 4)]
                     ["latest" (queue/latest)]
                     ["unbounded" (queue/unbounded)]]]
    (testing label
      (let [result (promise)
            t (Thread/startVirtualThread #(deliver result (queue/take! q)))]
        (await-parked! t 1000)
        (is (not (realized? result)) "take! has not returned yet")
        (queue/offer! q :frame)
        (is (= :frame (deref result 1000 ::timeout)))
        (.join t)))))

(deftest close!-wakes-a-blocked-take!-with-nil
  (doseq [[label q] [["bounded" (queue/bounded 4)]
                     ["latest" (queue/latest)]
                     ["unbounded" (queue/unbounded)]]]
    (testing label
      (let [result (promise)
            t (Thread/startVirtualThread #(deliver result (queue/take! q)))]
        (await-parked! t 1000)
        (queue/close! q)
        (is (nil? (deref result 1000 ::timeout)) "closed queues return nil, ending the drain loop")
        (.join t)))))

(deftest queue-fn-resolution
  (testing "keywords map to built-ins at their defaults"
    (is (satisfies? queue/Queue ((queue/->queue-fn :bounded) {})))
    (is (satisfies? queue/Queue ((queue/->queue-fn :latest) {})))
    (is (satisfies? queue/Queue ((queue/->queue-fn :unbounded) {}))))

  (testing "a function is used as-is and receives the connection"
    (let [seen (atom nil)
          f (queue/->queue-fn (fn [conn] (reset! seen conn) (queue/latest)))]
      (f {:topic :app})
      (is (= {:topic :app} @seen))))

  (testing "anything else throws"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Unknown :queue-fn"
                          (queue/->queue-fn :nonsense)))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #":queue-fn must be"
                          (queue/->queue-fn 42)))))
