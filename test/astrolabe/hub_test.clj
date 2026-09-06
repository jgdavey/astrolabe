(ns astrolabe.hub-test
  (:require [clojure.test :refer [deftest is testing]]
            [astrolabe.hub :as hub]
            [astrolabe.queue :as queue]
            [astrolabe.sse :as sse]
            [starfederation.datastar.clojure.adapter.test :as adapter.test]))

(def itp (sse/interpreter {:render str :write-json pr-str}))

(defn test-hub [] (hub/in-memory {:interpreter itp}))

(defn test-conn
  ([topic] (test-conn topic {}))
  ([topic meta]
   (hub/->Connection (adapter.test/->sse-recorder) (queue/unbounded) topic meta)))

(deftest subscribe-and-unsubscribe
  (let [h (test-hub)
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
  (let [h (test-hub)
        a (test-conn :one)
        b (test-conn :two)]
    (hub/subscribe! h :one a)
    (hub/subscribe! h :two b)
    (is (= [a] (hub/conns h :one)))
    (is (= [b] (hub/conns h :two)))))

(deftest unsubscribe-is-idempotent
  (let [h (test-hub)
        a (test-conn :room)]
    (hub/subscribe! h :room a)
    (hub/unsubscribe! h :room a)
    (is (nil? (hub/unsubscribe! h :room a)) "unsubscribing twice is not an error")
    (is (= [] (hub/conns h :room)))))

(deftest meta-exposes-app-data
  (let [c (test-conn :room {:uid 42})]
    (is (= {:uid 42} (hub/meta c)))))

(deftest hub-defaults
  (testing "queue-fn defaults to :bounded"
    (let [h (test-hub)]
      (is (satisfies? queue/Queue ((:queue-fn h) (test-conn :room))))))
  (testing "an interpreter is required"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #":interpreter is required"
                          (hub/in-memory {})))))
