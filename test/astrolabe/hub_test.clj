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

(defn- drain-one
  "Take one frame from a connection's queue without blocking forever."
  [conn]
  (let [p (promise)]
    (Thread/startVirtualThread #(deliver p (queue/take! (:queue conn))))
    (deref p 1000 ::timeout)))

(deftest send!-enqueues-rather-than-writing
  (let [h (test-hub)
        c (test-conn :room)
        f (hub/frame h [[:remove-element "#x"]])]
    (hub/send! h c f)
    (is (empty? @(:!rec (:sse-gen c)))
        "send! must not touch the generator; the drain loop does the writing")
    (is (= f (drain-one c)) "the frame is waiting on the queue")))

(deftest broadcast!-renders-once-for-all-connections
  (let [renders (atom 0)
        itp (sse/interpreter {:render (fn [el] (swap! renders inc) (str el))})
        h (hub/in-memory {:interpreter itp})
        a (test-conn :room) b (test-conn :room) c (test-conn :room)]
    (doseq [conn [a b c]] (hub/subscribe! h :room conn))

    (hub/broadcast! h :room [[:patch-elements "<div/>"]])

    (is (= 1 @renders) "one render, no matter how many subscribers")
    (let [frames (map drain-one [a b c])]
      (is (apply = frames) "every connection got the identical frame"))))

(deftest broadcast-each!-renders-per-connection
  (let [renders (atom 0)
        itp (sse/interpreter {:render (fn [el] (swap! renders inc) (str el))})
        h (hub/in-memory {:interpreter itp})
        a (test-conn :room {:uid :a})
        b (test-conn :room {:uid :b})]
    (doseq [conn [a b]] (hub/subscribe! h :room conn))

    (hub/broadcast-each! h :room
                         (fn [conn] [[:patch-elements (str "<p>" (name (:uid (hub/meta conn))) "</p>")]]))

    (is (= 2 @renders) "one render per connection")
    (is (= [{:op :patch-elements :elements "<p>a</p>"}] (drain-one a)))
    (is (= [{:op :patch-elements :elements "<p>b</p>"}] (drain-one b)))))

(deftest a-refusing-queue-closes-its-connection
  (let [h (test-hub)
        full (hub/->Connection (adapter.test/->sse-recorder) (queue/bounded 1) :room {})
        ok   (test-conn :room)]
    (doseq [conn [full ok]] (hub/subscribe! h :room conn))

    (hub/broadcast! h :room [[:remove-element "#a"]])   ; fills `full`
    (hub/broadcast! h :room [[:remove-element "#b"]])   ; `full` refuses

    (is (= [ok] (hub/conns h :room))
        "the refusing connection is unsubscribed, the healthy one is untouched")))

(deftest broadcast!-is-not-stalled-by-a-slow-client
  (let [h (test-hub)
        slow (hub/->Connection (adapter.test/->sse-recorder) (queue/latest) :room {})
        fast (test-conn :room)]
    (doseq [conn [slow fast]] (hub/subscribe! h :room conn))
    ;; nobody is draining `slow`; broadcast must still return promptly
    (dotimes [_ 100] (hub/broadcast! h :room [[:remove-element "#x"]]))
    (is (= 2 (count (hub/conns h :room))) "a latest-queue never refuses, so nobody is dropped")))

(deftest connect!-holds-open-drains-and-cleans-up
  (let [h (test-hub)
        resp (hub/connect! h :room {:meta {:uid 7}})
        gen (adapter.test/->sse-recorder)
        done (promise)]
    (is (sse/response? resp))

    ;; connect! blocks, so run its on-open on its own virtual thread
    (let [t (Thread/startVirtualThread #(do ((:on-open resp) gen) (deliver done true)))]
      ;; wait for the subscription to appear
      (loop [n 0]
        (when (and (empty? (hub/conns h :room)) (< n 100))
          (Thread/sleep 10)
          (recur (inc n))))
      (let [conn (first (hub/conns h :room))]
        (is (some? conn) "connect! subscribed")
        (is (= {:uid 7} (hub/meta conn)) "connect! attached :meta")

        (hub/broadcast! h :room [[:remove-element "#x"]])
        (loop [n 0]
          (when (and (empty? @(:!rec gen)) (< n 100))
            (Thread/sleep 10)
            (recur (inc n))))
        (is (= 1 (count @(:!rec gen))) "the drain loop wrote the broadcast frame")

        (queue/close! (:queue conn)))

      (is (true? (deref done 1000 ::timeout)) "closing the queue ends the drain loop")
      (.join t)
      (is (= [] (hub/conns h :room)) "connect! unsubscribes on the way out"))))

(deftest a-custom-drain-loop-can-dedupe
  (let [h (hub/in-memory
           {:interpreter itp
            :queue-fn :latest
            :drain (fn [_conn q write!]
                     (loop [prev nil]
                       (when-let [f (queue/take! q)]
                         (when (not= f prev) (write! f))
                         (recur f))))})
        resp (hub/connect! h :room)
        gen (adapter.test/->sse-recorder)]
    (Thread/startVirtualThread #((:on-open resp) gen))
    (loop [n 0] (when (and (empty? (hub/conns h :room)) (< n 100)) (Thread/sleep 10) (recur (inc n))))
    (let [conn (first (hub/conns h :room))]
      (dotimes [_ 5]
        (hub/send! h conn (hub/frame h [[:remove-element "#same"]]))
        (Thread/sleep 20))
      (is (= 1 (count @(:!rec gen))) "identical frames are written once")
      (queue/close! (:queue conn)))))
