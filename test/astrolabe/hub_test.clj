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
   (conn/connection (adapter.test/->sse-recorder) (queue/unbounded) topic data)))

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
        full (conn/connection (adapter.test/->sse-recorder) (queue/bounded 1) :room nil)
        ok   (test-conn :room)]
    (doseq [c [full ok]] (hub/subscribe! h :room c))

    (hub/broadcast! h :room (frame [[:remove-element "#a"]]))   ; fills `full`
    (hub/broadcast! h :room (frame [[:remove-element "#b"]]))   ; `full` refuses

    (is (= [ok] (hub/conns h :room))
        "the refusing connection is unsubscribed, the healthy one is untouched")))

(deftest broadcast!-is-not-stalled-by-a-slow-client
  (let [h    (hub/in-memory)
        slow (conn/connection (adapter.test/->sse-recorder) (queue/latest) :room nil)
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
    (conn/connection sse-gen (queue/unbounded) topic data))
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

;; -----------------------------------------------------------------------------
;; Topics, disconnect! and shutdown!

(deftest topics-lists-only-topics-holding-connections
  (let [h (hub/in-memory)
        a (test-conn :one)
        b (test-conn :two)]
    (is (= #{} (set (hub/topics h))) "a fresh hub has no topics")

    (hub/subscribe! h :one a)
    (hub/subscribe! h :two b)
    (is (= #{:one :two} (set (hub/topics h))))

    (hub/unsubscribe! h :one a)
    (is (= #{:two} (set (hub/topics h)))
        "an emptied topic is gone, not left behind as an empty entry")))

(deftest disconnect!-unsubscribes-and-closes-the-queue
  (let [h (hub/in-memory)
        a (test-conn :room)
        b (test-conn :room)]
    (doseq [c [a b]] (hub/subscribe! h :room c))

    (hub/disconnect! h a)

    (is (= [b] (hub/conns h :room)) "only the named connection leaves the topic")
    (is (false? (queue/offer! (:queue a) :frame))
        "its queue is closed, which ends whatever drain loop is holding it")
    (is (true? (queue/offer! (:queue b) :frame)) "the other connection is untouched")))

(deftest shutdown!-closes-every-connection-on-every-topic
  (let [h (hub/in-memory)
        a (test-conn :one)
        b (test-conn :one)
        c (test-conn :two)]
    (hub/subscribe! h :one a)
    (hub/subscribe! h :one b)
    (hub/subscribe! h :two c)

    (hub/shutdown! h)

    (is (= #{} (set (hub/topics h))) "the registry is empty")
    (is (= [] (hub/conns h :one)))
    (is (= [] (hub/conns h :two)))
    (doseq [x [a b c]]
      (is (false? (queue/offer! (:queue x) :frame)) "every queue is closed"))))

(deftest shutdown!-ends-held-connections-and-closes-their-generators
  (let [h    (hub/in-memory)
        resp (hub/connect! h (conn/connector) :room)
        gen  (adapter.test/->sse-recorder)
        done (promise)]
    (Thread/startVirtualThread #(do ((:on-open resp) gen) (deliver done true)))
    (is (wait-for #(seq (hub/conns h :room))) "connect! subscribed")

    (hub/shutdown! h)

    (is (true? (deref done 2000 ::timeout))
        "shutdown! ends the drain, so the thread holding the connection open returns")
    (is (false? @(:!open? gen)) "the SSE generator is closed")
    (is (= [] (hub/conns h :room)))))

;; -----------------------------------------------------------------------------
;; connect! lifecycle hooks

(deftest hooks-bracket-the-drain-loop
  ;; The StubConnector already logs :open/:drain/:close, so the hooks can log
  ;; into the same atom and the whole ordering falls out as one value.
  (let [!calls (atom [])
        h      (hub/in-memory)
        resp   (hub/connect! h (->StubConnector !calls) :room
                             {:on-connect    (fn [_] (swap! !calls conj :connect))
                              :on-disconnect (fn [_ _] (swap! !calls conj :disconnect))})
        done   (promise)]
    (Thread/startVirtualThread #(do ((:on-open resp) ::gen) (deliver done true)))
    (is (wait-for #(seq (hub/conns h :room))))
    (queue/close! (:queue (first (hub/conns h :room))))

    (is (true? (deref done 2000 ::timeout)))
    (is (= [:open :connect :drain :close :disconnect] @!calls)
        ":on-connect runs before the drain, :on-disconnect after teardown")))

(deftest on-connect-sees-a-subscribed-connection
  (let [h    (hub/in-memory)
        seen (promise)
        resp (hub/connect! h (conn/connector) :room
                           {:data       {:uid 3}
                            :on-connect (fn [c] (deliver seen [(conn/data c)
                                                               (hub/conns h :room)]))})
        done (promise)]
    (Thread/startVirtualThread #(do ((:on-open resp) (adapter.test/->sse-recorder))
                                    (deliver done true)))
    (let [[data conns] (deref seen 2000 ::timeout)]
      (is (= {:uid 3} data) ":on-connect receives the Connection, not the generator")
      (is (= 1 (count conns))
          "the connection is already on the topic, so a broadcast racing the
           initial render cannot miss it"))
    (hub/shutdown! h)
    (is (true? (deref done 2000 ::timeout)))))

(deftest an-initial-render-sent-from-on-connect-reaches-the-client
  (let [h    (hub/in-memory)
        gen  (adapter.test/->sse-recorder)
        resp (hub/connect! h (conn/connector) :room
                           {:on-connect (fn [c]
                                          (hub/send! h c (frame [[:patch-elements "<p>hi</p>"]])))})
        done (promise)]
    (Thread/startVirtualThread #(do ((:on-open resp) gen) (deliver done true)))
    (is (wait-for #(seq @(:!rec gen)))
        "the frame enqueued from :on-connect is drained, not stranded")
    (hub/shutdown! h)
    (is (true? (deref done 2000 ::timeout)))))

(deftest on-disconnect-sees-a-fully-torn-down-connection
  (let [h    (hub/in-memory)
        gen  (adapter.test/->sse-recorder)
        seen (promise)
        resp (hub/connect! h (conn/connector) :room
                           {:on-disconnect (fn [c _]
                                             (deliver seen {:conns  (hub/conns h :room)
                                                            :open?  @(:!open? gen)
                                                            :topic  (:topic c)}))})]
    (Thread/startVirtualThread #((:on-open resp) gen))
    (is (wait-for #(seq (hub/conns h :room))))
    (hub/shutdown! h)

    (is (= {:conns [] :open? false :topic :room} (deref seen 2000 ::timeout))
        ":on-disconnect runs after unsubscribe and after the generator is closed")))

(deftest an-on-connect-that-throws-still-tears-the-connection-down
  (let [h      (hub/in-memory)
        gen    (adapter.test/->sse-recorder)
        boom   (ex-info "initial render failed" {})
        !threw (atom nil)
        resp   (hub/connect! h (conn/connector) :room
                             {:on-connect (fn [_] (throw boom))})
        done   (promise)]
    (Thread/startVirtualThread
     #(do (try ((:on-open resp) gen) (catch Exception e (reset! !threw e)))
          (deliver done true)))

    (is (true? (deref done 2000 ::timeout)))
    (is (identical? boom @!threw)
        "the exception surfaces to the adapter rather than being swallowed like
         a dead client")
    (is (= [] (hub/conns h :room)) "the connection is not left on the topic")
    (is (false? @(:!open? gen)) "the generator is closed")))

;; -----------------------------------------------------------------------------
;; Disconnect reason

(defn- info-seen-by-on-disconnect
  "Hold a connection open with `connect!` and return the info map its
  `:on-disconnect` received. `opts` is merged over the hook, and `f` is called
  with the live Connection once it is on the topic -- the chance to end it."
  ([h] (info-seen-by-on-disconnect h (conn/connector) (adapter.test/->sse-recorder) {} (fn [_])))
  ([h connector gen opts f]
   (let [seen (promise)
         resp (hub/connect! h connector :room
                            (merge {:on-disconnect (fn [_ info] (deliver seen info))} opts))]
     ;; on-connect exceptions propagate by design; the adapter, not this thread,
     ;; is what normally sees them.
     (Thread/startVirtualThread #(try ((:on-open resp) gen) (catch Exception _ nil)))
     (when (wait-for #(seq (hub/conns h :room)))
       (f (first (hub/conns h :room))))
     (deref seen 2000 ::timeout))))

(deftest disconnect!-records-why-the-connection-left
  (let [h (hub/in-memory)
        a (test-conn :room)]
    (hub/subscribe! h :room a)
    (hub/disconnect! h a)
    (is (= {:reason :disconnected :exception nil} (conn/disconnect-info a))
        "an app-initiated disconnect is not a client going away")))

(deftest disconnect!-takes-a-caller-supplied-reason
  (let [h (hub/in-memory)
        a (test-conn :room)]
    (hub/subscribe! h :room a)
    (hub/disconnect! h a :idle-timeout)
    (is (= {:reason :idle-timeout :exception nil} (conn/disconnect-info a))
        "an app that knows why it is dropping a client can say so")))

(deftest shutdown!-records-shutdown
  (let [h (hub/in-memory)
        a (test-conn :one)
        b (test-conn :two)]
    (hub/subscribe! h :one a)
    (hub/subscribe! h :two b)

    (hub/shutdown! h)

    (doseq [c [a b]]
      (is (= {:reason :shutdown :exception nil} (conn/disconnect-info c))
          "a server going down is distinguishable from a client going away"))))

(deftest a-refusing-queue-records-queue-full
  (let [h    (hub/in-memory)
        full (conn/connection (adapter.test/->sse-recorder) (queue/bounded 1) :room nil)]
    (hub/subscribe! h :room full)

    (hub/send! h full (frame [[:remove-element "#a"]]))   ; fills it
    (hub/send! h full (frame [[:remove-element "#b"]]))   ; refused

    (is (= {:reason :queue-full :exception nil} (conn/disconnect-info full))
        "a client too slow to keep up is a distinct failure from one that left")))

(deftest a-client-that-went-away-is-not-relabelled-by-the-teardown-that-follows
  ;; The drain notices the client first and closes the queue; whatever tears the
  ;; connection down next must not overwrite the cause with its own consequence.
  (let [h (hub/in-memory)
        a (test-conn :room)]
    (hub/subscribe! h :room a)
    (conn/closing! a :client-gone)

    (hub/disconnect! h a)

    (is (= {:reason :client-gone :exception nil} (conn/disconnect-info a)))))

(deftest on-disconnect-receives-the-reason
  (let [h (hub/in-memory)]
    (is (= {:reason :kicked :exception nil}
           (info-seen-by-on-disconnect h (conn/connector) (adapter.test/->sse-recorder) {}
                                       #(hub/disconnect! h % :kicked))))))

(deftest on-disconnect-reports-a-client-that-went-away
  (let [h (hub/in-memory)]
    (is (= {:reason :client-gone :exception nil}
           (info-seen-by-on-disconnect
            h (conn/connector) (->failing-gen 0)
            {:on-connect (fn [c] (hub/send! h c (frame [[:remove-element "#x"]])))}
            (fn [_])))
        "the write that fails is the one that names the reason")))

(deftest on-disconnect-reports-unknown-when-nothing-named-a-reason
  (let [h (hub/in-memory)]
    (is (= {:reason :unknown :exception nil}
           (info-seen-by-on-disconnect h (conn/connector) (adapter.test/->sse-recorder) {}
                                       #(queue/close! (:queue %))))
        "a queue closed behind the hub's back is honestly unknown, not a guess")))

(deftest on-disconnect-reports-an-on-connect-that-threw
  (let [h    (hub/in-memory)
        boom (ex-info "initial render failed" {})
        info (info-seen-by-on-disconnect h (conn/connector) (adapter.test/->sse-recorder)
                                         {:on-connect (fn [_] (throw boom))}
                                         (fn [_]))]
    (is (= :error (:reason info)))
    (is (identical? boom (:exception info))
        "the throwable reaches :on-disconnect even though the drain never ran")))

(deftest on-disconnect-reports-a-drain-that-threw
  (let [h    (hub/in-memory)
        boom (ex-info "drain blew up" {})
        info (info-seen-by-on-disconnect h
                                         (conn/connector {:drain (fn [_ _ _] (throw boom))})
                                         (adapter.test/->sse-recorder) {} (fn [_]))]
    (is (= :error (:reason info)))
    (is (identical? boom (:exception info))
        "a drain exception is swallowed, so :on-disconnect is the only place it
         ever surfaces")))
