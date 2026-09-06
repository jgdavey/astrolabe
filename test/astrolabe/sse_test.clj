(ns astrolabe.sse-test
  (:require [clojure.test :refer [deftest is testing]]
            [astrolabe.sse :as sse]
            [starfederation.datastar.clojure.adapter.test :as adapter.test]
            [starfederation.datastar.clojure.protocols :as p]))

(defn- fake-render
  "Stand-in for hiccup/chassis: [:div \"hi\"] -> \"<div>hi</div>\"."
  [el]
  (if (string? el)
    el
    (let [[tag & children] el]
      (str "<" (name tag) ">" (apply str children) "</" (name tag) ">"))))

(def itp (sse/interpreter {:render fake-render :write-json pr-str}))

(deftest interpreter-requires-render
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #":render is required"
                        (sse/interpreter {}))))

(deftest frame-renders-elements-once
  (testing "hiccup becomes an HTML string, opts survive"
    (is (= [{:op :patch-elements :elements "<div>hi</div>" :mode :append}]
           (sse/frame itp [[:patch-elements [:div "hi"] {:mode :append}]]))))

  (testing "patch-elements-seq renders each element"
    (is (= [{:op :patch-elements-seq :elements ["<li>a</li>" "<li>b</li>"]}]
           (sse/frame itp [[:patch-elements-seq [[:li "a"] [:li "b"]]]]))))

  (testing "ops without elements are untouched"
    (is (= [{:op :remove-element :selector "#gone"}]
           (sse/frame itp [[:remove-element "#gone"]])))))

(deftest frame-serializes-signal-maps
  (testing "a map goes through :write-json"
    (is (= [{:op :patch-signals :signals (pr-str {:name ""})}]
           (sse/frame itp [[:patch-signals {:name ""}]]))))

  (testing "a string passes through untouched"
    (is (= [{:op :patch-signals :signals "{\"name\":\"\"}"}]
           (sse/frame itp [[:patch-signals "{\"name\":\"\"}"]]))))

  (testing "a map with no :write-json configured throws"
    (let [bare (sse/interpreter {:render fake-render})]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #":write-json"
                            (sse/frame bare [[:patch-signals {:name ""}]])))))

  (testing "a non-collection, non-string signals value throws"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Unable to coerce"
                          (sse/frame itp [[:patch-signals 42]])))))

(deftest frames-are-values
  (is (= (sse/frame itp [[:patch-elements [:div "hi"]]])
         (sse/frame itp [[:patch-elements [:div "hi"]]]))
      "equal inputs produce = frames, which the dedupe drain loop relies on"))

(deftest apply!-writes-events-to-the-generator
  (let [gen (adapter.test/->sse-recorder)]
    (sse/apply! itp gen (sse/frame itp [[:patch-elements [:div "hi"] {:mode :append}]
                                        [:patch-signals {:name ""}]]))
    (let [events @(:!rec gen)]
      (is (= 2 (count events)))
      (is (re-find #"event: datastar-patch-elements" (first events)))
      (is (re-find #"elements <div>hi</div>" (first events)))
      (is (re-find #"mode append" (first events)))
      (is (re-find #"event: datastar-patch-signals" (second events))))))

(defn- ->counting-gen
  "An SSEGenerator whose `send-event!` succeeds `max-writes` times and reports
  a closed connection -- `false`, never an exception -- from then on, the way
  the SDK's adapters behave once a client has gone away."
  [!writes max-writes]
  (reify p/SSEGenerator
    (send-event! [_ _ _ _] (<= (swap! !writes inc) max-writes))
    (get-lock [_] nil)
    (close-sse! [_] true)
    (sse-gen? [_] true)))

(deftest apply!-reports-whether-the-connection-is-still-open
  (let [f (sse/frame itp [[:remove-element "#a"]
                          [:remove-element "#b"]
                          [:remove-element "#c"]])]
    (testing "an open connection"
      (is (true? (sse/apply! itp (->counting-gen (atom 0) 3) f))))

    (testing "a closed connection"
      (let [!writes (atom 0)]
        (is (false? (sse/apply! itp (->counting-gen !writes 0) f)))
        (is (= 3 @!writes)
            "every event is still attempted; only the verdict changes")))

    (testing "a connection that closes mid-frame"
      (let [!writes (atom 0)]
        (is (false? (sse/apply! itp (->counting-gen !writes 1) f)))
        (is (= 3 @!writes))))))

(deftest response-tags-data-for-the-middleware
  (let [r (sse/response {:events [[:remove-element "#x"]]})]
    (is (sse/response? r))
    (is (= [[:remove-element "#x"]] (:events r))))
  (is (not (sse/response? {:status 204}))))
