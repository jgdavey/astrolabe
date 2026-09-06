(ns astrolabe.event-test
  (:require [clojure.test :refer [deftest is testing]]
            [astrolabe.event :as event]))

(deftest vector-sugar-normalizes-to-canonical-map
  (testing "primary arg only"
    (is (= {:op :patch-elements :elements [:div "hi"]}
           (event/normalize [:patch-elements [:div "hi"]]))))

  (testing "primary arg plus opts"
    (is (= {:op :patch-elements :elements [:div "hi"] :mode :append :selector "#lane-0"}
           (event/normalize [:patch-elements [:div "hi"] {:mode :append :selector "#lane-0"}]))))

  (testing "a map primary arg is not mistaken for opts"
    (is (= {:op :patch-signals :signals {:name ""}}
           (event/normalize [:patch-signals {:name ""}]))))

  (testing "every documented op"
    (is (= {:op :patch-elements-seq :elements [[:li "a"]]}
           (event/normalize [:patch-elements-seq [[:li "a"]]])))
    (is (= {:op :remove-element :selector "#gone"}
           (event/normalize [:remove-element "#gone"])))
    (is (= {:op :execute-script :script "alert(1)"}
           (event/normalize [:execute-script "alert(1)"])))))

(deftest canonical-maps-pass-through-unchanged
  (let [m {:op :patch-elements :elements "<div/>" :mode :append}]
    (is (= m (event/normalize m)))))

(deftest invalid-events-throw
  (testing "unknown op"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Unknown event op"
                          (event/normalize [:frobnicate "x"])))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Unknown event op"
                          (event/normalize {:op :frobnicate}))))

  (testing "missing primary argument"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"missing primary argument"
                          (event/normalize [:patch-elements])))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"missing primary argument"
                          (event/normalize {:op :patch-elements :mode :append}))))

  (testing "trailing non-map"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"options must be a map"
                          (event/normalize [:patch-elements "<div/>" "oops"]))))

  (testing "too many elements"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"too many"
                          (event/normalize [:patch-elements "<div/>" {} :extra]))))

  (testing "not a vector or map"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"must be a vector or a map"
                          (event/normalize "nope")))))
