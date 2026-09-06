(ns astrolabe.event-test
  (:require [clojure.test :refer [deftest is testing]]
            [astrolabe.event :as event]
            [starfederation.datastar.clojure.api :as d*]))

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

(deftest friendly-opts-map-onto-sdk-keys
  (testing "patch-elements opts"
    (is (= {d*/selector             "#lane-0"
            d*/patch-mode           d*/pm-append
            d*/use-view-transition  true
            d*/retry-duration       500
            d*/element-ns           d*/ns-svg
            d*/id                   "evt-1"}
           (event/->sdk-opts
            {:op :patch-elements :elements "<div/>"
             :selector "#lane-0" :mode :append :use-view-transition? true
             :retry-duration 500 :element-ns :svg :id "evt-1"}))))

  (testing "signals and script opts"
    (is (= {d*/only-if-missing true}
           (event/->sdk-opts {:op :patch-signals :signals {} :only-if-missing? true})))
    (is (= {d*/auto-remove false d*/attributes {"type" "module"}}
           (event/->sdk-opts {:op :execute-script :script "x"
                              :auto-remove? false :attributes {"type" "module"}}))))

  (testing "absent opts produce no keys"
    (is (= {} (event/->sdk-opts {:op :patch-elements :elements "<div/>"}))))

  (testing "false and nil are preserved, not dropped"
    (is (= {d*/use-view-transition false}
           (event/->sdk-opts {:op :patch-elements :elements "<div/>"
                              :use-view-transition? false}))))

  (testing "every documented mode keyword"
    (doseq [[kw const] {:outer   d*/pm-outer   :inner   d*/pm-inner
                        :append  d*/pm-append  :prepend d*/pm-prepend
                        :before  d*/pm-before  :after   d*/pm-after
                        :remove  d*/pm-remove  :replace d*/pm-replace}]
      (is (= {d*/patch-mode const}
             (event/->sdk-opts {:op :patch-elements :elements "x" :mode kw}))
          (str "mode " kw))))

  (testing "raw SDK constants pass through untouched"
    (is (= {d*/patch-mode "append"}
           (event/->sdk-opts {:op :patch-elements :elements "x" :mode "append"}))
        "a string is assumed to already be an SDK constant")
    (is (= {d*/element-ns d*/ns-svg}
           (event/->sdk-opts {:op :patch-elements :elements "x" :element-ns d*/ns-svg}))))

  (testing "unknown enum keywords throw"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Unknown :mode"
                          (event/->sdk-opts {:op :patch-elements :elements "x" :mode :sideways})))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Unknown :element-ns"
                          (event/->sdk-opts {:op :patch-elements :elements "x" :element-ns :xaml})))))
