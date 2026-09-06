(ns astrolabe.brotli-test
  (:require [clojure.test :refer [deftest is testing]]
            [astrolabe.brotli :as brotli]
            [starfederation.datastar.clojure.adapter.common :as ac]))

(deftest profile-is-a-usable-write-profile
  (testing "defaults are SSE-tuned, below the SDK module's 5/24"
    (let [p (brotli/profile)]
      (is (= "br" (ac/content-encoding p)))
      (is (fn? (ac/wrap-output-stream p)))))

  (testing "quality and window size are overridable"
    (let [p (brotli/profile {:quality 6 :window-size 22})]
      (is (= "br" (ac/content-encoding p))))))

(deftest defaults-are-documented-values
  (is (= {:quality 4 :window-size 20} brotli/default-opts)))
