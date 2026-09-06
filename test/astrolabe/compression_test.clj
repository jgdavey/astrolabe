(ns astrolabe.compression-test
  (:require [clojure.test :refer [deftest is testing]]
            [astrolabe.compression :as compression]))

(deftest parsing-accept-encoding
  (is (= {"gzip" 1.0 "br" 1.0} (compression/parse-accept-encoding "gzip, br")))
  (is (= {"gzip" 1.0 "br" 0.5} (compression/parse-accept-encoding "gzip, br;q=0.5")))
  (is (= {"br" 0.0 "gzip" 1.0} (compression/parse-accept-encoding "br;q=0, gzip")))
  (is (= {"gzip" 1.0} (compression/parse-accept-encoding "  GZIP  ")) "case and space insensitive")
  (is (= {} (compression/parse-accept-encoding nil)))
  (is (= {} (compression/parse-accept-encoding ""))))

(deftest negotiation-picks-server-preference-first
  (let [br :br-profile
        gzip :gzip-profile
        config [["br" br] ["gzip" gzip]]]

    (testing "first configured coding the client accepts wins"
      (is (= br (compression/negotiate config "br, gzip")))
      (is (= br (compression/negotiate config "gzip, br")) "server order, not client order")
      (is (= gzip (compression/negotiate config "gzip"))))

    (testing "q=0 is a refusal"
      (is (= gzip (compression/negotiate config "br;q=0, gzip"))))

    (testing "no acceptable coding means uncompressed"
      (is (nil? (compression/negotiate config "deflate")))
      (is (nil? (compression/negotiate config nil)))
      (is (nil? (compression/negotiate [] "br, gzip"))))

    (testing "a wildcard accepts anything configured"
      (is (= br (compression/negotiate config "*")))
      (is (= gzip (compression/negotiate config "br;q=0, *"))
          "an explicit refusal beats the wildcard"))))
