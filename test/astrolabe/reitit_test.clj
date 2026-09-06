(ns astrolabe.reitit-test
  (:require [clojure.test :refer [deftest is testing]]
            [astrolabe.reitit :as astrolabe]
            [astrolabe.sse :as sse]
            [reitit.ring :as ring]
            [starfederation.datastar.clojure.adapter.test :as adapter.test]
            [starfederation.datastar.clojure.adapter.common :as ac]))

(def itp (sse/interpreter {:render str :write-json pr-str}))

(defn- app [routes & {:as opts}]
  (ring/ring-handler
   (ring/router
    routes
    {:data {:middleware [(astrolabe/middleware
                          (merge {:->sse-response adapter.test/->sse-response
                                  :parse-json     read-string
                                  :interpreter    itp}
                                 opts))]}})))

(deftest signals-are-parsed-onto-the-request
  (testing "POST reads the body"
    (let [captured (atom nil)
          handler (app [["/x" {:datastar true
                               :post (fn [req] (reset! captured (:signals req)) {:status 204})}]])]
      (handler {:request-method :post :uri "/x" :body "{:name \"jo\"}"})
      (is (= {:name "jo"} @captured))))

  (testing "GET reads the datastar query param"
    (let [captured (atom nil)
          handler (app [["/x" {:datastar true
                               :get (fn [req] (reset! captured (:signals req)) {:status 204})}]])]
      (handler {:request-method :get :uri "/x" :query-params {"datastar" "{:name \"jo\"}"}})
      (is (= {:name "jo"} @captured))))

  (testing "DELETE reads the query param too"
    (let [captured (atom nil)
          handler (app [["/x" {:datastar true
                               :delete (fn [req] (reset! captured (:signals req)) {:status 204})}]])]
      (handler {:request-method :delete :uri "/x" :query-params {"datastar" "{:a 1}"}})
      (is (= {:a 1} @captured))))

  (testing "an InputStream body is slurped before parsing"
    (let [captured (atom nil)
          handler (app [["/x" {:datastar true
                               :post (fn [req] (reset! captured (:signals req)) {:status 204})}]])]
      (handler {:request-method :post :uri "/x"
                :body (java.io.ByteArrayInputStream. (.getBytes "{:name \"jo\"}"))})
      (is (= {:name "jo"} @captured))))

  (testing "already-parsed :body-params are used as-is"
    (let [captured (atom nil)
          handler (app [["/x" {:datastar true
                               :post (fn [req] (reset! captured (:signals req)) {:status 204})}]])]
      (handler {:request-method :post :uri "/x" :body-params {:name "jo"} :body "ignored"})
      (is (= {:name "jo"} @captured) "parse-json is not called again")))

  (testing "an empty body yields no :signals key rather than a parse error"
    (let [captured (atom ::unset)
          handler (app [["/x" {:datastar true
                               :post (fn [req] (reset! captured (contains? req :signals)) {:status 204})}]])]
      (handler {:request-method :post :uri "/x" :body ""})
      (is (false? @captured)))))

(deftest non-datastar-routes-are-untouched
  (let [captured (atom ::unset)
        handler (app [["/plain" {:get (fn [req] (reset! captured (:signals req)) {:status 200})}]])]
    (handler {:request-method :get :uri "/plain" :query-params {"datastar" "{:a 1}"}})
    (is (nil? @captured) "no :signals key is added to routes without :datastar true")))

(deftest sse-response-data-becomes-a-streaming-response
  (let [handler (app [["/x" {:datastar true
                             :post (fn [_] (sse/response
                                            {:events [[:patch-elements "<div/>" {:mode :append}]]}))}]])
        resp (handler {:request-method :post :uri "/x" :body "{}"})]
    (is (= 200 (:status resp)))
    (is (= "text/event-stream" (get-in resp [:headers "Content-Type"])))
    (let [events @(:body resp)]
      (is (= 1 (count events)))
      (is (re-find #"elements <div/>" (first events)))
      (is (re-find #"mode append" (first events))))))

(deftest plain-ring-responses-pass-through
  (let [handler (app [["/x" {:datastar true :post (fn [_] {:status 204 :body "ok"})}]])]
    (is (= {:status 204 :body "ok"} (handler {:request-method :post :uri "/x" :body "{}"})))))

(deftest compression-is-negotiated-per-request
  (let [chosen (atom nil)
        capture-response (fn [req opts] (reset! chosen (ac/write-profile opts)) {:status 200})
        br-profile {ac/content-encoding "br"}
        handler (app [["/x" {:datastar true :post (fn [_] (sse/response {:events []}))}]]
                     :->sse-response capture-response
                     :compression [["br" br-profile] ["gzip" ac/gzip-profile]])]

    (handler {:request-method :post :uri "/x" :body "{}"
              :headers {"accept-encoding" "gzip"}})
    (is (= ac/gzip-profile @chosen) "gzip when that is all the client offers")

    (handler {:request-method :post :uri "/x" :body "{}"
              :headers {"accept-encoding" "br, gzip"}})
    (is (= br-profile @chosen) "br when offered, following server preference")

    (handler {:request-method :post :uri "/x" :body "{}" :headers {}})
    (is (nil? @chosen) "uncompressed when the client offers nothing")))

(deftest sdk-callbacks-and-auto-close-are-passed-through
  (let [captured (atom nil)
        capture-response (fn [_req opts] (reset! captured opts) {:status 200})
        on-close (fn [_] :closed)
        on-exception (fn [_ _ _] :boom)
        handler (app [["/x" {:datastar true
                             :post (fn [_] (sse/response {:events []
                                                          :on-close on-close
                                                          :on-exception on-exception
                                                          :status 201
                                                          :headers {"X-Test" "1"}}))}]]
                     :->sse-response capture-response)]
    (handler {:request-method :post :uri "/x" :body "{}"})
    (is (= on-close (ac/on-close @captured)))
    (is (= on-exception (ac/on-exception @captured)))
    (is (= 201 (:status @captured)))
    (is (= {"X-Test" "1"} (:headers @captured)))))

(deftest an-explicit-write-profile-bypasses-negotiation
  (let [chosen (atom nil)
        capture-response (fn [req opts] (reset! chosen (ac/write-profile opts)) {:status 200})
        mine {ac/content-encoding "br"}
        handler (app [["/x" {:datastar true
                             :post (fn [_] (sse/response {:events [] :write-profile mine}))}]]
                     :->sse-response capture-response)]
    (handler {:request-method :post :uri "/x" :body "{}" :headers {"accept-encoding" "gzip"}})
    (is (= mine @chosen))))
