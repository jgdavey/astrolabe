(ns astrolabe.reitit
  "Reitit wiring. Returns a plain middleware map, so reitit is not a dependency."
  (:require [astrolabe.compression :as compression]
            [astrolabe.sse :as sse]
            [clojure.string :as str]
            [starfederation.datastar.clojure.api :as d*]
            [starfederation.datastar.clojure.adapter.common :as ac]
            [starfederation.datastar.clojure.consts :as consts]))

(defn- read-signals
  "Return the raw Datastar signals payload, or nil when there is nothing to read.

  Adapted from hindsight. This does more than the SDK's `get-signals`, which
  returns an InputStream for non-GET requests and leaves the rest to you:
  - GET/DELETE read the `datastar` query param (per the SDK)
  - a body already parsed upstream (muuntaja's `:body-params`) is used as-is
  - anything else is slurped to a string, with a blank body treated as absent
    so an empty POST does not become a JSON parse error"
  [req]
  (case (:request-method req)
    (:get :delete) (get-in req [:query-params consts/datastar-key])
    (if (seq (:body-params req))
      (:body-params req)
      (let [raw (:body req)
            s   (if (string? raw) raw (some-> raw slurp))]
        (when-not (str/blank? s) s)))))

(defn- with-signals
  "Attach `:signals` when there is a payload. A payload that arrived
  pre-parsed is passed through without calling `parse-json` again."
  [req parse-json]
  (if-let [raw (read-signals req)]
    (assoc req :signals (if (string? raw) (parse-json raw) raw))
    req))

(defn- ->on-open
  "Every response is either a set of events to write (then close, unless
  `:auto-close?` is false) or a caller-supplied on-open that takes over the
  connection, as `hub/connect!` does."
  [interpreter {:keys [events on-open auto-close?]}]
  (or on-open
      (fn [sse-gen]
        (if (false? auto-close?)
          (sse/apply! interpreter sse-gen (sse/frame interpreter events))
          (d*/with-open-sse sse-gen
            (sse/apply! interpreter sse-gen (sse/frame interpreter events)))))))

(defn- ->response
  [{:keys [->sse-response interpreter compression]} req resp]
  (let [profile (or (:write-profile resp)
                    (compression/negotiate compression (get-in req [:headers "accept-encoding"])))]
    (->sse-response
     req
     (cond-> {ac/on-open (->on-open interpreter resp)}
       profile               (assoc ac/write-profile profile)
       (:on-close resp)      (assoc ac/on-close (:on-close resp))
       (:on-exception resp)  (assoc ac/on-exception (:on-exception resp))
       (:status resp)        (assoc :status (:status resp))
       (:headers resp)       (assoc :headers (:headers resp))))))

(defn middleware
  "Reitit middleware for Datastar routes. Applies only to routes with
  `:datastar true`.

  Opts:
  - `:->sse-response` the SDK adapter's ->sse-response (required)
  - `:parse-json`     reads Datastar signals off the request (required)
  - `:interpreter`    an `astrolabe.sse/interpreter` (required)
  - `:compression`    ordered [[coding write-profile] ...], defaults to gzip"
  [{:keys [->sse-response parse-json interpreter compression] :as opts}]
  (when (nil? ->sse-response) (throw (ex-info ":->sse-response is required" {})))
  (when (nil? parse-json)     (throw (ex-info ":parse-json is required" {})))
  (when (nil? interpreter)    (throw (ex-info ":interpreter is required" {})))
  (let [opts (assoc opts :compression (or compression compression/default-compression))]
    {:name ::middleware
     :compile (fn [route-data _router-opts]
                (when (:datastar route-data)
                  (fn [handler]
                    (fn [req]
                      (let [req  (with-signals req parse-json)
                            resp (handler req)]
                        (if (sse/response? resp)
                          (->response opts req resp)
                          resp))))))}))
