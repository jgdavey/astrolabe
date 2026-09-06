(ns astrolabe.sse
  "Interpreters, frames, and SSE response data."
  (:require [astrolabe.event :as event]
            [starfederation.datastar.clojure.api :as d*]))

(defrecord Interpreter [render write-json])

(defn interpreter
  "Build an interpreter. `:render` turns an element into an HTML string and is
  required. `:write-json` serializes `:patch-signals` maps and is required only
  if you pass maps."
  [{:keys [render write-json]}]
  (when-not (ifn? render)
    (throw (ex-info ":render is required and must be a function" {:render render})))
  (->Interpreter render write-json))

(defn- ->json
  "Adapted from hindsight's `ensure-json`: a string is already JSON, a
  collection is serialized, anything else is a mistake worth naming."
  [{:keys [write-json]} signals]
  (cond
    (string? signals) signals
    (not (coll? signals))
    (throw (ex-info "Unable to coerce :signals as a JSON string"
                    {:signals signals}))
    (nil? write-json)
    (throw (ex-info "A :patch-signals map needs :write-json on the interpreter"
                    {:signals signals}))
    :else (write-json signals)))

(defn- render-event [{:keys [render] :as itp} ev]
  (case (:op ev)
    :patch-elements     (update ev :elements render)
    :patch-elements-seq (update ev :elements #(mapv render %))
    :patch-signals      (update ev :signals #(->json itp %))
    ev))

(defn frame
  "Normalize and render `events` into a connection-independent frame: a vector
  of canonical event maps whose payloads are already strings."
  [itp events]
  (mapv #(render-event itp (event/normalize %)) events))

(defn- write-one!
  "Write one canonical event to an SDK sse-gen, returning the SDK's own boolean:
  `false` if the connection is closed, `true` otherwise."
  [sse-gen ev]
  (let [opts (event/->sdk-opts ev)]
    (case (:op ev)
      :patch-elements     (d*/patch-elements!     sse-gen (:elements ev) opts)
      :patch-elements-seq (d*/patch-elements-seq! sse-gen (:elements ev) opts)
      :patch-signals      (d*/patch-signals!      sse-gen (:signals ev)  opts)
      :remove-element     (d*/remove-element!     sse-gen (:selector ev) opts)
      :execute-script     (d*/execute-script!     sse-gen (:script ev)   opts))))

(defn apply!
  "Write a frame to an SDK sse-gen.

  Returns `false` if any write reported the connection closed, `true`
  otherwise. Every event in the frame is attempted regardless -- a failure
  never skips the rest of the frame, only the verdict is affected. This is the
  only signal a caller gets that a client has gone away: the SDK's adapters
  catch the disconnect internally and report it as a `false` return, never as
  an exception."
  [sse-gen frame]
  (reduce (fn [ok ev] (if (write-one! sse-gen ev) ok false)) true frame))

(defn response
  "Describe an SSE response as data. The middleware turns this into a streaming
  response. Normally it carries either `:events` (write them, then close) or
  `:on-open` (take over the connection, as `hub/connect!` does); neither is
  enforced, and `{:events []}` is a legitimate empty response.

  Keys:
  - `:events`        event data to write
  - `:on-open`       (fn [sse-gen]) taking over the connection; used by `hub/connect!`
  - `:auto-close?`   close after writing `:events`; defaults to true
  - `:write-profile` an explicit SDK write profile, bypassing negotiation
  - `:status`        HTTP status, defaults to 200
  - `:headers`       extra response headers
  - `:on-close`      SDK on-close callback
  - `:on-exception`  SDK on-exception callback"
  [m]
  (assoc m ::response true))

(defn response? [x]
  (boolean (and (map? x) (::response x))))
