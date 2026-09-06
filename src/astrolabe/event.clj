(ns astrolabe.event
  "Event data: vector sugar in, canonical maps out."
  (:require [starfederation.datastar.clojure.api :as d*]))

(def op->primary
  "Maps an event op to the canonical key holding its primary argument."
  {:patch-elements     :elements
   :patch-elements-seq :elements
   :patch-signals      :signals
   :remove-element     :selector
   :execute-script     :script})

(defn- primary-key [op event]
  (or (get op->primary op)
      (throw (ex-info "Unknown event op"
                      {:op op :event event :known (set (keys op->primary))}))))

(defn- normalize-vector [event]
  (let [[op primary opts] event
        pk (primary-key op event)
        n  (count event)]
    (when (< n 2)
      (throw (ex-info "Event missing primary argument" {:op op :event event})))
    (when (> n 3)
      (throw (ex-info "Event has too many elements" {:event event})))
    (when (and (= n 3) (not (map? opts)))
      (throw (ex-info "Event options must be a map" {:opts opts :event event})))
    (assoc (or opts {}) :op op pk primary)))

(defn- normalize-map [event]
  (let [pk (primary-key (:op event) event)]
    (when-not (contains? event pk)
      (throw (ex-info "Event missing primary argument"
                      {:op (:op event) :expected-key pk :event event})))
    event))

(defn normalize
  "Normalize an event into its canonical map form. Accepts `[op primary opts?]`
  vector sugar or an already-canonical map. Throws on anything else."
  [event]
  (cond
    (map? event)    (normalize-map event)
    (vector? event) (normalize-vector event)
    :else (throw (ex-info "Event must be a vector or a map" {:event event}))))

(def ^:private mode->const
  {:outer   d*/pm-outer    :inner   d*/pm-inner
   :remove  d*/pm-remove   :prepend d*/pm-prepend
   :append  d*/pm-append   :before  d*/pm-before
   :after   d*/pm-after    :replace d*/pm-replace})

(def ^:private element-ns->const
  {:html d*/ns-html :svg d*/ns-svg :mathml d*/ns-mathml})

(defn- enum
  "Translate a friendly keyword into its SDK constant. Strings pass through --
  they are assumed to be SDK constants already, which is what hindsight's
  `#(get patch-modes % %)` fallback bought. Unknown keywords throw, so a typo
  is a loud error rather than a silently dropped option."
  [table label v]
  (cond
    (string? v) v
    (contains? table v) (get table v)
    :else (throw (ex-info (str "Unknown " label " value")
                          {label v :known (set (keys table))}))))

;; friendly key -> [sdk key, value transform]
(def ^:private opt-table
  {:id                    [d*/id                   identity]
   :retry-duration        [d*/retry-duration       identity]
   :selector              [d*/selector             identity]
   :mode                  [d*/patch-mode           #(enum mode->const :mode %)]
   :use-view-transition?  [d*/use-view-transition  identity]
   :element-ns            [d*/element-ns           #(enum element-ns->const :element-ns %)]
   :only-if-missing?      [d*/only-if-missing      identity]
   :auto-remove?          [d*/auto-remove          identity]
   :attributes            [d*/attributes           identity]})

(defn ->sdk-opts
  "Translate a canonical event's friendly options into the SDK's option map.
  Keys absent from the event are absent from the result; `false` is preserved."
  [event]
  (reduce-kv (fn [acc friendly [sdk-key xf]]
               (if (contains? event friendly)
                 (assoc acc sdk-key (xf (get event friendly)))
                 acc))
             {}
             opt-table))
