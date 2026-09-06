(ns astrolabe.event
  "Event data: vector sugar in, canonical maps out.")

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
