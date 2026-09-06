# Astrolabe Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `astrolabe`, an ergonomic Clojure companion layer over the official `datastar-clojure` SDK: data-first SSE events, reitit wiring, content-negotiated compression, and a connection hub for broadcasting.

**Architecture:** Events are plain data (vector sugar normalized to canonical maps). An *interpreter* carries `:render` and `:write-json` and turns events into a *frame* — a connection-independent value whose `:elements` are already HTML strings. Frames are written to connections through per-connection queues drained by virtual threads, which makes `send!` non-blocking and isolates slow clients. Everything library-specific (HTML rendering, JSON, compression codecs) is injected, never required by `astrolabe` itself.

**Tech Stack:** Clojure, `dev.data-star.clojure/sdk` 1.0.0-RC11, deps.edn, kaocha (`bin/kaocha`), matcher-combinators, test.check. Java 21+ (virtual threads).

**Spec:** `README.md` (the design doc — read it before starting; it is the source of truth for API shape and naming)

**Prior art:** `~/src/hindsight/src/clj/hindsight/datastar.clj` is a working
single-app version of this layer. Several decisions below are taken from it
directly and are marked where they appear: the reitit `:compile` shape, the
richer signals reader, `ensure-json`, and the on-close/on-exception passthrough.
Read it before Tasks 2, 3, and 8. It differs from this spec in three ways —
it flags routes `:datastar?` (we use `:datastar`), it renders eagerly at
dispatch time (we render into frames), and it has no hub — so copy its
mechanics, not its structure.

## Global Constraints

- **Java 21 or later.** Virtual threads are the delivery model, not an optimization.
- **SDK version `1.0.0-RC11`.** SDK option keys are namespaced keywords (`:d*.sse/id`, `:d*.elements/selector`, …); always reference them through `starfederation.datastar.clojure.api` vars (`d*/id`, `d*/selector`), never as literals.
- **`astrolabe` must never load `starfederation.datastar.clojure.brotli`** from any namespace except `astrolabe.brotli`. That namespace calls `Brotli4jLoader/ensureAvailability` at load time and throws when natives are absent. No `requiring-resolve` detection either.
- **No hard dependencies** on hiccup, chassis, jsonista, or reitit. Rendering and JSON are injected. `astrolabe.reitit` returns a plain reitit middleware *map* and must not `require` reitit.
- **Frames must be values comparable with `=`.** The documented dedupe drain loop depends on it. No records with identity semantics, no lazy seqs in frames.
- **Public namespaces:** `astrolabe.event`, `astrolabe.sse`, `astrolabe.queue`, `astrolabe.hub`, `astrolabe.compression`, `astrolabe.reitit`, `astrolabe.brotli`.
- **Test runner:** `bin/kaocha`. Every task ends green before commit.

---

### Task 1: Project setup and event normalization

**Files:**
- Modify: `deps.edn`
- Delete: `src/astrolabe/api.clj`, `test/astrolabe/api_test.clj`
- Create: `src/astrolabe/event.clj`
- Test: `test/astrolabe/event_test.clj`

**Interfaces:**
- Consumes: nothing.
- Produces: `astrolabe.event/normalize` — `(normalize event) → canonical-map`. Accepts a vector `[op primary opts?]` or an already-canonical map; throws `ExceptionInfo` otherwise. `astrolabe.event/op->primary` — map of `op → primary-key`.

- [ ] **Step 1: Add `:paths` and `:deps` to `deps.edn`**

The boilerplate has only `:aliases`. Add an explicit source path and the SDK, so `src` compiles outside the test alias:

```clojure
{:paths ["src"]
 :deps  {org.clojure/clojure       {:mvn/version "1.12.0"}
         dev.data-star.clojure/sdk {:mvn/version "1.0.0-RC11"}}
 :aliases {:test   {:extra-paths ["test"]
                    :extra-deps  {org.clojure/test.check       {:mvn/version "1.1.3"}
                                  nubank/matcher-combinators   {:mvn/version "3.11.0"}
                                  dev.data-star.clojure/ring   {:mvn/version "1.0.0-RC11"}
                                  dev.data-star.clojure/brotli {:mvn/version "1.0.0-RC11"}}}
           :kaocha {:main-opts  ["-m" "kaocha.runner"]
                    :extra-deps {lambdaisland/kaocha {:mvn/version "1.91.1392"}}}}}
```

- [ ] **Step 2: Delete the boilerplate placeholder namespace**

```bash
git rm src/astrolabe/api.clj test/astrolabe/api_test.clj
```

- [ ] **Step 3: Write the failing test**

Create `test/astrolabe/event_test.clj`:

```clojure
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
```

- [ ] **Step 4: Run the test to verify it fails**

Run: `bin/kaocha --focus astrolabe.event-test`
Expected: FAIL — `astrolabe.event` namespace does not exist.

- [ ] **Step 5: Write the implementation**

Create `src/astrolabe/event.clj`:

```clojure
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
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `bin/kaocha --focus astrolabe.event-test`
Expected: PASS, 3 tests.

- [ ] **Step 7: Commit**

```bash
git add deps.edn src/astrolabe/event.clj test/astrolabe/event_test.clj
git commit -m "Add event normalization"
```

---

### Task 2: Friendly options to SDK option keys

**Files:**
- Modify: `src/astrolabe/event.clj`
- Test: `test/astrolabe/event_test.clj`

**Interfaces:**
- Consumes: `astrolabe.event/normalize` (Task 1).
- Produces: `astrolabe.event/->sdk-opts` — `(->sdk-opts canonical-event) → map of SDK namespaced-keyword options`. Ignores `:op` and the primary key; throws on unknown `:mode` / `:element-ns` keywords.

- [ ] **Step 1: Write the failing test**

Append to `test/astrolabe/event_test.clj`:

```clojure
(ns astrolabe.event-test
  (:require [clojure.test :refer [deftest is testing]]
            [astrolabe.event :as event]
            [starfederation.datastar.clojure.api :as d*]))

;; ... existing tests above ...

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
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `bin/kaocha --focus astrolabe.event-test`
Expected: FAIL — `->sdk-opts` is not defined.

- [ ] **Step 3: Write the implementation**

Add to `src/astrolabe/event.clj` (extend the `ns` form first):

```clojure
(ns astrolabe.event
  "Event data: vector sugar in, canonical maps out."
  (:require [starfederation.datastar.clojure.api :as d*]))
```

```clojure
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
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `bin/kaocha --focus astrolabe.event-test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/astrolabe/event.clj test/astrolabe/event_test.clj
git commit -m "Translate friendly event options to SDK option keys"
```

---

### Task 3: Interpreter, frames, and SSE responses

**Files:**
- Create: `src/astrolabe/sse.clj`
- Test: `test/astrolabe/sse_test.clj`

**Interfaces:**
- Consumes: `astrolabe.event/normalize`, `astrolabe.event/->sdk-opts` (Tasks 1–2).
- Produces:
  - `astrolabe.sse/interpreter` — `(interpreter {:render f :write-json g}) → Interpreter`. `:render` required; `:write-json` optional.
  - `astrolabe.sse/frame` — `(frame itp events) → vector of canonical maps` with `:elements` rendered to strings and `:signals` serialized. Comparable with `=`.
  - `astrolabe.sse/apply!` — `(apply! itp sse-gen frame) → nil`. Writes a frame to an SDK sse-gen.
  - `astrolabe.sse/response` — `(response m) → m` tagged with `::response`; keys `:events`, `:on-open`, `:status`, `:headers`, `:write-profile`, `:auto-close?`.
  - `astrolabe.sse/response?` — predicate used by the middleware.

- [ ] **Step 1: Write the failing test**

Create `test/astrolabe/sse_test.clj`:

```clojure
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

(deftest response-tags-data-for-the-middleware
  (let [r (sse/response {:events [[:remove-element "#x"]]})]
    (is (sse/response? r))
    (is (= [[:remove-element "#x"]] (:events r))))
  (is (not (sse/response? {:status 204}))))
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `bin/kaocha --focus astrolabe.sse-test`
Expected: FAIL — `astrolabe.sse` does not exist.

- [ ] **Step 3: Write the implementation**

Create `src/astrolabe/sse.clj`:

```clojure
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

(defn apply!
  "Write a frame to an SDK sse-gen."
  [_itp sse-gen frame]
  (doseq [ev frame]
    (let [opts (event/->sdk-opts ev)]
      (case (:op ev)
        :patch-elements     (d*/patch-elements!     sse-gen (:elements ev) opts)
        :patch-elements-seq (d*/patch-elements-seq! sse-gen (:elements ev) opts)
        :patch-signals      (d*/patch-signals!      sse-gen (:signals ev)  opts)
        :remove-element     (d*/remove-element!     sse-gen (:selector ev) opts)
        :execute-script     (d*/execute-script!     sse-gen (:script ev)   opts))))
  nil)

(defn response
  "Describe an SSE response as data. The middleware turns this into a streaming
  response. Either `:events` (write them, then close) or `:on-open` (take over
  the connection) must be present.

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
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `bin/kaocha --focus astrolabe.sse-test`
Expected: PASS. If the recorder field access `@(:!rec gen)` fails, note that
`RecordMsgGen` is a `defrecord` with fields `[lock !rec !open?]` — the keyword
accessor works, and `!rec` is a volatile, so deref with `@`.

- [ ] **Step 5: Commit**

```bash
git add src/astrolabe/sse.clj test/astrolabe/sse_test.clj
git commit -m "Add interpreter, frames, and SSE response data"
```

---

### Task 4: Queues

**Files:**
- Create: `src/astrolabe/queue.clj`
- Test: `test/astrolabe/queue_test.clj`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `astrolabe.queue/Queue` protocol — `(offer! q frame) → boolean` (false = close the connection), `(take! q) → frame or nil` (nil = closed), `(close! q) → nil`.
  - Constructors `(bounded n)`, `(latest)`, `(unbounded)`.
  - `astrolabe.queue/->queue-fn` — `(->queue-fn x) → (fn [conn] queue)`; accepts `:bounded` / `:latest` / `:unbounded` keywords or a 1-arg fn.

- [ ] **Step 1: Write the failing test**

Create `test/astrolabe/queue_test.clj`:

```clojure
(ns astrolabe.queue-test
  (:require [clojure.test :refer [deftest is testing]]
            [astrolabe.queue :as queue]))

(deftest bounded-refuses-when-full
  (let [q (queue/bounded 2)]
    (is (true? (queue/offer! q :a)))
    (is (true? (queue/offer! q :b)))
    (is (false? (queue/offer! q :c)) "a full bounded queue refuses, asking for a disconnect")
    (is (= :a (queue/take! q)))
    (is (= :b (queue/take! q)))
    (is (true? (queue/offer! q :d)) "space frees up after a take")))

(deftest latest-coalesces
  (let [q (queue/latest)]
    (is (true? (queue/offer! q :a)))
    (is (true? (queue/offer! q :b)) "never refuses")
    (is (true? (queue/offer! q :c)))
    (is (= :c (queue/take! q)) "only the newest frame survives")))

(deftest unbounded-never-refuses
  (let [q (queue/unbounded)]
    (dotimes [i 1000] (is (true? (queue/offer! q i))))
    (is (= 0 (queue/take! q)))))

(deftest take!-blocks-until-a-frame-arrives
  (doseq [[label q] [["bounded" (queue/bounded 4)]
                     ["latest" (queue/latest)]
                     ["unbounded" (queue/unbounded)]]]
    (testing label
      (let [result (promise)
            t (Thread/startVirtualThread #(deliver result (queue/take! q)))]
        (is (not (realized? result)) "take! has not returned yet")
        (queue/offer! q :frame)
        (is (= :frame (deref result 1000 ::timeout)))
        (.join t)))))

(deftest close!-wakes-a-blocked-take!-with-nil
  (doseq [[label q] [["bounded" (queue/bounded 4)]
                     ["latest" (queue/latest)]
                     ["unbounded" (queue/unbounded)]]]
    (testing label
      (let [result (promise)
            t (Thread/startVirtualThread #(deliver result (queue/take! q)))]
        (queue/close! q)
        (is (nil? (deref result 1000 ::timeout)) "closed queues return nil, ending the drain loop")
        (.join t)))))

(deftest queue-fn-resolution
  (testing "keywords map to built-ins at their defaults"
    (is (satisfies? queue/Queue ((queue/->queue-fn :bounded) {})))
    (is (satisfies? queue/Queue ((queue/->queue-fn :latest) {})))
    (is (satisfies? queue/Queue ((queue/->queue-fn :unbounded) {}))))

  (testing "a function is used as-is and receives the connection"
    (let [seen (atom nil)
          f (queue/->queue-fn (fn [conn] (reset! seen conn) (queue/latest)))]
      (f {:topic :app})
      (is (= {:topic :app} @seen))))

  (testing "anything else throws"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Unknown :queue-fn"
                          (queue/->queue-fn :nonsense)))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #":queue-fn must be"
                          (queue/->queue-fn 42)))))
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `bin/kaocha --focus astrolabe.queue-test`
Expected: FAIL — `astrolabe.queue` does not exist.

- [ ] **Step 3: Write the implementation**

Create `src/astrolabe/queue.clj`:

```clojure
(ns astrolabe.queue
  "Per-connection frame queues. Overflow policy is `offer!`'s return value:
  a queue that returns false is asking for its connection to be closed."
  (:import [java.util.concurrent LinkedBlockingQueue]
           [java.util.concurrent.locks ReentrantLock Condition]))

(defprotocol Queue
  (offer! [q frame]
    "Accept a frame. Returns false if the connection should close.")
  (take! [q]
    "Block until a frame is available. Returns nil when the queue is closed.")
  (close! [q]
    "Close the queue, waking any blocked `take!` with nil."))

(def ^:private CLOSED ::closed)

(deftype BlockingFrameQueue [^LinkedBlockingQueue q]
  Queue
  (offer! [_ frame] (.offer q frame))
  (take!  [_] (let [v (.take q)]
                (if (identical? v CLOSED)
                  (do (.offer q CLOSED) nil)   ; stay closed for any other taker
                  v)))
  (close! [_] (.clear q) (.offer q CLOSED) nil))

(defn bounded
  "A queue holding at most `n` frames. Refuses further frames when full, which
  closes the connection so the client reconnects and resyncs. Use for
  event-based topics, where dropping a delta would diverge the client silently."
  [n]
  (->BlockingFrameQueue (LinkedBlockingQueue. (int n))))

(defn unbounded
  "A queue that never refuses and grows without limit. Useful in tests."
  []
  (->BlockingFrameQueue (LinkedBlockingQueue.)))

(deftype LatestFrameQueue [^ReentrantLock lock
                           ^Condition ready
                           ^:volatile-mutable slot
                           ^:volatile-mutable closed?]
  Queue
  (offer! [_ frame]
    (.lock lock)
    (try
      (when-not closed?
        (set! slot frame)
        (.signalAll ready))
      true
      (finally (.unlock lock))))

  (take! [_]
    (.lock lock)
    (try
      (loop []
        (cond
          (some? slot) (let [v slot] (set! slot nil) v)
          closed?      nil
          :else        (do (.await ready) (recur))))
      (finally (.unlock lock))))

  (close! [_]
    (.lock lock)
    (try
      (set! closed? true)
      (set! slot nil)
      (.signalAll ready)
      nil
      (finally (.unlock lock)))))

(defn latest
  "A depth-1 queue where a newer frame replaces the pending one. Never refuses.
  Use for state-based topics, where each frame is a complete snapshot and a
  superseded one is worthless."
  []
  (let [lock (ReentrantLock.)]
    (->LatestFrameQueue lock (.newCondition lock) nil false)))

(def ^:private built-ins
  {:bounded   #(bounded 64)
   :latest    latest
   :unbounded unbounded})

(defn ->queue-fn
  "Resolve a `:queue-fn` config value into a 1-arg function of a connection."
  [x]
  (cond
    (keyword? x) (if-let [ctor (get built-ins x)]
                   (fn [_conn] (ctor))
                   (throw (ex-info "Unknown :queue-fn keyword"
                                   {:queue-fn x :known (set (keys built-ins))})))
    (ifn? x) x
    :else (throw (ex-info ":queue-fn must be a keyword or a function of one connection"
                          {:queue-fn x}))))
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `bin/kaocha --focus astrolabe.queue-test`
Expected: PASS, 6 tests.

- [ ] **Step 5: Commit**

```bash
git add src/astrolabe/queue.clj test/astrolabe/queue_test.clj
git commit -m "Add per-connection frame queues"
```

---

### Task 5: Hub registry — subscribe, unsubscribe, conns, meta

**Files:**
- Create: `src/astrolabe/hub.clj`
- Test: `test/astrolabe/hub_test.clj`

**Interfaces:**
- Consumes: `astrolabe.queue` (Task 4), `astrolabe.sse/interpreter` (Task 3).
- Produces:
  - `astrolabe.hub/Hub` protocol — `(-subscribe! hub topic conn)`, `(-unsubscribe! hub topic conn)`, `(-conns hub topic)`.
  - `astrolabe.hub/in-memory` — `(in-memory {:interpreter itp :queue-fn qf :drain d}) → Hub`.
  - `astrolabe.hub/->Connection`, with fields `sse-gen`, `queue`, `topic`, `meta`.
  - `astrolabe.hub/subscribe!`, `unsubscribe!`, `conns`, `meta`.
  - **`astrolabe.hub` must `(:refer-clojure :exclude [meta])`.**

- [ ] **Step 1: Write the failing test**

Create `test/astrolabe/hub_test.clj`:

```clojure
(ns astrolabe.hub-test
  (:require [clojure.test :refer [deftest is testing]]
            [astrolabe.hub :as hub]
            [astrolabe.queue :as queue]
            [astrolabe.sse :as sse]
            [starfederation.datastar.clojure.adapter.test :as adapter.test]))

(def itp (sse/interpreter {:render str :write-json pr-str}))

(defn test-hub [] (hub/in-memory {:interpreter itp}))

(defn test-conn
  ([topic] (test-conn topic {}))
  ([topic meta]
   (hub/->Connection (adapter.test/->sse-recorder) (queue/unbounded) topic meta)))

(deftest subscribe-and-unsubscribe
  (let [h (test-hub)
        a (test-conn :room)
        b (test-conn :room)]
    (is (= [] (hub/conns h :room)) "unknown topics have no connections")

    (hub/subscribe! h :room a)
    (hub/subscribe! h :room b)
    (is (= #{a b} (set (hub/conns h :room))))

    (hub/unsubscribe! h :room a)
    (is (= [b] (hub/conns h :room)))

    (hub/unsubscribe! h :room b)
    (is (= [] (hub/conns h :room)) "the topic is empty once its last conn leaves")))

(deftest topics-are-isolated
  (let [h (test-hub)
        a (test-conn :one)
        b (test-conn :two)]
    (hub/subscribe! h :one a)
    (hub/subscribe! h :two b)
    (is (= [a] (hub/conns h :one)))
    (is (= [b] (hub/conns h :two)))))

(deftest unsubscribe-is-idempotent
  (let [h (test-hub)
        a (test-conn :room)]
    (hub/subscribe! h :room a)
    (hub/unsubscribe! h :room a)
    (is (nil? (hub/unsubscribe! h :room a)) "unsubscribing twice is not an error")
    (is (= [] (hub/conns h :room)))))

(deftest meta-exposes-app-data
  (let [c (test-conn :room {:uid 42})]
    (is (= {:uid 42} (hub/meta c)))))

(deftest hub-defaults
  (testing "queue-fn defaults to :bounded"
    (let [h (test-hub)]
      (is (satisfies? queue/Queue ((:queue-fn h) (test-conn :room))))))
  (testing "an interpreter is required"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #":interpreter is required"
                          (hub/in-memory {})))))
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `bin/kaocha --focus astrolabe.hub-test`
Expected: FAIL — `astrolabe.hub` does not exist.

- [ ] **Step 3: Write the implementation**

Create `src/astrolabe/hub.clj`:

```clojure
(ns astrolabe.hub
  "A topic-keyed registry of open SSE connections."
  (:refer-clojure :exclude [meta])
  (:require [astrolabe.queue :as queue]
            [astrolabe.sse :as sse]))

(defrecord Connection [sse-gen queue topic meta])

(defn meta
  "The app-supplied data attached to a connection at `connect!` time."
  [conn]
  (:meta conn))

(defprotocol Hub
  (-subscribe!   [hub topic conn])
  (-unsubscribe! [hub topic conn])
  (-conns        [hub topic]))

(defrecord InMemoryHub [!topics interpreter queue-fn drain]
  Hub
  (-subscribe! [_ topic conn]
    (swap! !topics update topic (fnil conj #{}) conn)
    nil)
  (-unsubscribe! [_ topic conn]
    (swap! !topics (fn [topics]
                     (let [remaining (disj (get topics topic #{}) conn)]
                       (if (seq remaining)
                         (assoc topics topic remaining)
                         (dissoc topics topic)))))
    nil)
  (-conns [_ topic]
    (vec (get @!topics topic))))

(defn subscribe!   [hub topic conn] (-subscribe! hub topic conn))
(defn unsubscribe! [hub topic conn] (-unsubscribe! hub topic conn))
(defn conns        [hub topic]      (-conns hub topic))

(declare default-drain)

(defn in-memory
  "A single-node hub. Connections live in this process and are lost on restart;
  browsers reconnect via Datastar's SSE retry."
  [{:keys [interpreter queue-fn drain]}]
  (when (nil? interpreter)
    (throw (ex-info ":interpreter is required" {})))
  (->InMemoryHub (atom {})
                 interpreter
                 (queue/->queue-fn (or queue-fn :bounded))
                 (or drain default-drain)))
```

Add a placeholder `default-drain` so the namespace loads; Task 6 fills it in:

```clojure
(defn default-drain
  "Take frames until the queue closes, writing each one."
  [_conn q write!]
  (loop []
    (when-let [frame (queue/take! q)]
      (write! frame)
      (recur))))
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `bin/kaocha --focus astrolabe.hub-test`
Expected: PASS, 5 tests.

- [ ] **Step 5: Commit**

```bash
git add src/astrolabe/hub.clj test/astrolabe/hub_test.clj
git commit -m "Add in-memory hub registry"
```

---

### Task 6: Delivery — send!, broadcast!, broadcast-each!, connect!

**Files:**
- Modify: `src/astrolabe/hub.clj`
- Test: `test/astrolabe/hub_test.clj`

**Interfaces:**
- Consumes: everything from Tasks 3–5.
- Produces:
  - `astrolabe.hub/frame` — `(frame hub events) → frame`, delegating to `sse/frame` with the hub's interpreter.
  - `astrolabe.hub/send!` — `(send! hub conn frame) → boolean`. Enqueues; **never writes**. Closes the connection if the queue refuses.
  - `astrolabe.hub/broadcast!` — `(broadcast! hub topic events) → nil`. Frames **once**.
  - `astrolabe.hub/broadcast-each!` — `(broadcast-each! hub topic f) → nil`, `f : conn → events`. Frames **per connection**.
  - `astrolabe.hub/connect!` — `(connect! hub topic opts?) → sse response data`, `opts` supports `:meta`.

- [ ] **Step 1: Write the failing test**

Append to `test/astrolabe/hub_test.clj`:

```clojure
(defn- drain-one
  "Take one frame from a connection's queue without blocking forever."
  [conn]
  (let [p (promise)]
    (Thread/startVirtualThread #(deliver p (queue/take! (:queue conn))))
    (deref p 1000 ::timeout)))

(deftest send!-enqueues-rather-than-writing
  (let [h (test-hub)
        c (test-conn :room)
        f (hub/frame h [[:remove-element "#x"]])]
    (hub/send! h c f)
    (is (empty? @(:!rec (:sse-gen c)))
        "send! must not touch the generator; the drain loop does the writing")
    (is (= f (drain-one c)) "the frame is waiting on the queue")))

(deftest broadcast!-renders-once-for-all-connections
  (let [renders (atom 0)
        itp (sse/interpreter {:render (fn [el] (swap! renders inc) (str el))})
        h (hub/in-memory {:interpreter itp})
        a (test-conn :room) b (test-conn :room) c (test-conn :room)]
    (doseq [conn [a b c]] (hub/subscribe! h :room conn))

    (hub/broadcast! h :room [[:patch-elements "<div/>"]])

    (is (= 1 @renders) "one render, no matter how many subscribers")
    (let [frames (map drain-one [a b c])]
      (is (apply = frames) "every connection got the identical frame"))))

(deftest broadcast-each!-renders-per-connection
  (let [renders (atom 0)
        itp (sse/interpreter {:render (fn [el] (swap! renders inc) (str el))})
        h (hub/in-memory {:interpreter itp})
        a (test-conn :room {:uid :a})
        b (test-conn :room {:uid :b})]
    (doseq [conn [a b]] (hub/subscribe! h :room conn))

    (hub/broadcast-each! h :room
                         (fn [conn] [[:patch-elements (str "<p>" (name (:uid (hub/meta conn))) "</p>")]]))

    (is (= 2 @renders) "one render per connection")
    (is (= [{:op :patch-elements :elements "<p>a</p>"}] (drain-one a)))
    (is (= [{:op :patch-elements :elements "<p>b</p>"}] (drain-one b)))))

(deftest a-refusing-queue-closes-its-connection
  (let [h (test-hub)
        full (hub/->Connection (adapter.test/->sse-recorder) (queue/bounded 1) :room {})
        ok   (test-conn :room)]
    (doseq [conn [full ok]] (hub/subscribe! h :room conn))

    (hub/broadcast! h :room [[:remove-element "#a"]])   ; fills `full`
    (hub/broadcast! h :room [[:remove-element "#b"]])   ; `full` refuses

    (is (= [ok] (hub/conns h :room))
        "the refusing connection is unsubscribed, the healthy one is untouched")))

(deftest broadcast!-is-not-stalled-by-a-slow-client
  (let [h (test-hub)
        slow (hub/->Connection (adapter.test/->sse-recorder) (queue/latest) :room {})
        fast (test-conn :room)]
    (doseq [conn [slow fast]] (hub/subscribe! h :room conn))
    ;; nobody is draining `slow`; broadcast must still return promptly
    (dotimes [_ 100] (hub/broadcast! h :room [[:remove-element "#x"]]))
    (is (= 2 (count (hub/conns h :room))) "a latest-queue never refuses, so nobody is dropped")))

(deftest connect!-holds-open-drains-and-cleans-up
  (let [h (test-hub)
        resp (hub/connect! h :room {:meta {:uid 7}})
        gen (adapter.test/->sse-recorder)
        done (promise)]
    (is (sse/response? resp))

    ;; connect! blocks, so run its on-open on its own virtual thread
    (let [t (Thread/startVirtualThread #(do ((:on-open resp) gen) (deliver done true)))]
      ;; wait for the subscription to appear
      (loop [n 0]
        (when (and (empty? (hub/conns h :room)) (< n 100))
          (Thread/sleep 10)
          (recur (inc n))))
      (let [conn (first (hub/conns h :room))]
        (is (some? conn) "connect! subscribed")
        (is (= {:uid 7} (hub/meta conn)) "connect! attached :meta")

        (hub/broadcast! h :room [[:remove-element "#x"]])
        (loop [n 0]
          (when (and (empty? @(:!rec gen)) (< n 100))
            (Thread/sleep 10)
            (recur (inc n))))
        (is (= 1 (count @(:!rec gen))) "the drain loop wrote the broadcast frame")

        (queue/close! (:queue conn)))

      (is (true? (deref done 1000 ::timeout)) "closing the queue ends the drain loop")
      (.join t)
      (is (= [] (hub/conns h :room)) "connect! unsubscribes on the way out"))))

(deftest a-custom-drain-loop-can-dedupe
  (let [h (hub/in-memory
           {:interpreter itp
            :queue-fn :latest
            :drain (fn [_conn q write!]
                     (loop [prev nil]
                       (when-let [f (queue/take! q)]
                         (when (not= f prev) (write! f))
                         (recur f))))})
        resp (hub/connect! h :room)
        gen (adapter.test/->sse-recorder)]
    (Thread/startVirtualThread #((:on-open resp) gen))
    (loop [n 0] (when (and (empty? (hub/conns h :room)) (< n 100)) (Thread/sleep 10) (recur (inc n))))
    (let [conn (first (hub/conns h :room))]
      (dotimes [_ 5]
        (hub/send! h conn (hub/frame h [[:remove-element "#same"]]))
        (Thread/sleep 20))
      (is (= 1 (count @(:!rec gen))) "identical frames are written once")
      (queue/close! (:queue conn)))))
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `bin/kaocha --focus astrolabe.hub-test`
Expected: FAIL — `hub/frame`, `hub/send!`, `hub/broadcast!` are not defined.

- [ ] **Step 3: Write the implementation**

Add to `src/astrolabe/hub.clj`:

```clojure
(defn frame
  "Normalize and render `events` once, producing a connection-independent frame."
  [hub events]
  (sse/frame (:interpreter hub) events))

(defn- close-conn!
  "Unsubscribe a connection and close its queue, ending its drain loop."
  [hub conn]
  (-unsubscribe! hub (:topic conn) conn)
  (queue/close! (:queue conn))
  nil)

(defn send!
  "Enqueue one frame for one connection. Never writes and never blocks. If the
  connection's queue refuses the frame, the connection is closed."
  [hub conn frame]
  (let [accepted (boolean (queue/offer! (:queue conn) frame))]
    (when-not accepted
      (close-conn! hub conn))
    accepted))

(defn broadcast!
  "Render `events` once and send the resulting frame to every connection on
  `topic`. Use when every client sees the same HTML."
  [hub topic events]
  (let [f (frame hub events)]
    (doseq [conn (conns hub topic)]
      (send! hub conn f)))
  nil)

(defn broadcast-each!
  "Call `f` with each connection on `topic` and send that connection its own
  frame. Use when clients see different HTML."
  [hub topic f]
  (doseq [conn (conns hub topic)]
    (send! hub conn (frame hub (f conn))))
  nil)

(defn connect!
  "Return SSE response data that subscribes to `topic`, holds the connection
  open by draining its queue, and unsubscribes when the client disconnects.

  Opts:
  - `:meta` app data attached to the connection, readable with [[meta]]"
  ([hub topic] (connect! hub topic {}))
  ([hub topic {m :meta}]
   (sse/response
    {:on-open
     (fn [sse-gen]
       ;; the queue-fn sees the connection, so build it in two steps
       (let [proto (->Connection sse-gen nil topic m)
             q     ((:queue-fn hub) proto)
             conn  (assoc proto :queue q)
             write! (fn [frame] (sse/apply! (:interpreter hub) sse-gen frame))]
         (-subscribe! hub topic conn)
         (try
           ((:drain hub) conn q write!)
           (catch Exception _
             nil)   ; a dead client is normal; fall through to cleanup
           (finally
             (-unsubscribe! hub topic conn)
             (queue/close! q)))))})))
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `bin/kaocha --focus astrolabe.hub-test`
Expected: PASS, 12 tests.

- [ ] **Step 5: Run the whole suite**

Run: `bin/kaocha`
Expected: PASS. Watch for hangs — a hanging suite means a drain loop is not
being woken by `queue/close!`; revisit Task 4's `close!` implementations.

- [ ] **Step 6: Commit**

```bash
git add src/astrolabe/hub.clj test/astrolabe/hub_test.clj
git commit -m "Add frame delivery, broadcasting, and connect!"
```

---

### Task 7: Content-encoding negotiation

**Files:**
- Create: `src/astrolabe/compression.clj`
- Test: `test/astrolabe/compression_test.clj`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `astrolabe.compression/parse-accept-encoding` — `(parse-accept-encoding header) → {coding q-value}` with `coding` a lowercase string and `q` a double.
  - `astrolabe.compression/negotiate` — `(negotiate compression header) → write-profile or nil`. `compression` is an ordered vector of `[coding profile]`; returns the first acceptable profile, else nil (meaning uncompressed).
  - `astrolabe.compression/default-compression` — `[["gzip" ac/gzip-profile]]`.
  - When `negotiate` returns nil the middleware sets no `write-profile` at all,
    so the SDK adapter falls back to its own default (`basic-profile`,
    uncompressed). That is the intended "identity" branch — do not invent a
    profile for it. The SDK's buffered variants (`buffered-writer-profile`,
    `gzip-buffered-writer-profile`) are a reasonable future option for held-open
    connections; they are out of scope here, and a user can already select one
    per response via `:write-profile`.

- [ ] **Step 1: Write the failing test**

Create `test/astrolabe/compression_test.clj`:

```clojure
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
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `bin/kaocha --focus astrolabe.compression-test`
Expected: FAIL — `astrolabe.compression` does not exist.

- [ ] **Step 3: Write the implementation**

Create `src/astrolabe/compression.clj`:

```clojure
(ns astrolabe.compression
  "Content-encoding negotiation. The SDK does none of this: it sets
  Content-Encoding from whatever write profile it is handed."
  (:require [clojure.string :as str]
            [starfederation.datastar.clojure.adapter.common :as ac]))

(def default-compression
  "gzip only, which needs no dependency beyond the SDK core."
  [["gzip" ac/gzip-profile]])

(defn- parse-q [params]
  (or (some (fn [p]
              (let [[k v] (str/split (str/trim p) #"=" 2)]
                (when (and (= "q" (str/lower-case k)) v)
                  (parse-double (str/trim v)))))
            params)
      1.0))

(defn parse-accept-encoding
  "Parse an Accept-Encoding header into {coding q-value}."
  [header]
  (if (str/blank? header)
    {}
    (into {}
          (keep (fn [part]
                  (let [[coding & params] (str/split part #";")
                        coding (str/lower-case (str/trim coding))]
                    (when-not (str/blank? coding)
                      [coding (parse-q params)]))))
          (str/split header #","))))

(defn negotiate
  "Return the first write profile in `compression` whose coding the client
  accepts, or nil for uncompressed. `compression` is an ordered vector of
  [coding write-profile], server preference first."
  [compression header]
  (let [accepted (parse-accept-encoding header)
        wildcard (get accepted "*")]
    (some (fn [[coding profile]]
            (let [q (get accepted coding wildcard)]
              (when (and q (pos? q)) profile)))
          compression)))
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `bin/kaocha --focus astrolabe.compression-test`
Expected: PASS, 2 tests.

- [ ] **Step 5: Commit**

```bash
git add src/astrolabe/compression.clj test/astrolabe/compression_test.clj
git commit -m "Add Accept-Encoding negotiation"
```

---

### Task 8: Reitit middleware

**Files:**
- Create: `src/astrolabe/reitit.clj`
- Test: `test/astrolabe/reitit_test.clj`
- Modify: `deps.edn` (add reitit to the `:test` alias only)

**Interfaces:**
- Consumes: `astrolabe.sse` (Task 3), `astrolabe.compression` (Task 7).
- Produces: `astrolabe.reitit/middleware` — `(middleware {:->sse-response f :parse-json g :interpreter itp :compression c}) → reitit middleware map`. Applies only to routes with `:datastar true` (via `:compile`). Parses signals onto `:signals`; converts `sse/response` data into a streaming response.
- Prior art: this task is the closest to `hindsight/datastar.clj`. Take its
  `:compile` shape and its signals reader; leave behind its `:gzip?`/`:buffer?`
  flags (replaced by negotiation) and its `:connection` key (replaced by the hub).

- [ ] **Step 1: Add reitit to the test alias**

`astrolabe.reitit` returns a plain map and must not require reitit. Tests need
the real router to prove the `:compile` wiring, so add it under `:test` only:

```clojure
metosin/reitit-ring {:mvn/version "0.7.2"}
```

- [ ] **Step 2: Write the failing test**

Create `test/astrolabe/reitit_test.clj`:

```clojure
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
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `bin/kaocha --focus astrolabe.reitit-test`
Expected: FAIL — `astrolabe.reitit` does not exist.

- [ ] **Step 4: Write the implementation**

Create `src/astrolabe/reitit.clj`:

```clojure
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
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `bin/kaocha --focus astrolabe.reitit-test`
Expected: PASS, 7 tests.

- [ ] **Step 6: Commit**

```bash
git add deps.edn src/astrolabe/reitit.clj test/astrolabe/reitit_test.clj
git commit -m "Add reitit middleware for signals, SSE responses, and compression"
```

---

### Task 9: Optional Brotli module

**Files:**
- Create: `src/astrolabe/brotli.clj`
- Test: `test/astrolabe/brotli_test.clj`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `astrolabe.brotli/profile` — `(profile) → write-profile` and `(profile {:quality q :window-size w}) → write-profile`. Defaults `{:quality 4 :window-size 20}`.

- [ ] **Step 1: Verify no other namespace references brotli**

Run: `grep -rn "clojure.brotli" src/ | grep -v "^src/astrolabe/brotli.clj"`
Expected: no output. Any hit is a Global Constraint violation — loading that
namespace throws where the natives are absent.

- [ ] **Step 2: Write the failing test**

Create `test/astrolabe/brotli_test.clj`:

```clojure
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
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `bin/kaocha --focus astrolabe.brotli-test`
Expected: FAIL — `astrolabe.brotli` does not exist.

- [ ] **Step 4: Write the implementation**

Create `src/astrolabe/brotli.clj`:

```clojure
(ns astrolabe.brotli
  "Optional Brotli support. Requiring this namespace loads brotli4j and its
  native library; nothing else in astrolabe may reference it."
  (:require [starfederation.datastar.clojure.brotli :as brotli]))

(def default-opts
  "Lower than the SDK module's own defaults (quality 5, window 24) because SSE
  compression costs are paid per live connection rather than once per asset."
  {:quality 4 :window-size 20})

(defn profile
  "A Brotli write profile tuned for SSE."
  ([] (profile nil))
  ([opts] (brotli/->brotli-profile (merge default-opts opts))))
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `bin/kaocha --focus astrolabe.brotli-test`
Expected: PASS, 2 tests. If this fails with an `UnsatisfiedLinkError` or
`Brotli4jLoader` error, the native classifier is missing from the `:test` alias
— add `com.aayushatharva.brotli4j/native-osx-aarch64` (or the matching
platform) rather than working around it in code.

- [ ] **Step 6: Run the whole suite**

Run: `bin/kaocha`
Expected: PASS, all namespaces.

- [ ] **Step 7: Commit**

```bash
git add src/astrolabe/brotli.clj test/astrolabe/brotli_test.clj
git commit -m "Add optional Brotli write profile"
```

---

## Verification

After Task 9, confirm the library matches the README end to end:

- [ ] `bin/kaocha` passes with every namespace green.
- [ ] `grep -rn "clojure.brotli" src/ | grep -v brotli.clj` returns nothing.
- [ ] Every code example in `README.md` uses only functions that now exist, with
      matching arities: `sse/interpreter`, `sse/response`, `hub/in-memory`,
      `hub/connect!`, `hub/broadcast!`, `hub/broadcast-each!`, `hub/frame`,
      `hub/send!`, `hub/conns`, `hub/meta`, `queue/bounded`, `queue/latest`,
      `queue/take!`, `astrolabe/middleware`, `astrolabe.brotli/profile`.
- [ ] The README's dedupe drain loop and the state-based ticker compile when
      pasted into a REPL against this code.
