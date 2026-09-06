# astrolabe

An idiomatic [Datastar](https://data-star.dev) backend for Clojure, built as a
**companion to the official [`datastar-clojure`](https://github.com/starfederation/datastar-clojure)
SDK** — not a replacement for it.

`astrolabe` adds the ergonomic and integration layer the SDK deliberately leaves
out: a data-first event API, first-class [reitit](https://github.com/metosin/reitit)
wiring, Brotli compression on by default, and a small connection registry for
broadcasting to many clients — while staying agnostic about your web server and
owning none of your application state.

> **Status: alpha.** API is expected to change.

## Why

The official SDK is excellent at the wire protocol and is already modular
(separate `ring` / `http-kit` / `aleph` adapters, a `brotli` write-profile). But
using it directly leaves you writing the same glue every project needs:

- Imperative `patch-elements!` / `patch-signals!` bang-calls against a mutable
  `sse-gen`, instead of describing what to send as data.
- Translating friendly options into the SDK's opaque constant keys by hand.
- Parsing Datastar signals off the request yourself (the SDK gives you the raw
  value but no JSON step — by design).
- Holding SSE connections open and fanning updates out to more than one client.
- Remembering to turn compression on at all.

`astrolabe` packages those patterns. It is a thin layer: events are interpreted
straight onto the SDK's generator, and compression is just the SDK's own
`brotli` write-profile, switched on by default.

### Non-goals

`astrolabe` deliberately does **not**:

- Replace your router — it plugs into reitit.
- Lock in a web server — you pass it whichever SDK adapter's `->sse-response`
  you want (Ring/Jetty, http-kit, Aleph, …).
- Own your application state or prescribe a render loop. Broadcasting is a small
  primitive; the "re-render everything from one atom" model is a
  [documented recipe](#state-based-recipe), not framework machinery.

## Install

`astrolabe` requires **Java 21 or later** (see
[Delivery & concurrency](#delivery--concurrency)). It builds on the SDK core and
one of its server adapters — pick the adapter that matches your server:

```clojure
;; deps.edn
{:deps {com.joshuadavey/astrolabe  {:mvn/version "0.1.0"}
        dev.data-star.clojure/ring {:mvn/version "1.0.0-RC11"}}}  ; or /http-kit, /aleph
```

That's enough for gzip, which ships in the SDK core. Brotli is optional — add
the module only if you want it:

```clojure
dev.data-star.clojure/brotli {:mvn/version "1.0.0-RC11"}
```

Brotli uses the native `brotli4j` library, so it also needs a classifier
matching each platform you run on — `native-linux-x86_64` for a typical Linux
deploy, plus `native-osx-aarch64` or `native-osx-x86_64` for local development.
See [Opting into Brotli](#opting-into-brotli).

## Quick start

Add the middleware to your reitit router's `:data`, then flag Datastar routes
with `:datastar true`.

```clojure
(ns example.handler
  (:require [reitit.ring :as ring]
            [jsonista.core :as json]
            [astrolabe.reitit :as astrolabe]
            [astrolabe.sse :as sse]
            [starfederation.datastar.clojure.adapter.ring :as adapter.ring]))

(defn- read-json [s]
  (json/read-value s json/keyword-keys-object-mapper))

(def interpreter
  (sse/interpreter {:render     my-hiccup->html
                    :write-json json/write-value-as-string}))

(def router
  (ring/router
   [["/greet" {:datastar true
               :post (fn [{:keys [signals]}]
                       (sse/response
                        {:events [[:patch-elements [:div#greeting (str "Hi, " (:name signals))]]
                                  [:patch-signals {:name ""}]]}))}]]
   {:data {:middleware [(astrolabe/middleware
                          {:->sse-response adapter.ring/->sse-response
                           :parse-json     read-json
                           :interpreter    interpreter})]}}))
```

Three things to note:

- The handler **returns data** (`sse/response`), not side effects. The
  middleware turns that value into a streaming SSE response.
- An **interpreter** carries everything needed to turn event data into bytes:
  `:render` (see [Rendering](#rendering)) and `:write-json`. Build one and share
  it between the middleware and any hubs, so rendering is configured once.
- JSON is yours in both directions — the SDK is library-agnostic and so is
  `astrolabe`. `:parse-json` reads signals off the request; `:write-json`
  serializes `:patch-signals` maps. A `:patch-signals` value that is already a
  string passes through untouched, so `:write-json` is only required if you use
  the map form.

Incoming Datastar signals are parsed onto `:signals` on the request for any
route flagged `:datastar true`.

### Rendering

`:render` turns whatever you put in an event's `:elements` into an HTML string.
`astrolabe` never requires a particular library — you pass the function. Two
choices dominate in Clojure, and both take the same hiccup-style vectors:

```clojure
;; hiccup — the long-standing standard
(require '[hiccup2.core :as h])
(defn render [el] (str (h/html el)))

;; chassis — newer, substantially faster
(require '[dev.onionpancakes.chassis.core :as c])
(def render c/html)
```

Neither is a dependency of `astrolabe`; add whichever you use to your own
`deps.edn`. Chassis additionally offers `dev.onionpancakes.chassis.compiler/compile`,
which folds constant parts of a view into string literals at macroexpansion —
worth reaching for in hot views, and invisible to `astrolabe` either way.

Which to pick depends on how much HTML you generate per second. Request/response
handlers render a patch or two per interaction, and render time is noise next to
the network — use whichever your codebase already speaks, which for most
existing projects is hiccup. The [state-based recipe](#state-based-recipe) is
the opposite case: it re-renders the entire view for every connected client many
times a second, so rendering sits squarely on the hot path and `chassis` is the
better default there.

If your `:elements` are already HTML strings, pass `str`.

## Event model

Every event has a **canonical map form** keyed by `:op`. Vectors are accepted as
sugar and normalized into that map before anything else happens, so the
interpreter only ever sees maps — and the map form is exactly what you'd
validate against the SDK's malli schemas.

```clojure
;; these two are identical
[:patch-elements el {:mode :append :selector "#lane-0"}]
{:op :patch-elements :elements el :mode :append :selector "#lane-0"}
```

The vector grammar is uniform: `[op primary-arg opts?]`, where `op` selects the
primary argument's canonical key. A trailing non-map, an unknown `op`, or a
missing primary arg throws.

| `op`                  | primary key | primary value              | common opts |
|-----------------------|-------------|----------------------------|-------------|
| `:patch-elements`     | `:elements` | hiccup or HTML string      | `:selector` `:mode` `:use-view-transition?` `:retry-duration` `:element-ns` |
| `:patch-elements-seq` | `:elements` | seq of the above           | (as above) |
| `:patch-signals`      | `:signals`  | a map, or a JSON string    | `:only-if-missing?` |
| `:remove-element`     | `:selector` | CSS selector string        | |
| `:execute-script`     | `:script`   | JavaScript string          | `:auto-remove?` `:attributes` |

`:mode` accepts friendly keywords — `:outer` `:inner` `:append` `:prepend`
`:before` `:after` `:remove` `:replace` — mapped onto the SDK's patch-mode
constants for you. Every op also accepts `:id` (the SSE event id).

A `:patch-signals` map is serialized with the interpreter's `:write-json`; a
string is sent as-is. Passing a map with no `:write-json` configured throws.

## Reading signals

For any `:datastar true` route, `astrolabe` reads the Datastar payload (the
`datastar` query param on GET/DELETE, the JSON body otherwise) through your
`:parse-json` and places it on the request as `:signals`:

```clojure
(fn [{:keys [signals]}]
  (let [title (:cardTitle signals)]
    ...))
```

## Broadcasting

For updates that must reach clients other than the one making the request, use a
**hub**: a topic-keyed registry of open connections. Create one hub, sharing the
same interpreter the middleware uses:

```clojure
(require '[astrolabe.hub :as hub]
         '[astrolabe.sse :as sse])

(def interpreter (sse/interpreter {:render     my-hiccup->html
                                  :write-json json/write-value-as-string}))
(def hub         (hub/in-memory {:interpreter interpreter}))
```

A long-lived connection subscribes to a topic and stays open until the client
disconnects:

```clojure
["/boards/:id/sse" {:datastar true
                    :post (fn [{:keys [path-params]}]
                            (hub/connect! hub (:id path-params)))}]
```

A mutation changes state, broadcasts the specific patch to everyone on the
topic, and can still reply to the caller on its own stream:

```clojure
["/boards/:id/cards" {:datastar true
                      :post (fn [{:keys [path-params signals]}]
                              (let [card (cards/create! (:id path-params) signals)]
                                (hub/broadcast! hub (:id path-params)
                                                [[:patch-elements (views/card card)
                                                  {:mode :append :selector "#lane-0"}]])
                                (sse/response
                                 {:events [[:patch-signals {:cardTitle ""}]]})))}]
```

`broadcast!` accepts the same event data as `sse/response`, so vector sugar and
canonical maps both work.

### Two broadcast paths

Sending to many connections raises a question a single response never does:
*when does rendering happen?* `astrolabe` surfaces both answers as separate
functions rather than choosing for you.

A **frame** is the seam. `frame` normalizes event data and runs `:render`,
producing a payload whose `:elements` are already HTML strings and which depends
on no particular connection. `send!` writes one frame to one connection.

```clojure
(hub/frame hub events)      ; events → frame  (normalize + render)
(hub/send! hub conn frame)  ; enqueue one frame for one connection
```

Everything else is those two composed:

| call | renders | use when |
|------|---------|----------|
| `(hub/broadcast! hub topic events)` | once, shared by all | every client sees the same HTML |
| `(hub/broadcast-each! hub topic f)` | once per connection   | clients see different HTML |

`broadcast!` frames once and reuses that frame for every connection, so
`:render` runs a single time no matter how many clients are listening.
`broadcast-each!` takes `f`, a function of one connection, and frames its return
value separately for each — the cost of personalization, paid only when you ask
for it.

`send!` accepts frames only, never raw events. That's deliberate: allowing
events there would hide whether a render is shared or per-client at the one call
site where the distinction costs something. Call `frame` yourself.

`send!` enqueues rather than writes, so neither broadcast blocks on a slow
client — see [Delivery & concurrency](#delivery--concurrency). A connection
that has gone away is unsubscribed by its own drain loop; the fan-out never
sees the failure, and one dead client cannot abort a broadcast.

### Connection metadata

`broadcast-each!` needs something to personalize *on*, so `connect!` takes
app-supplied metadata that rides along with the connection:

```clojure
(hub/connect! hub :app {:meta {:uid uid}})

(hub/broadcast-each! hub :app
  (fn [conn] (views/board @!state (:uid (hub/meta conn)))))
```

### Hub API

```clojure
(hub/frame           hub events)       ; events → frame (normalize + render)
(hub/send!           hub conn frame)   ; enqueue one frame for one connection
(hub/broadcast!      hub topic events) ; render once; send to every conn on topic
(hub/broadcast-each! hub topic f)      ; f : conn → events; render per connection
(hub/subscribe!      hub topic conn)   ; register; returns a promise realized on disconnect
(hub/unsubscribe!    hub topic conn)
(hub/conns           hub topic)        ; current connections on topic
(hub/meta            conn)             ; app data supplied at connect!
(hub/connect!        hub topic opts?)  ; a datastar response that holds open + (un)subscribes
```

`in-memory` is single-node: connections live in one process and are lost on
restart (browsers reconnect via Datastar's SSE retry). The `Hub` protocol is the
seam for a future multi-node backend (Redis, Postgres `LISTEN`/`NOTIFY`).

## State-based recipe

`astrolabe`'s hub is **event-based**: you decide what changed and broadcast that
patch. An alternative, popularized by [hyperlith](https://github.com/andersmurphy/hyperlith),
is **state-based**: hold one source of truth, and on every change re-render the
whole view for every client, letting Datastar's morph diff it on the front end.
Brotli's streaming compression makes "re-send everything" cheap, and reconnects
self-heal because a fresh connection simply renders current state.

`astrolabe` does not ship this as machinery — it's a few lines composed from the
primitives above:

```clojure
(defonce !state (atom initial))

;; Each connection carries the user it belongs to. Nothing else to track.
["/app/sse" {:datastar true
             :post (fn [{:keys [uid]}]
                     (hub/connect! hub :app {:meta {:uid uid}}))}]

;; Mutations just move state; they do NOT broadcast.
["/app/toggle/:id" {:datastar true
                    :post (fn [{:keys [path-params]}]
                            (swap! !state toggle (:id path-params))
                            {:status 204})}]

;; A state-based hub coalesces: a superseded snapshot is worthless.
(def hub (hub/in-memory {:interpreter interpreter :queue-fn :latest}))

;; One ticker re-renders current state for every connection, ~10x/sec.
(defonce ticker
  (.start (Thread/ofVirtual)
          #(loop []
             (let [state @!state]
               (hub/broadcast-each! hub :app
                                    (fn [conn]
                                      (views/app state (:uid (hub/meta conn))))))
             (Thread/sleep 100)
             (recur))))
```

This uses `broadcast-each!` because each user sees their own view. If every
client would see identical HTML, `broadcast!` renders once per tick instead of
once per client — a difference you feel at a few hundred connections.

The two refinements production state-based apps want are both delivery concerns,
so neither is app code: dropping frames for slow clients is `:queue-fn :latest`
above, and deduping unchanged renders is a
[custom drain loop](#the-drain-loop). Both are covered under
[Delivery & concurrency](#delivery--concurrency).

**Tradeoff:** state-based is the simplest mental model (no missed-event log,
reconnects self-heal) but pays the cost of a constant re-render/fan-out loop.
Event-based does less work and integrates with ordinary request/response, but
your handlers own the correctness of partial updates. You can mix both on
different topics.

## Compression

Compression is **on by default** and content-negotiated. The SDK does no
negotiation of its own — `api.sse/headers` sets `Content-Encoding` from whatever
write profile you hand it and never reads `Accept-Encoding` — so choosing an
encoding per request is `astrolabe`'s job.

`:compression` is an ordered vector of `[content-coding write-profile]` pairs,
server preference first. `astrolabe` picks the first coding the client's
`Accept-Encoding` accepts (treating `q=0` as a refusal) and falls back to
uncompressed when none match.

The default needs no extra dependency, because gzip ships in the SDK core:

```clojure
;; the default, if you say nothing
{:compression [["gzip" common/gzip-profile]]}
```

### Opting into Brotli

Brotli lives in a separate artifact and is **opt-in**. It has to be: the SDK's
`brotli` namespace calls `Brotli4jLoader/ensureAvailability` at load time, which
throws when the native library is absent. `astrolabe` therefore never references
that namespace — not directly, and not by classpath detection, which would
trigger the same load. You require it, you pass the profile:

```clojure
(require '[astrolabe.brotli :as astrolabe.brotli]
         '[starfederation.datastar.clojure.adapter.common :as common])

(astrolabe/middleware
 {:->sse-response adapter.ring/->sse-response
  :parse-json     read-json
  :interpreter    interpreter
  :compression    [["br"   (astrolabe.brotli/profile)]
                   ["gzip" common/gzip-profile]]})
```

`astrolabe.brotli` is a thin optional namespace over the SDK's `->brotli-profile`;
it exists only to carry SSE-tuned defaults. Requiring it is what loads brotli4j,
so a project that never requires it never needs the native library.

### Tuning

Brotli over SSE is a per-connection **streaming** compressor: one encoder stays
open for the whole connection, so the highly repetitive HTML across re-renders
compresses extremely well (ratios in the 100:1+ range are common). The window
size is therefore a per-connection memory cost, and quality a per-connection CPU
cost. `astrolabe.brotli/profile` defaults to `{:quality 4 :window-size 20}`, below
the SDK module's own defaults of quality 5 and window 24, because those costs
are paid per live connection rather than once per asset.

Override per response with an explicit `:write-profile`, which bypasses
negotiation entirely:

```clojure
(require '[starfederation.datastar.clojure.brotli :as brotli])

(sse/response {:events [...]
               :write-profile (brotli/->brotli-profile {:quality 6 :window-size 22})})
```

Because compression state is per connection, compressed bytes are never shared
between connections — even when the rendered HTML is identical. That's inherent
to streaming compression, and it's the right tradeoff for the ratios it buys.

## Delivery & concurrency

`astrolabe` requires **Java 21 or later** and uses virtual threads throughout. This
is a hard requirement, not a recommendation — one virtual thread per open
connection is the whole delivery model, and it is what makes blocking writes
safe to hold at scale.

Every connection has a **queue** and a **drain loop**. `send!` does not write; it
offers a frame to the connection's queue and returns immediately. A separate
virtual thread takes frames off that queue and does the blocking write. So a
slow client can never stall `broadcast!` — the fan-out is a series of enqueues,
and each connection absorbs its own backpressure.

Hold-open and delivery are the same loop. `connect!` subscribes, then drains
until the client disconnects:

```clojure
;; connect!, in essence
(subscribe! hub topic conn)
(try
  (drain conn queue write!)   ; blocks until the queue closes
  (finally (unsubscribe! hub topic conn)))
```

On synchronous adapters this borrows the request's own virtual thread, so no
extra thread exists per connection. On async adapters (http-kit, Aleph) there is
no request thread to borrow and `connect!` spawns one. The API and the semantics
are identical either way.

### Adapters and held connections

`connect!` works on every adapter, including synchronous Ring/Jetty. The
mechanism is worth knowing, because it is easy to assume otherwise: the ring
adapter's response body is a `StreamableResponseBody`, and its
`write-body-to-stream` calls `on-open` as its last act. Ring's Jetty adapter
invokes that *after* your handler has returned, while streaming the body — so
blocking in `on-open` blocks `write-body-to-stream`, and that is precisely what
holds the stream open. The stream lives as long as `on-open` blocks, not as long
as the handler runs.

The cost is thread occupancy: on a synchronous adapter each held connection
occupies a container thread for its lifetime. Run Jetty's thread pool on virtual
threads (Jetty 12 exposes a virtual-thread executor, and `ring-jetty-adapter`
accepts a `:thread-pool`) or the number of concurrent SSE connections you can
hold is capped by pool size. Async adapters (http-kit, Aleph) don't have this
constraint, since there is no container thread to hold.

The request/response style — `sse/response` with `:auto-close? true`, the
default — is unaffected either way, on any adapter.

### Queues

A queue is anything satisfying `astrolabe.queue/Queue`:

```clojure
(defprotocol Queue
  (offer! [q frame] "Accept a frame. Returns false if the connection should close.")
  (take!  [q]       "Block until a frame is available. nil means closed.")
  (close! [q]))
```

Overflow policy is not a setting — it is what `offer!` returns. A queue that
refuses a frame is asking for its connection to be dropped; one that coalesces
simply never refuses. Three built-ins:

| queue | on a full queue | for |
|-------|-----------------|-----|
| `(queue/bounded n)` | refuses → connection closes → client reconnects and resyncs | event-based |
| `(queue/latest)` | newest frame replaces the pending one | state-based |
| `(queue/unbounded)` | never refuses; grows without limit | testing |

The choice follows from what a frame *means*. Event-based frames are deltas, so
silently dropping one leaves that client permanently diverged with no way to
notice — closing the connection is the honest response, because Datastar
reconnects and a fresh connection re-renders current state. State-based frames
are complete snapshots, so a superseded frame is worthless and coalescing is
strictly better than buffering.

### Configuring the hub

`:queue-fn` and `:drain` live on the hub, so every connection on it shares one
delivery policy. Both receive the connection, which carries `:topic` and
`:meta`, so policy can still vary where that's meaningful:

```clojure
(def hub
  (hub/in-memory
   {:interpreter interpreter
    :queue-fn    :bounded}))       ; keyword sugar for (fn [_] (queue/bounded 64))
```

```clojure
;; varying by topic, when one hub serves both models
{:queue-fn (fn [{:keys [topic]}]
             (if (= topic :app)
               (queue/latest)
               (queue/bounded 64)))}
```

`:queue-fn` accepts a keyword (`:bounded` `:latest` `:unbounded`, at their
defaults) or a function of one connection returning a queue. It is called once
per connection.

### The drain loop

`:drain` is a function of `[conn q write!]`. The default takes frames and writes
them until the queue closes. Replacing it is how you get behavior the library
doesn't ship — dedupe, for instance, is three lines:

```clojure
{:drain (fn [_conn q write!]
          (loop [prev nil]
            (when-let [frame (queue/take! q)]
              (when (not= frame prev) (write! frame))
              (recur frame))))}
```

Because frames are plain values, `=` is the whole test. Batching — drain
everything pending, write once — is the other natural one.

A custom drain loop owns its own error handling. The default catches write
failures, closes the connection and unsubscribes it; a replacement that doesn't
will leak subscriptions when a client disappears mid-write.

## Prior art & thanks

- [`datastar-clojure`](https://github.com/starfederation/datastar-clojure) — the
  official SDK `astrolabe` builds on.
- [hyperlith](https://github.com/andersmurphy/hyperlith) by Anders Murphy — the
  source of the streaming-Brotli-over-SSE technique and the state-based model.

## License

TBD.
