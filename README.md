# astrolabe

An idiomatic [Datastar](https://data-star.dev) backend for Clojure, built as a
**companion to the official [`datastar-clojure`](https://github.com/starfederation/datastar-clojure)
SDK** — not a replacement for it.

`astrolabe` adds the ergonomic and integration layer the SDK deliberately leaves
out: a data-first event API, first-class [reitit](https://github.com/metosin/reitit)
wiring, content-negotiated compression (gzip by default, Brotli opt-in), and a
small connection registry for broadcasting to many clients — while staying
agnostic about your web server and owning none of your application state.

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
straight onto the SDK's generator, and compression is just the SDK's own write
profiles — `gzip` by default, since it ships in the SDK core, with `brotli`
available opt-in — chosen per request from the client's `Accept-Encoding`.

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
| `:patch-elements`     | `:elements` | hiccup or HTML string      | `:selector` `:mode` `:use-view-transition?` `:view-transition-selector` `:retry-duration` `:element-ns` |
| `:patch-elements-seq` | `:elements` | seq of the above           | (as above) |
| `:patch-signals`      | `:signals`  | a map, or a JSON string    | `:only-if-missing?` |
| `:remove-element`     | `:selector` | CSS selector string        | |
| `:execute-script`     | `:script`   | JavaScript string          | `:auto-remove?` `:attributes` |

`:mode` accepts friendly keywords — `:outer` `:inner` `:append` `:prepend`
`:before` `:after` `:remove` `:replace` — mapped onto the SDK's patch-mode
constants for you. Every op also accepts `:id` (the SSE event id).
`:view-transition-selector` is applied by Datastar only when
`:use-view-transition?` is true.

Only the options in the table above are translated; every other key on an event
map is ignored rather than rejected, so you can carry your own data alongside an
event. The tradeoff is that a *misspelled* option key is dropped silently — an
unknown `op` or an unknown enum value still throws.

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

On GET and DELETE, Datastar sends signals as the `datastar` **query parameter**,
and `astrolabe` reads them from `[:query-params "datastar"]` — the key ring's
`wrap-params` (or reitit's parameters middleware) populates. Without one of
those in your middleware chain, a GET Datastar route sees no signals at all, and
does so silently. POST and the other methods read the body and are unaffected.

## Broadcasting

For updates that must reach clients other than the one making the request you
need two things: a **hub**, a topic-keyed registry of open connections, and a
**connector**, which knows how to build and run one connection. They are
separate because only the hub varies — an atom today, a database later — while
the connection machinery is the same either way.

```clojure
(require '[astrolabe.hub :as hub]
         '[astrolabe.connection :as conn]
         '[astrolabe.sse :as sse])

(def interpreter (sse/interpreter {:render     my-hiccup->html
                                   :write-json json/write-value-as-string}))
(def hub         (hub/in-memory))
(def connector   (conn/connector))
```

A long-lived connection subscribes to a topic and stays open until the client
disconnects:

```clojure
["/boards/:id/sse" {:datastar true
                    :post (fn [{:keys [path-params]}]
                            (hub/connect! hub connector (:id path-params)))}]
```

A mutation changes state, broadcasts the specific patch to everyone on the
topic, and can still reply to the caller on its own stream:

```clojure
["/boards/:id/cards" {:datastar true
                      :post (fn [{:keys [path-params signals]}]
                              (let [card (cards/create! (:id path-params) signals)]
                                (hub/broadcast! hub (:id path-params)
                                                (sse/frame interpreter
                                                           [[:patch-elements (views/card card)
                                                             {:mode :append :selector "#lane-0"}]]))
                                (sse/response
                                 {:events [[:patch-signals {:cardTitle ""}]]})))}]
```

`broadcast!` takes an already-rendered **frame**, not events — `sse/frame`
accepts the same event data as `sse/response`, so vector sugar and canonical
maps both work there.

### Two broadcast paths

Sending to many connections raises a question a single response never does:
*when does rendering happen?* `astrolabe` surfaces both answers as separate
functions rather than choosing for you.

A **frame** is the seam. `sse/frame` normalizes event data and runs `:render`,
producing a payload whose `:elements` are already HTML strings and which depends
on no particular connection. `send!` enqueues one frame for one connection.

```clojure
(sse/frame interpreter events)  ; events → frame  (normalize + render)
(hub/send! hub conn frame)      ; enqueue one frame for one connection
```

Everything else is those two composed:

| call | renders | use when |
|------|---------|----------|
| `(hub/broadcast! hub topic frame)` | once, shared by all | every client sees the same HTML |
| `(hub/broadcast-each! hub topic f)` | once per connection | clients see different HTML |

`broadcast!` takes one frame and hands it to every connection, so `:render` ran
a single time no matter how many clients are listening. `broadcast-each!` takes
`f`, a function of one connection returning that connection's own frame — the
cost of personalization, paid only when you ask for it.

Both take frames, never raw events. That's deliberate: accepting events would
hide whether a render is shared or per-client at the two call sites where the
distinction costs something. Render with `sse/frame` yourself, and where it
sits in your code says which you chose.

`send!` enqueues rather than writes, so neither broadcast blocks on a slow
client — see [Delivery & concurrency](#delivery--concurrency). A connection
that has gone away is unsubscribed on its own thread, by the `connect!` that is
holding it open; the fan-out never sees the failure, and one dead client cannot
abort a broadcast.

That guarantee is about *delivery*. `broadcast-each!` still calls your `f` and
renders inline, so an exception thrown while rendering for one connection does
propagate out of `broadcast-each!` and skips the connections after it. If a
per-connection render can fail, catch inside `f`.

### Connection data

`broadcast-each!` needs something to personalize *on*, so `connect!` takes
app-supplied data that rides along with the connection:

```clojure
(hub/connect! hub connector :app {:data {:uid uid}})

(hub/broadcast-each! hub :app
  (fn [conn]
    (sse/frame interpreter (views/board @!state (:uid (conn/data conn))))))
```

`connect!` also accepts `:on-close` and `:on-exception`, passed straight through
to the SDK adapter as the corresponding callbacks.

### Hub API

```clojure
;; the hub: storage, and the glue that uses it
(hub/subscribe!      hub topic conn)   ; register a connection on a topic
(hub/unsubscribe!    hub topic conn)
(hub/conns           hub topic)        ; current connections on topic
(hub/send!           hub conn frame)   ; enqueue one frame for one connection
(hub/broadcast!      hub topic frame)  ; send one frame to every conn on topic
(hub/broadcast-each! hub topic f)      ; f : conn → frame; one render per connection
(hub/connect!        hub connector topic opts?)  ; a datastar response that holds open

;; connections
(conn/connector      opts?)            ; build the default connector
(conn/data           conn)             ; app data supplied at connect!
```

Only the first three are the `Hub` protocol. `send!`, `broadcast!`,
`broadcast-each!` and `connect!` are plain functions written against it, so a
new backend gets them for free.

`connect!` owns the connection lifecycle: it spawns a connection with the
connector, subscribes it, drains it, and unsubscribes on the way out.
`subscribe!`/`unsubscribe!` are the raw registry operations underneath it —
reach for them only when you are managing a connection's lifetime yourself.

A `Connection` belongs to exactly one topic: it carries its `:topic`, and that
is the topic it is removed from when it has to be closed. Do not use
`subscribe!` to place one connection on a second topic — the second
subscription will not be cleaned up. Give a client one connection per topic, or
broadcast to a topic it is already on.

`in-memory` is single-node: connections live in one process and are lost on
restart (browsers reconnect via Datastar's SSE retry). The `Hub` protocol is the
seam for a future backend (Redis, Postgres `LISTEN`/`NOTIFY`) — three methods
over whatever storage you like. Everything about running a connection lives
behind [`Connector`](#the-connector) instead, so a new hub inherits it.

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

(def hub (hub/in-memory))

;; A state-based connector coalesces: a superseded snapshot is worthless.
(def connector (conn/connector {:queue-fn :latest}))

;; Each connection carries the user it belongs to. Nothing else to track.
["/app/sse" {:datastar true
             :post (fn [{:keys [uid]}]
                     (hub/connect! hub connector :app {:data {:uid uid}}))}]

;; Mutations just move state; they do NOT broadcast.
["/app/toggle/:id" {:datastar true
                    :post (fn [{:keys [path-params]}]
                            (swap! !state toggle (:id path-params))
                            {:status 204})}]

;; One ticker re-renders current state for every connection, ~10x/sec.
(defonce ticker
  (.start (Thread/ofVirtual)
          #(loop []
             (let [state @!state]
               (hub/broadcast-each! hub :app
                                    (fn [conn]
                                      (sse/frame interpreter
                                                 (views/app state (:uid (conn/data conn)))))))
             (Thread/sleep 100)
             (recur))))
```

This uses `broadcast-each!` because each user sees their own view. If every
client would see identical HTML, render once per tick with `sse/frame` and pass
that one frame to `broadcast!` instead — a difference you feel at a few hundred
connections.

The two refinements production state-based apps want are both delivery concerns,
so neither is app code: both are connector configuration. Dropping frames for
slow clients is `:queue-fn :latest` above; deduping unchanged renders is a
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
;; connect!, in essence: spawn, register, drain, tear down
(let [conn (conn/-open connector sse-gen topic data)]
  (subscribe! hub topic conn)
  (try
    (conn/-drain! connector conn)   ; blocks until the queue closes
    (catch Exception _ nil)         ; a dead client is normal
    (finally
      (unsubscribe! hub topic conn)
      (conn/-close! connector conn))))
```

The connector supplies the `write!` its drain uses, and that is where a
disconnect is noticed:

```clojure
(fn [frame]
  (when-not (sse/apply! sse-gen frame)
    (queue/close! queue)))   ; ends the drain, so connect!'s cleanup runs
```

A disconnected client is detected on the write, not by an exception: the SDK's
adapters swallow the IOException and every later write returns `false`. That
`false` closes the queue, which ends the drain, which runs the cleanup above.

`connect!` spawns nothing. Its `on-open` blocks until the connection ends, on
every adapter — so whichever thread the adapter calls `on-open` on is the thread
that holds the connection open. On synchronous adapters that is the request's
own thread, which is exactly what keeps the response body streaming, so no extra
thread exists per connection. On async adapters (http-kit, Aleph) `on-open` runs
on a carrier thread of the server's own pool, and blocking it for the life of
the connection is your responsibility to account for: run those adapters on
virtual threads, or hand `connect!`'s `:on-open` to a thread you control.

### Adapters and held connections

`connect!` works on every adapter, including synchronous Ring/Jetty. The
mechanism is worth knowing, because it is easy to assume otherwise: the ring
adapter's response body is a `StreamableResponseBody`, and its
`write-body-to-stream` calls `on-open` as its last act. Ring's Jetty adapter
invokes that *after* your handler has returned, while streaming the body — so
blocking in `on-open` blocks `write-body-to-stream`, and that is precisely what
holds the stream open. The stream lives as long as `on-open` blocks, not as long
as the handler runs.

The cost is thread occupancy: a held connection occupies whatever thread the
adapter called `on-open` on, for its lifetime. On synchronous Ring/Jetty that is
a container thread — run Jetty's thread pool on virtual threads (Jetty 12
exposes a virtual-thread executor, and `ring-jetty-adapter` accepts a
`:thread-pool`) or the number of concurrent SSE connections you can hold is
capped by pool size. Async adapters (http-kit, Aleph) have no container thread
per request, but `connect!` still blocks wherever they run `on-open`, so budget
for that too rather than assuming the block is free.

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

### The connector

A **connector** spawns connections: it builds the queue, runs the drain loop,
notices the client leaving, and tears the connection down. It is separate from
the hub on purpose — a hub stores connections, and how it stores them is what
varies between an atom and a database. None of the machinery here does.

```clojure
(defprotocol Connector
  (-open   [connector sse-gen topic data])   ; build a Connection with its queue
  (-drain! [connector conn])                 ; block until the queue closes
  (-close! [connector conn]))                ; close the queue, then the generator
```

You rarely implement it. The default connector takes the two things worth
varying as plain configuration:

```clojure
(def connector (conn/connector {:queue-fn :bounded}))   ; the default
```

`:queue-fn` accepts a keyword (`:bounded` `:latest` `:unbounded`, at their
defaults) or a function of `{:topic :data}` returning a queue. It is called once
per connection, so policy can vary where that's meaningful:

```clojure
;; varying by topic, when one connector serves both models
{:queue-fn (fn [{:keys [topic]}]
             (if (= topic :app)
               (queue/latest)
               (queue/bounded 64)))}
```

Implement `Connector` yourself only to replace the machinery wholesale — to
instrument every connection, say. `connect!` reaches for nothing beyond these
three methods, so any implementation drops straight in.

### The drain loop

`:drain` is a function of `[conn q write!]` on the connector. The default takes
frames and writes them until the queue closes. Replacing it is how you get
behavior the library doesn't ship — dedupe, for instance, is three lines:

```clojure
{:drain (fn [_conn q write!]
          (loop [prev nil]
            (when-let [frame (queue/take! q)]
              (when (not= frame prev) (write! frame))
              (recur frame))))}
```

Because frames are plain values, `=` is the whole test. Batching — drain
everything pending, write once — is the other natural one.

A custom drain loop does **not** have to own cleanup. The `try`/`finally` lives
in `connect!`, outside the drain, so whatever drain you supply, `connect!`
unsubscribes the connection and calls the connector's `-close!` — closing the
queue and the SSE generator — once the drain returns or throws. `default-drain`
itself contains no error handling at all.

Disconnect detection is likewise not yours: the `write!` the connector hands
your drain closes the queue as soon as a write reports the connection closed,
which ends your `take!` loop the same way an explicit `close!` would. The SDK never throws
on an ordinary disconnect — its adapters catch the IOException and report the
closure as a `false` return from the next write — so a drain that tries to
detect a dead client by catching exceptions will not see one.

## Prior art & thanks

- [`datastar-clojure`](https://github.com/starfederation/datastar-clojure) — the
  official SDK `astrolabe` builds on.
- [hyperlith](https://github.com/andersmurphy/hyperlith) by Anders Murphy — the
  source of the streaming-Brotli-over-SSE technique and the state-based model.

## License

TBD.
