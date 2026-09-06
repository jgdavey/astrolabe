# Connector Protocol — Design

**Status:** approved, ready for implementation planning
**Supersedes:** the `Hub` extension story in `README.md` (Broadcasting, Hub API,
Delivery & concurrency)

## Problem

`astrolabe.hub/Hub` declares three methods:

```clojure
(defprotocol Hub
  (-subscribe!   [hub topic conn])
  (-unsubscribe! [hub topic conn])
  (-conns        [hub topic]))
```

But `connect!` and `frame` reach past the protocol for four more things by
structural convention:

| site | reads |
|------|-------|
| `hub.clj:121` | `((:queue-fn hub) proto)` |
| `hub.clj:124` | `(sse/apply! (:interpreter hub) …)` |
| `hub.clj:129` | `((:drain hub) conn q write!)` |
| `hub.clj:63` | `(sse/frame (:interpreter hub) events)` |

So the real contract is "implement three methods **and** be a record carrying
exactly these four field names." A conforming implementation that is not shaped
like `InMemoryHub` breaks. The README calls the protocol "the seam for a future
multi-node backend"; it is not one.

The deeper problem is that two unrelated concerns are fused into one record:

- **Storage** — add a connection to a topic, remove it, list a topic's
  connections. Genuinely varies by backend: an atom today, a database later.
- **Local connection machinery** — build the queue, run the drain loop, detect
  disconnects, tear down. Invariant across backends. A database-backed hub
  would reimplement `connect!` *identically*, which is the tell that it does not
  belong to `Hub` at all.

## Design

Split along that line. `Hub` keeps storage. A new protocol owns connection
lifecycle. `connect!` becomes the glue that brings the two together.

### `astrolabe.connection` (new namespace, alias `conn`)

`Connection` stays a plain value — it lives in sets, gets compared, and is
handed to user functions — so behavior lives on the connector, not in the
record.

```clojure
(defrecord Connection [sse-gen queue topic data])

(defn data
  "The app-supplied data attached to a connection at `connect!` time."
  [conn]
  (:data conn))

(defprotocol Connector
  (-open   [connector sse-gen topic data]
    "Build a Connection with its queue. Does not subscribe or block.")
  (-drain! [connector conn]
    "Block, draining `conn`'s queue and writing frames, until the queue closes.")
  (-close! [connector conn]
    "Close the queue, then the SSE generator."))
```

Birth, life, death. The default implementation keeps `queue-fn` and `drain` as
**configuration**, so the common customizations — a `:latest` queue, a deduping
drain — need no protocol implementation at all. The protocol exists to replace
the whole mechanism.

```clojure
(defrecord DefaultConnector [queue-fn drain]
  Connector
  (-open [_ sse-gen topic data]
    (->Connection sse-gen (queue-fn {:topic topic :data data}) topic data))

  (-drain! [_ conn]
    (let [{:keys [sse-gen queue]} conn
          write! (fn [frame]
                   (when-not (sse/apply! sse-gen frame)
                     ;; the client is gone; end the drain loop
                     (queue/close! queue)))]
      (drain conn queue write!)))

  (-close! [_ conn]
    (queue/close! (:queue conn))
    (d*/close-sse! (:sse-gen conn))))

(defn default-drain
  "Take frames until the queue closes, writing each one."
  [_conn q write!]
  (loop []
    (when-let [frame (queue/take! q)]
      (write! frame)
      (recur))))

(defn connector
  "Build the default connector.

  Opts:
  - `:queue-fn` keyword or (fn [{:keys [topic data]}]) -> Queue; defaults to `:bounded`
  - `:drain`    (fn [conn queue write!]) -> blocks until the queue closes;
                defaults to [[default-drain]]"
  ([] (connector {}))
  ([{:keys [queue-fn drain]}]
   (->DefaultConnector (queue/->queue-fn (or queue-fn :bounded))
                       (or drain default-drain))))
```

Two consequences worth naming:

- **Disconnect detection becomes a private detail of the connector** rather than
  an inline closure in `connect!`. Its semantics are unchanged and remain
  load-bearing: the SDK never throws when a client goes away — the adapter
  catches the `IOException`, closes the generator, and every later write
  silently returns `false` — so `write!` closing the queue on a `false` verdict
  is the only signal that ends the drain.
- **The two-step `proto` / `assoc :queue` dance disappears.** `queue-fn` only
  ever needed `:topic` and `:data`, so it now receives `{:topic … :data …}`
  instead of a half-built `Connection`. The README's documented
  `(fn [{:keys [topic]}] …)` form still works verbatim.

### `astrolabe.hub` (reduced to storage)

The `Hub` protocol is unchanged. The record loses every field except its state.

```clojure
(defrecord InMemoryHub [!topics])

(defn in-memory
  "A single-node hub. Connections live in this process and are lost on restart;
  browsers reconnect via Datastar's SSE retry."
  []
  (->InMemoryHub (atom {})))
```

`connect!` becomes glue and reads as the sequence it performs — spawn, register,
drain, clean up:

```clojure
(defn connect!
  ([hub connector topic] (connect! hub connector topic {}))
  ([hub connector topic {:keys [data on-close on-exception]}]
   (sse/response
    (cond-> {:on-open
            (fn [sse-gen]
              (let [conn (conn/-open connector sse-gen topic data)]
                (subscribe! hub topic conn)
                (try
                  (conn/-drain! connector conn)
                  (catch Exception _ nil)
                  (finally
                    (unsubscribe! hub topic conn)
                    (conn/-close! connector conn)))))}
      on-close     (assoc :on-close on-close)
      on-exception (assoc :on-exception on-exception)))))
```

Nothing is read off the hub record. A database-backed hub implements three
methods and inherits all of the above unchanged.

### Rendering moves out of the hub

With the hub reduced to storage, `:interpreter` is a wart on it for the same
reason `:queue-fn` was. Rendering moves to the call site, and `broadcast!`
becomes frame-only — consistent with the existing deliberate rule that `send!`
accepts frames only, and making the render-once property visible where it
happens:

```clojure
(def itp (sse/interpreter {:render render}))

(hub/broadcast! hub topic
  (sse/frame itp [[:patch-elements (views/card card) {:mode :append}]]))
```

`hub/frame` is deleted; `sse/frame` is the single renderer. `broadcast-each!`'s
`f` returns a frame rather than events.

### Naming: `data`, not `meta`

`Connection`'s app-supplied payload is `:data`, read with `conn/data`. Besides
reading better, `data` collides with nothing in `clojure.core`, so
**`(:refer-clojure :exclude [meta])` disappears from the codebase** — an
exclusion whose omission silently shadows `clojure.core/meta`.

## API changes

The library is unreleased alpha; these are breaking and intentional.

| before | after |
|---|---|
| `(hub/in-memory {:interpreter … :queue-fn … :drain …})` | `(hub/in-memory)` |
| `(hub/connect! hub topic opts)` | `(hub/connect! hub connector topic opts)` |
| `(hub/frame hub events)` | deleted — use `(sse/frame itp events)` |
| `(hub/broadcast! hub topic events)` | `(hub/broadcast! hub topic frame)` |
| `(hub/broadcast-each! hub topic f)` | same; `f` returns a frame |
| `(hub/meta conn)` | `(conn/data conn)` |
| `connect!` opt `:meta` | `:data` |
| `hub/default-drain` | `conn/default-drain` |
| `(sse/apply! itp sse-gen frame)` | `(sse/apply! sse-gen frame)` |
| `Connection` field `:meta` | `:data` |

`sse/apply!` already ignores its interpreter argument (`sse.clj:66` binds
`_itp`); frames arrive pre-rendered, so writing needs no interpreter. Dropping
the parameter also updates its two call sites in `astrolabe.reitit`
(`reitit.clj:46` and `:48`).

## Preserved exactly

These were established by earlier review and must not regress:

- **Cleanup ordering:** unsubscribe → close queue → close generator. `-close!`
  performs the last two in that order.
- **Disconnect detection:** a `false` from `sse/apply!` closes the queue, ending
  the drain, so `connect!`'s `finally` unsubscribes. The regression test using a
  stub `SSEGenerator` whose `send-event!` returns `false` must still fail if the
  behavior is reverted.
- **`apply!` attempts every event in a frame**; a failed write never skips the
  rest, only the verdict changes.
- **`send!` stays frame-only** and closes a connection whose queue refuses.
- **Frames stay `=`-comparable plain values.**
- **One topic per connection** — `send!`'s refusal path unsubscribes using
  `(:topic conn)`.

## Testing

- New `test/astrolabe/connection_test.clj`: `-open` builds a queue from
  `queue-fn` and passes it `{:topic :data}`; `-drain!` writes frames until the
  queue closes; `-drain!` closes the queue when a write reports the client gone;
  `-close!` closes queue then generator; `conn/data` round-trips.
- `hub_test.clj`: rework for the new `connect!` arity and frame-only broadcasts.
  The 14 existing tests keep their assertions; only construction changes. The
  disconnect regression test moves to exercising `-drain!` plus `connect!`'s
  cleanup.
- `sse_test.clj`: drop the interpreter argument from the four `apply!` call
  sites.
- `reitit_test.clj`: unaffected except through `apply!`'s arity.
- A hub test should construct a `Connector` that is **not** `DefaultConnector`
  (a stub recording calls) to prove `connect!` depends only on the protocol.

## README sections to rewrite

Broadcasting; Two broadcast paths; Connection metadata (→ "Connection data");
Hub API; State-based recipe; Configuring the hub; The drain loop.

## Non-goals

- **No multi-node design.** `Hub`'s add/remove/list-by-topic is correct for a
  database-backed implementation as it stands; delivery semantics across nodes
  are out of scope and deliberately not designed here.
- No change to `astrolabe.event`, `astrolabe.queue`, `astrolabe.compression`, or
  `astrolabe.brotli`.
- No new queue or drain implementations.
- `Connector` gains no fourth method speculatively.
