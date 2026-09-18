# Game of Life

Conway's Game of Life, played by everyone at once. Tap the board and a cross of
your colour appears; from there the usual B3/S23 rules take over and your colour
spreads, meets other people's, and dies out.

A port of [hyperlith's `game_of_life` example](https://github.com/andersmurphy/hyperlith/tree/master/examples/game_of_life)
onto astrolabe, and a worked version of the
[state-based recipe](../../README.md#state-based-recipe).

## Run it

Java 21 or later.

```bash
cd examples/game-of-life
clojure -M:run
```

Then open <http://localhost:8080>. Open it twice, in two browsers, and tap in
one — the other sees it within a tick. `PORT` overrides the port.

From a REPL, `(game-of-life.main/start!)` and `(stop!)`; the comment block at
the bottom of `main.clj` has the rest.

## What it does

One atom holds a 50×50 board. Nothing else is tracked.

- **Mutations move state and broadcast nothing.** `POST /tap` paints five cells
  and answers `204`. That is the whole handler.
- **One ticker owns the fan-out.** Every 200ms it advances a generation, renders
  the board *once*, and hands that single frame to every connection. Datastar's
  morph diffs it on the front end.
- **Reconnects self-heal.** A fresh connection renders current state in
  `:on-connect`, so there is no missed-event log and nothing to replay.

## The decisions worth reading

Each of these is a line or two of code with a reason behind it.

**`broadcast!`, not `broadcast-each!`** — every client sees identical HTML, so
the board is rendered once per tick rather than once per client. `broadcast-each!`
is for when clients see different views; using it here would pay 2500 divs of
rendering per connection per tick for no reason.

**`:queue-fn :latest`** — a state-based frame is a whole snapshot, so a
superseded one is worthless. A client that falls behind should skip to the
present, not work through a backlog.

**Dedupe before rendering.** The ticker skips the broadcast when the new board
equals the previous one, so an untouched board is silent. astrolabe also offers
a connector-level [`:drain`](../../README.md#the-drain-loop) that dedupes per
connection; doing it once, up front, is cheaper when the state is shared.

**`:heartbeat-ms 30000`** — a consequence of that dedupe. An empty board is
genuinely idle, and on an idle topic nothing ever attempts the write that
reveals a client has gone away. The
[state-based recipe](../../README.md#state-based-recipe) says a ticker needs no
keepalive; that holds only while the ticker is actually writing.

**`data-init`, on its own div.** Datastar 1.0's run-once attribute is
`data-init` — `data-on:load` would just add a `load` listener that never fires.
It sits outside `#morph` deliberately: `#morph` is replaced every tick, so an
init there would re-run and open a fresh stream each time.

**A virtual-thread worker pool.** `hub/connect!` blocks for the life of the
connection, on whichever thread http-kit called `on-open` on. With http-kit's
default pool that caps you at a handful of concurrent clients — see
[Adapters and held connections](../../README.md#adapters-and-held-connections).

**`/tap` is not flagged `:datastar true`.** It takes its argument in the query
string and answers 204, so there are no signals to parse and no SSE to write.

## Why brotli is not optional here

Each tick re-sends all 2500 tiles. Uncompressed that is ~109KB a frame — but the
HTML barely changes between renders, and one brotli encoder stays open for the
whole connection, so the stream compresses against everything it has already
sent. Three clients reading the same 20 frames:

| `Accept-Encoding` | bytes | ratio |
|---|---:|---:|
| `identity` | 2,184,272 | 1:1 |
| `gzip` | 140,490 | 16:1 |
| `br` | 6,871 | **318:1** |

That is what makes "re-render everything" affordable, and it is why
`deps.edn` carries the brotli4j native classifiers. gzip is still configured as
the fallback for clients that do not offer `br`.

## Layout

```
deps.edn                          astrolabe via :local/root, plus a server and a renderer
resources/public/game-of-life.css static; --cols is set inline so the grid tracks board-size
src/game_of_life/game.clj         the rules. Pure; knows nothing about HTTP
src/game_of_life/views.clj        chassis hiccup: the shim page and the board
src/game_of_life/main.clj         interpreter, hub, routes, ticker, server
```

## Credits

The rules come from hyperlith's example by
[Anders Murphy](https://github.com/andersmurphy), which adapted
[kaepr/game-of-life-cljs](https://github.com/kaepr/game-of-life-cljs). The
multiplayer-colour idea is Anders'.
