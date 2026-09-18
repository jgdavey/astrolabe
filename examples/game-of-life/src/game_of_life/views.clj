(ns game-of-life.views
  "Chassis hiccup. `astrolabe.sse/interpreter` is handed `chassis/html` as its
  `:render`, so these functions return data and never strings."
  (:require [dev.onionpancakes.chassis.core :as c]))

(def ^:private datastar-src
  "https://cdn.jsdelivr.net/gh/starfederation/datastar@1.0.3/bundles/datastar.js")

(defn board
  "The patch target. Rendered fresh on every tick and sent to every client, so
  it is a complete snapshot of the game rather than a delta."
  [cells cols]
  [:main#morph.main
   [:h1 "Game of Life"]
   [:p "Tap the board. Your colour spreads -- and so does everyone else's."]
   ;; The handler sits on the grid rather than each tile: 2500 identical
   ;; attributes would dominate the payload, and the event bubbles anyway.
   [:div.board {:style               (str "--cols:" cols)
                :data-on:pointerdown "@post(`/tap?id=${evt.target.dataset.id}`)"}
    (map-indexed (fn [idx cell]
                   [:div.tile {:class (name cell) :data-id idx}])
                 cells)]])

(defn page
  "The shim document. It ships an empty `#morph`; the first frame the SSE
  connection sends fills it in -- see `:on-connect` in `game-of-life.main`."
  []
  [c/doctype-html5
   [:html {:lang "en"}
    [:head
     [:meta {:charset "utf-8"}]
     [:meta {:name "viewport" :content "width=device-width, initial-scale=1"}]
     [:title "Game of Life"]
     [:meta {:name "description" :content "Conway's Game of Life, played by everyone at once"}]
     [:link {:rel "stylesheet" :href "/game-of-life.css"}]
     [:script {:type "module" :src datastar-src}]]
    [:body
     ;; data-init runs once, when Datastar initialises the element. It lives on
     ;; its own div rather than on #morph, which is replaced on every tick and
     ;; would therefore re-initialise -- opening a fresh stream each time.
     ;;
     ;; retryMaxCount: Infinity so a client that sleeps through a long outage
     ;; still comes back; Datastar otherwise gives up after a handful of tries.
     [:div {:data-init "@post('/updates', {retryMaxCount: Infinity})"}]
     [:noscript "This page needs JavaScript."]
     [:main#morph.main]]]])
