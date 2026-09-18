(ns game-of-life.main
  "Multiplayer Conway's Game of Life, as a worked example of astrolabe's
  state-based recipe.

  One atom holds the board. Mutations move it and broadcast nothing. A single
  ticker advances a generation five times a second, renders the whole board
  once, and hands that one frame to every connected client, letting Datastar's
  morph diff it on the front end. Reconnects self-heal, because a fresh
  connection simply renders current state."
  (:gen-class)
  (:require [astrolabe.brotli :as astrolabe.brotli]
            [astrolabe.connection :as conn]
            [astrolabe.hub :as hub]
            [astrolabe.reitit :as astrolabe]
            [astrolabe.sse :as sse]
            [dev.onionpancakes.chassis.core :as c]
            [game-of-life.game :as game]
            [game-of-life.views :as views]
            [jsonista.core :as json]
            [org.httpkit.server :as hk]
            [reitit.ring :as ring]
            [ring.middleware.cookies :refer [wrap-cookies]]
            [ring.middleware.params :refer [wrap-params]]
            [starfederation.datastar.clojure.adapter.common :as common]
            [starfederation.datastar.clojure.adapter.http-kit :as adapter.http-kit])
  (:import [java.util.concurrent Executors]))

(def board-size 50)
(def tick-ms 200)

(def colours [:red :blue :green :orange :fuchsia :purple])

;; ---------------------------------------------------------------------------
;; state

(defonce !state (atom {:board (game/empty-board board-size board-size)}))

(def interpreter
  (sse/interpreter {:render     c/html
                    :write-json json/write-value-as-string}))

(defonce hub (hub/in-memory))

;; `:latest` because a state-based frame is a whole snapshot: a superseded one
;; is worthless, so coalescing beats buffering for a client that falls behind.
;;
;; `:heartbeat-ms` because the ticker below skips broadcasting an unchanged
;; board. An empty board is therefore genuinely idle, and on an idle topic
;; nothing ever attempts the write that reveals a client has gone away.
(def connector
  (conn/connector {:queue-fn :latest :heartbeat-ms 30000}))

(defn- board-frame
  "Render the board once. The result depends on no particular connection, so
  one frame serves every client -- see `broadcast!` below."
  [board]
  (sse/frame interpreter [[:patch-elements (views/board board board-size)]]))

;; ---------------------------------------------------------------------------
;; handlers

(defn- read-json [s]
  (json/read-value s json/keyword-keys-object-mapper))

(defn- home [_]
  {:status  200
   :headers {"Content-Type" "text/html; charset=utf-8"}
   :body    (c/html (views/page))})

(defn- updates
  "The long-lived stream. `:on-connect` fills the client's empty `#morph` with
  current state; after that the ticker is the only thing that writes."
  [_]
  (hub/connect! hub connector :game
                {:on-connect (fn [conn]
                               (hub/send! hub conn (board-frame (:board @!state))))}))

(defn- colour-for [uid]
  (nth colours (mod (hash uid) (count colours))))

(defn- fill [board colour idx]
  (if (<= 0 idx (dec (* board-size board-size)))
    (assoc board idx colour)
    board))

(defn- fill-cross
  "Paint a plus sign centred on `idx`. Crude: it does not stop at row edges, so
  a tap on the last column bleeds one cell onto the next row. The original does
  the same, and on a 50x50 board nobody minds."
  [board colour idx]
  (-> board
      (fill colour (- idx board-size))
      (fill colour (dec idx))
      (fill colour idx)
      (fill colour (inc idx))
      (fill colour (+ idx board-size))))

(defn- tap
  "A mutation: it moves state and broadcasts nothing. The ticker will pick the
  change up on its next pass, which is what keeps this handler trivial."
  [{:keys [uid query-params]}]
  (when-let [idx (some-> (get query-params "id") parse-long)]
    (swap! !state update :board fill-cross (colour-for uid) idx))
  {:status 204})

;; ---------------------------------------------------------------------------
;; identity

(defn- wrap-uid
  "Give every browser a stable id in a cookie, and put it on the request. It is
  used for one thing -- picking your colour -- but it is also what a real app
  would hang `conn/data` off for per-client rendering."
  [handler]
  (fn [req]
    (let [existing (get-in req [:cookies "uid" :value])
          uid      (or existing (str (random-uuid)))
          resp     (handler (assoc req :uid uid))]
      (if existing
        resp
        (assoc-in resp [:cookies "uid"] {:value     uid
                                         :path      "/"
                                         :max-age   31536000
                                         :same-site :lax})))))

;; ---------------------------------------------------------------------------
;; routing

(def router
  (ring/router
   [["/"        {:get home}]
    ["/updates" {:datastar true :post updates}]
    ;; Not flagged `:datastar true`: it takes its argument in the query string
    ;; and answers 204, so there are no signals to read and no SSE to write.
    ["/tap"     {:post tap}]]
   {:data
    {:middleware
     [wrap-params
      wrap-cookies
      wrap-uid
      (astrolabe/middleware
       {:->sse-response adapter.http-kit/->sse-response
        :parse-json     read-json
        :interpreter    interpreter
        ;; Streaming brotli is what makes re-sending 2500 divs five times a
        ;; second cheap: one encoder stays open per connection, and the HTML
        ;; barely changes between renders.
        :compression    [["br"   (astrolabe.brotli/profile)]
                         ["gzip" common/gzip-profile]]})]}}))

(def app
  (ring/ring-handler router
                     (ring/routes
                      (ring/create-resource-handler {:path "/"})
                      (ring/create-default-handler))))

;; ---------------------------------------------------------------------------
;; the ticker

(defn- start-ticker!
  "Advance a generation every `tick-ms`, render it once, and send that single
  frame to everyone. `broadcast!` rather than `broadcast-each!` because every
  client sees identical HTML -- a difference you feel at a few hundred
  connections.

  It skips the broadcast when the board did not change, which is the whole of
  the dedupe story here. A connector-level `:drain` could do it per connection
  instead; doing it once, before rendering, is cheaper when the state is shared."
  []
  (Thread/startVirtualThread
   (fn []
     (try
       (loop [previous nil]
         (let [board (:board (swap! !state update :board
                                    game/next-generation board-size board-size))]
           (when (not= board previous)
             (hub/broadcast! hub :game (board-frame board)))
           (Thread/sleep tick-ms)
           (recur board)))
       (catch InterruptedException _ nil)))))

;; ---------------------------------------------------------------------------
;; lifecycle

(defonce !server (atom nil))
(defonce !ticker (atom nil))

(defn start!
  ([] (start! 8080))
  ([port]
   (reset! !ticker (start-ticker!))
   (reset! !server
           (hk/run-server #'app
                          {:port                  port
                           ;; `hub/connect!` blocks for the life of the
                           ;; connection, on whichever thread http-kit calls
                           ;; on-open on. With the default pool that caps you at
                           ;; a handful of concurrent clients; one virtual
                           ;; thread per request costs nothing to block.
                           :worker-pool           (Executors/newVirtualThreadPerTaskExecutor)
                           :legacy-return-value?  false}))
   (println (str "Game of Life running on http://localhost:" port))
   nil))

(defn stop! []
  (some-> @!ticker .interrupt)
  ;; Release held connections rather than leaving them blocking their threads.
  ;; Browsers reconnect on their own via Datastar's SSE retry.
  (hub/shutdown! hub)
  (some-> @!server (hk/server-stop! {:timeout 100}))
  (reset! !ticker nil)
  (reset! !server nil)
  nil)

(defn -main [& _]
  (.addShutdownHook (Runtime/getRuntime) (Thread. ^Runnable stop!))
  (start! (parse-long (or (System/getenv "PORT") "8080"))))

(comment
  (start!)
  (stop!)

  ;; how many clients are watching
  (count (hub/conns hub :game))

  ;; wipe the board
  (swap! !state assoc :board (game/empty-board board-size board-size))

  ;; how many cells are alive
  (count (remove #(= :dead %) (:board @!state))))
