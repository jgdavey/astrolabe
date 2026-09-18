(ns game-of-life.game
  "Conway's Game of Life on a flat vector board.

  A cell is either `:dead` or a colour keyword, and a newborn cell inherits the
  colour of one of the living neighbours that produced it. That is the whole
  multiplayer conceit: your colour spreads across the board, and you can watch
  it meet someone else's.

  Nothing here knows about astrolabe, HTTP, or rendering -- it is the pure core
  the rest of the example moves around.

  Ported from hyperlith's game_of_life example, which in turn adapted
  https://github.com/kaepr/game-of-life-cljs")

(def neighbour-offsets
  [[-1 -1] [-1 0] [-1 1]
   [0 -1]  #_cell [0 1]
   [1 -1]  [1 0]  [1 1]])

(defn dead-cell [] :dead)

(defn alive? [cell] (not= cell :dead))

(defn alive-cell
  "A newborn cell's colour: one of the living neighbours that spawned it."
  [living-neighbours]
  (rand-nth living-neighbours))

(defn coordinates->index [row col max-cols]
  (+ col (* row max-cols)))

(defn index->coordinates [idx max-cols]
  [(quot idx max-cols) (rem idx max-cols)])

(defn empty-board [max-rows max-cols]
  (vec (repeat (* max-rows max-cols) (dead-cell))))

(defn get-cell [board idx]
  (get board idx (dead-cell)))

(defn neighbour-indices
  "The on-board neighbours of `[row col]`, as indices into the flat board."
  [[row col] max-rows max-cols]
  (into []
        (keep (fn [[dr dc]]
                (let [r (+ row dr)
                      c (+ col dc)]
                  (when (and (<= 0 r) (< r max-rows)
                             (<= 0 c) (< c max-cols))
                    (coordinates->index r c max-cols)))))
        neighbour-offsets))

(defn cell-transition
  "The classic B3/S23 rules. `living-neighbours` is non-empty whenever the
  result is a live cell, which is what makes `alive-cell` safe here."
  [cell living-neighbours]
  (let [n (count living-neighbours)]
    (if (or (and (alive? cell) (or (= n 2) (= n 3)))
            (and (not (alive? cell)) (= n 3)))
      (alive-cell living-neighbours)
      (dead-cell))))

(defn next-generation [board max-rows max-cols]
  (let [size (* max-rows max-cols)]
    (loop [idx 0
           next (transient board)]
      (if (= idx size)
        (persistent! next)
        (let [living (into []
                           (comp (map #(get-cell board %))
                                 (filter alive?))
                           (neighbour-indices (index->coordinates idx max-cols)
                                              max-rows max-cols))]
          (recur (inc idx)
                 (assoc! next idx (cell-transition (get-cell board idx) living))))))))

(comment
  (empty-board 10 10)
  (next-generation (empty-board 10 10) 10 10))
