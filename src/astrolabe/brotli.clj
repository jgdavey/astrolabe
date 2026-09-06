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
