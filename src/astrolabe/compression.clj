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
