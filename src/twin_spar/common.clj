(ns twin-spar.common
  (:require (clojure       [pprint :refer :all])
            (clojure.java  [jdbc   :as    jdbc])
            (clj-time      [coerce :as    time.coerce]))
  (:import  (java.sql      Timestamp)
            (org.joda.time DateTime)))

;; Extending JDBC types for clj-time.
(extend-protocol jdbc/ISQLValue
  DateTime
  (sql-value [this]
    (time.coerce/to-timestamp this)))

(extend-protocol jdbc/IResultSetReadColumn
  Timestamp
  (result-set-read-column [this metadata index]
    (time.coerce/from-sql-time this)))

;; Returns an escaped name string.
(def sql-name
  (jdbc/as-sql-name (jdbc/quoted \")))

(defn dissoc-in
  "Dissociates a value in a nested associative structure, and returns a new map that does not contain a mapping for keys."
  [map [key & more]]
  (if more
    (assoc  map key (dissoc-in (get map key) more))
    (dissoc map key)))

(defn pprint-format
  "Format an object with pprint formatter."
  [object]
  (->> (with-out-str
         (pprint object))
       (butlast)
       (apply str)))
