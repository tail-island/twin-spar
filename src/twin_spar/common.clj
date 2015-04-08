(ns twin-spar.common
  (:require (clojure             [pprint :refer :all])
            (clojure.java        [jdbc   :as    jdbc])
            (clj-time            [coerce :as    time.coerce]))
  (:import  (java.sql            Timestamp)
            (org.joda.time       DateTime)
            (org.postgresql.util PGobject)))

(extend-protocol jdbc/ISQLValue
  DateTime
  (sql-value [this]
    (time.coerce/to-timestamp this)))

(extend-protocol jdbc/IResultSetReadColumn
  Timestamp
  (result-set-read-column [this metadata index]
    (time.coerce/from-sql-time this)))

(def sql-name
  ^{:doc "Returns the escaped name String."}
  (jdbc/as-sql-name (jdbc/quoted \")))

(defn dissoc-in
  "Dissociates a value in a nested associative structure, and returns a new map that does not contain a mapping for ks."
  [m [k & ks]]
  (if ks
    (assoc  m k (dissoc-in (get m k) ks))
    (dissoc m k)))

(defn pprint-format
  "Format an object with pprint formatter."
  [object]
  (->> (with-out-str
         (pprint object))
       (butlast)
       (apply str)))
