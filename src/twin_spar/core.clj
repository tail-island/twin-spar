(ns twin-spar.core
  (:use     (twin-spar     common))
  (:require (clojure       [string  :as    string])
            (clojure.core  [strint  :refer :all])
            (clojure.tools [logging :as    log])
            (clojure.java  [jdbc    :as    jdbc])
            (clj-time      [core    :as    time]))
  (:import  (java.util     UUID)))

(defn- many-to-one-relationship-key-to-physical-column-key
  [key]
  (keyword (<< "~(name key)-key")))

(defn- physical-column-keys
  [table-schema]
  (let [{:keys [columns many-to-one-relationships one-to-many-relationships]} table-schema]
    (concat [:key :modified-at]
            (keys columns)
            (->> (keys many-to-one-relationships)
                 (map many-to-one-relationship-key-to-physical-column-key)))))

(defn- merge-changes
  [& map-or-changes]
  (apply merge-with (partial merge-with merge) map-or-changes))

(defn new-key
  []
  (UUID/randomUUID))

(defprotocol IDatabaseElement
  (get-data    [_])
  (get-changes [_]))

(defn- row
  [database table-key {:keys [many-to-one-relationships one-to-many-relationships] :as table-schema} row-data & {:keys [changes]}]
  (reify
    clojure.lang.IPersistentMap
    (valAt [_ key]
      (or (if-let [{:keys [table-key]} (get many-to-one-relationships key)]
            (get-in database [table-key (get row-data (many-to-one-relationship-key-to-physical-column-key key))]))
          (if-let [{:keys [table-key many-to-one-relationship-key]} (get one-to-many-relationships key)]
            (->> (get database table-key)
                 (vals)
                 (filter #(= (get % (many-to-one-relationship-key-to-physical-column-key many-to-one-relationship-key)) (:key row-data)))))
          (get row-data key)))
    (entryAt [this key]
      (clojure.lang.MapEntry. key (.valAt this key)))
    (seq [this]
      (->> (physical-column-keys table-schema)
           (map #(.entryAt this %))))
    (assoc [_ key val]
      (assert (some #(= key %) (concat (physical-column-keys table-schema) (keys many-to-one-relationships))) "row can assoc with only columns and many-to-one-relationships")
      (assert (not= key :key)                                                                                 "changing key is not supported.")
      (-> [key val changes]
          (cond-> (contains? many-to-one-relationships key) ((fn [[key val changes]]
                                                               [(many-to-one-relationship-key-to-physical-column-key key)
                                                                (:key val)
                                                                (merge-changes changes (get-changes val))])))
          ((fn [[key val changes]]
             (let [modified? (not= (get row-data key) val)]
               (row database table-key table-schema (cond-> (assoc row-data key val)
                                                      modified? (assoc :modified? true))
                    :changes (cond-> (assoc-in changes [table-key (:key row-data) key] val)
                               modified? (-> (assoc-in [table-key (:key row-data) :modified?] true)))))))))
    IDatabaseElement
    (get-data [_]
      row-data)
    (get-changes [_]
      changes)))

(defn- table
  [database table-key {:keys [many-to-one-relationships] :as table-schema} table-data & {:keys [changes]}]
  (reify
    clojure.lang.IPersistentMap
    (valAt [_ key]
      (if-let [row-data (get table-data key)]
        (if-not (:deleted? row-data)
          (row database table-key table-schema row-data))))
    (entryAt [this key]
      (if-let [val (.valAt this key)]
        (clojure.lang.MapEntry. key val)))
    (seq [this]
      (->> (keys table-data)
           (keep #(.entryAt this %))))
    (assoc [_ key val]
      (assert (or (= (:key val) nil) (= (:key val) key)) "changing key is not supported.")
      (if (:key val)
        (table database table-key table-schema (assoc table-data key (get-data val)) :changes (merge-changes changes (get-changes val)))
        (let [val (assoc (reduce-kv #(apply assoc
                                       %1 (cond-> [%2 %3]
                                            (contains? many-to-one-relationships %2) ((fn [[relationship-key row]]
                                                                                        [(many-to-one-relationship-key-to-physical-column-key relationship-key) (:key row)]))))
                                    {}
                                    val)
                         :key       key
                         :inserted? (not (contains? table-data key))
                         :modified? true)]
          (table database table-key table-schema (assoc table-data key val) :changes (assoc-in changes [table-key key] val)))))
    (without [this key]
      (if-let [row-data (get table-data key)]
        (table database table-key table-schema (dissoc table-data key) :changes (update-in changes [table-key key] #(assoc % :deleted? true, :modified true)))
        this))
    (count [_]
      (count table-data))
    (iterator [this]
      (.iterator (seq this)))
    IDatabaseElement
    (get-data [_]
      table-data)
    (get-changes [_]
      changes)))

(defprotocol IDatabase
  (get-inserted-rows [_])
  (get-modified-rows [_])
  (get-deleted-rows  [_]))

(defn database
  [database-schema database-data]
  (letfn [(get-updated-rows [pred]
            (reduce-kv #(assoc %1 %2 (not-empty (reduce-kv (fn [result row-key row-data]
                                                             (cond-> result
                                                               (pred row-data) (assoc row-key row-data)))
                                                           {}
                                                           %3)))
                       {}
                       database-data))]
    (reify
      clojure.lang.IPersistentMap
      (valAt [this key]
        (if-let [table-data (get database-data key)]
          (table this key (get database-schema key) table-data)))
      (entryAt [this key]
        (clojure.lang.MapEntry. key (.valAt this key)))
      (seq [this]
        (->> (keys database-schema)
             (map #(.entryAt this %))))
      (assoc [_ key val]
        (database database-schema (merge-changes database-data (get-changes val))))
      IDatabase
      (get-inserted-rows [_]
        (get-updated-rows (every-pred :inserted? (complement :deleted?))))
      (get-modified-rows [_]
        (get-updated-rows (every-pred (complement :inserted?) :modified? (complement :deleted?))))
      (get-deleted-rows [_]
        (get-updated-rows (every-pred (complement :inserted?) :deleted?))))))

(def operators
  (atom #{}))

(def where-clause-fns
  (atom {}))

(def sql-parameters-fns
  (atom {}))

(defmacro defoperator
  [operator & {:keys [where-clause-fn sql-parameters-fn]}]
  `(do (swap! operators          conj  '~operator)
       (swap! where-clause-fns   assoc '~operator (or ~where-clause-fn
                                                      (fn [parameters#] (string/join (format " %s " (subs (string/upper-case (name '~operator)) 1)) parameters#))))
       (swap! sql-parameters-fns assoc '~operator (or ~sql-parameters-fn
                                                      (fn [parameters#] (mapcat identity parameters#))))
       (defmacro ~operator
         [& xs#]
         `(vector '~'~operator ~@xs#))))

(defoperator $and)
(defoperator $or)
(defoperator $not
  :where-clause-fn #(format "NOT %s" (first %))
  :sql-parameters-fn #(first %))
(defoperator $<)
(defoperator $>)
(defoperator $<=)
(defoperator $>=)
(defoperator $=)
(defoperator $<>)
(defoperator $is)
(defoperator $like)
(defoperator $in
  :where-clause-fn #(format "%s IN (%s)" (first %) (string/join ", " (second %)))
  :sql-parameters-fn #(second %))

(defn- select-sql
  [database-schema table-key & [condition]]
  (let [alias-number (atom 0)]
    (letfn [(normalize-property-key [table-key alias-key [property-key & more]]
              (if-let [[column-key next-table-key next-column-key] (or (if-let [relationship-schema (get-in database-schema [table-key :many-to-one-relationships property-key])]
                                                                         [(many-to-one-relationship-key-to-physical-column-key property-key) (:table-key relationship-schema) :key])
                                                                       (if-let [relationship-schema (get-in database-schema [table-key :one-to-many-relationships property-key])]
                                                                         [:key (:table-key relationship-schema) (many-to-one-relationship-key-to-physical-column-key (:many-to-one-relationship-key relationship-schema))]))]
                (let [next-alias-key (keyword (<< "_t-~(swap! alias-number inc)"))]
                  (-> (normalize-property-key next-table-key next-alias-key more)
                      (update-in [:join-clause] #(str (<< "LEFT JOIN ~(sql-name next-table-key) AS ~(sql-name next-alias-key) ON (~(sql-name next-alias-key).~(sql-name next-column-key) = ~(sql-name alias-key).~(sql-name column-key))") (and % " ") %))))
                {:join-clause  nil
                 :where-clause (<< "~(sql-name alias-key).~(sql-name property-key)")}))
            (normalize-condition [condition]
              (cond
                (keyword? condition) (normalize-property-key table-key table-key (map keyword (string/split (name condition) #"\.")))
                (coll?    condition) (map normalize-condition condition)
                :else                condition))
            (join-clause [condition]
              (cond
                (map?  condition) (:join-clause condition)
                (coll? condition) (string/join " " (keep (comp not-empty join-clause) condition))))
            (where-clause [condition]
              (cond
                (map?  condition) (:where-clause condition)
                (coll? condition) (if (contains? @operators (first condition))
                                    (->> ((get @where-clause-fns (first condition)) (map where-clause (next condition)))
                                         (format "(%s)"))
                                    (map where-clause condition))
                :else             (if-not (nil? condition)
                                    "?"
                                    "NULL")))
            (sql-parameters [condition]
              (cond
                (map?  condition) nil
                (coll? condition) (if (contains? @operators (first condition))
                                    ((get @sql-parameters-fns (first condition)) (map sql-parameters (next condition)))
                                    (mapcat sql-parameters condition))
                :else             (if-not (nil? condition)
                                    [condition])))]
      (let [[join-clause where-clause sql-parameters] ((juxt join-clause where-clause sql-parameters) (normalize-condition (or condition true)))]  ; conditionが指定されない場合はWHERE NULLになってしまうので、必ず真になる値に置き換えます。
        (apply vector
          (<< "SELECT DISTINCT ~(sql-name table-key).* FROM ~(sql-name table-key) ~{join-clause} ~(and where-clause \"WHERE\") ~{where-clause}")
          sql-parameters)))))

(defn merge-map-to-database-data
  [database-data table-key map]
  (update-in database-data [table-key] #(reduce (fn [result row] (assoc result (:key row) row))
                                                %
                                                map)))

(defn database-data
  [database-schema database-spec table-key & [condition other-data]]
  (letfn [(as-recursive-select-sql [table-key sql]
            (let [recursive-relationship-keys (->> (get-in database-schema [table-key :many-to-one-relationships])
                                                   (keep #(if (= (:table-key (second %)) table-key)
                                                            (first %))))]
              (letfn [(to-recursive-sql [sql]
                        (let [table               (sql-name table-key)
                              recursive-table-key (keyword (<< "recursive-~(name table-key)"))
                              recursive-table     (sql-name recursive-table-key)
                              join-on-clause      (->> recursive-relationship-keys
                                                       (map #(let [physical-column (sql-name (many-to-one-relationship-key-to-physical-column-key %))]
                                                               (<< "~{recursive-table}.~{physical-column} = ~{table}.~(sql-name :key)")))
                                                       (string/join " OR "))]
                          (<< "SELECT ~{table}.* FROM (WITH RECURSIVE ~{recursive-table} AS (~{sql} UNION SELECT ~{table}.* FROM ~{table} JOIN ~{recursive-table} ON ~{join-on-clause}) SELECT ~{recursive-table}.* FROM ~{recursive-table}) AS ~{table}")))]
                (cond-> sql
                  (not-empty recursive-relationship-keys) (to-recursive-sql)))))
          (relationship-table-key-and-sqls [table-key sql relationship-type continue?-fn column-keys-fn]
            (->> (get-in database-schema [table-key relationship-type])
                 (keep (fn [[relationship-key relationship-schema]]
                         (if (continue?-fn relationship-key relationship-schema)
                           (let [[next-column-key previous-column-key] (column-keys-fn relationship-key relationship-schema)
                                 next-table-key                        (:table-key relationship-schema)
                                 next-sql                              (->> (<< "SELECT ~(sql-name next-table-key).* FROM ~(sql-name next-table-key) WHERE ~(sql-name next-table-key).~(sql-name next-column-key) IN (~(string/replace-first sql \"*\" (sql-name previous-column-key)))")
                                                                            (as-recursive-select-sql next-table-key))]
                             (cons [next-table-key next-sql] (many-to-one-relationship-table-key-and-sqls next-table-key next-sql))))))  ; 親方向を再帰で辿ります。マスター・テーブルが不足すると、データとして不完全すぎるためです。で、子方向は、データ量が大きくなりすぎるので、無視します……。
                 (apply concat)))
          (many-to-one-relationship-table-key-and-sqls [table-key sql]
            (relationship-table-key-and-sqls table-key sql
                                             :many-to-one-relationships
                                             (fn [relationship-key relationship-schema] (not= (:table-key relationship-schema) table-key))  ; 自己参照は、WITH RECURSIVEで実現します。
                                             (fn [relationship-key relationship-schema] [:key (many-to-one-relationship-key-to-physical-column-key relationship-key)])))
          (one-to-many-relationship-table-key-and-sqls [table-key sql]
            (relationship-table-key-and-sqls table-key sql
                                             :one-to-many-relationships
                                             (fn [relationship-key relationship-schema] true)
                                             (fn [relationship-key relationship-schema] [(many-to-one-relationship-key-to-physical-column-key (:many-to-one-relationship-key relationship-schema)) :key])))]
    (let [[sql & sql-parameters] (select-sql database-schema table-key condition)
          recursive-sql          (as-recursive-select-sql table-key sql)]
      (->> (concat [[table-key recursive-sql]]
                   (many-to-one-relationship-table-key-and-sqls table-key recursive-sql)
                   (one-to-many-relationship-table-key-and-sqls table-key sql))
           (reduce (fn [result [table-key sql]]
                     (log/info (<< "Executing SQL.\n~{sql}\n~(pprint-format sql-parameters)"))
                     (merge-map-to-database-data result table-key (jdbc/query database-spec (apply vector sql sql-parameters))))
                   (or other-data {}))))))

(defn save!
  [database-schema database-spec database]
  (letfn [(concurrent-control [execute-results]
            (when (not= (first execute-results) 1)
              (throw (ex-info "data had been updated by another user" {}))))
          (update [table-key & updates]
            (let [[inserted-rows modified-rows deleted-rows] (let [physical-column-keys (physical-column-keys (get database-schema table-key))]
                                                               (map (fn [updated-rows]
                                                                      (map #(select-keys % physical-column-keys) updated-rows))
                                                                    updates))]
              (doseq [row inserted-rows]
                (log/info (<< "Inserting data.\n~{table-key}\n~(pprint-format row)"))
                (jdbc/insert! database-spec table-key (assoc row :modified-at (time/now)) :entities (jdbc/quoted \")))
              (doseq [row modified-rows]
                (log/info (<< "Updating data.\n~{table-key}\n~(pprint-format row)"))
                (->> (jdbc/update! database-spec table-key (assoc row :modified-at (time/now)) ["\"key\" = ? AND \"modified-at\" = ?" (:key row) (:modified-at row)] :entities (jdbc/quoted \"))
                     (concurrent-control)))
              (doseq [row deleted-rows]
                (log/info (<< "Deleting data.\n~{table-key}\n~(pprint-format row)"))
                (->> (jdbc/delete! database-spec table-key ["\"key\" = ? AND \"modified-at\" = ?" (:key row) (:modified-at row)] :entities (jdbc/quoted \"))
                     (concurrent-control)))))]
    (let [updates ((juxt get-inserted-rows get-modified-rows get-deleted-rows) database)]
      (doseq [table-key (keys database-schema)]
        (apply update table-key (map #(vals (get % table-key)) updates))))))

(def ^:private ^:const database-types
  {:string    "text"
   :integer   "integer"
   :decimal   "decimal(30,10)"
   :boolean   "boolean"
   :timestamp "timestamp"})

(defn create-tables
  [database-schema database-spec]
  (letfn [(column-spec [[column-key {:keys [type]}]]
            [column-key (get database-types type)])
          (many-to-one-relationship-spec [[relationship-key _]]
            [(many-to-one-relationship-key-to-physical-column-key relationship-key) "uuid"])]
    (doseq [[table-key table-schema] database-schema]
      (jdbc/db-do-commands database-spec (apply jdbc/create-table-ddl
                                           table-key
                                           [:key         "uuid NOT NULL PRIMARY KEY"]
                                           [:modified-at "timestamp"]
                                           (concat (map column-spec                   (:columns                   table-schema))
                                                   (map many-to-one-relationship-spec (:many-to-one-relationships table-schema))
                                                   [:entities (jdbc/quoted \")]))))))

(defn drop-tables
  [database-schema database-spec]
  (doseq [[table-key _] database-schema]
    (jdbc/db-do-commands database-spec (jdbc/drop-table-ddl table-key :entities (jdbc/quoted \")))))
