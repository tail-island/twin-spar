(ns twin-spar.core
  (:require (clojure       [pprint  :as    pprint]
                           [string  :as    string])
            (clojure.core  [strint  :refer :all])
            (clojure.tools [logging :as    log])
            (clojure.java  [jdbc    :as    jdbc])
            (clj-time      [core    :as    time]
                           [coerce  :as    time.coerce]))
  (:import  (java.sql      Timestamp)
            (java.util     UUID)
            (org.joda.time DateTime)))

;; Please create a map which shows database schema, when using twin-spar.
(comment
  ;; syntax
  {:table-key {:columns                   {:column-key       {:type      :column-type}}
               :many-to-one-relationships {:relationship-key {:table-key :parent-table}}
               :one-to-many-relationships {:relationship-key {:table-key :child-table, :many-to-one-relationship-key :reverse-relationship-key}}}}
  
  ;; example
  (def database-schema {:products   {:columns                   {:name                {:type      :string}
                                                                 :price               {:type      :decimal}}
                                     :many-to-one-relationships {:category            {:table-key :categories}}}
                        :categories {:columns                   {:name                {:type      :string}}
                                     :many-to-one-relationships {:superior-category   {:table-key :categories}}
                                     :one-to-many-relationships {:inferior-categories {:table-key :categories, :many-to-one-relationship-key :superior-category}
                                                                 :products            {:table-key :products,   :many-to-one-relationship-key :category}}}}))

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
(def ^:private sql-name
  (jdbc/as-sql-name (jdbc/quoted \")))

(defn- pprint-format
  "Format an object with pprint formatter."
  [object]
  (->> (with-out-str
         (pprint/pprint object))
       (butlast)
       (apply str)))

(defn dissoc-in
  "Dissociates a value in a nested associative structure, and returns a new map that does not contain a mapping for keys."
  [map [key & more]]
  (if more
    (assoc  map key (dissoc-in (get map key) more))
    (dissoc map key)))

(defn many-to-one-relationship-key-to-physical-column-key
  "Returns a column keyword from many-to-one-relationship-key.  
   A many-to-one-relationship-key `:parent` is changed to `:parent-key`."
  [key]
  (keyword (<< "~(name key)-key")))

(defn- physical-column-keys
  "Returns all columns' keywords on the table."
  [{:keys [columns many-to-one-relationships]}]
  (concat [:key :modified-at]
          (keys columns)
          (->> (keys many-to-one-relationships)
               (map many-to-one-relationship-key-to-physical-column-key))))

(defn- merge-changes
  "Merge database changes.  
   The database change is a map like `{:table-key {:row-key {:column-key changed-value}}}`."
  [& map-or-changes]
  (apply merge-with (partial merge-with merge) map-or-changes))

(defn new-key
  "Generates a new row's key."
  []
  (UUID/randomUUID))

(defprotocol IDataHolder
  "Data. Database, tables and rows."
  (get-data    [_] "Returns a map that shows data."))

(defprotocol IDatabaseElement
  "A database element. Tables and rows."
  (get-changes [_] "Returns a map that shows how changes"))

(defn- row
  "Creates a row object.

   * `(:column-key row)`  
     -> a column-value
   * `(:many-to-one-relationship-key row)`  
     -> a parent row
   * `(:one-to-many-relatoinship-key row)`  
     -> sequence of child rows
   * `(assoc row :column-key column-value)`  
     the column value will be changed.
   * `(assoc row :many-to-one-relationship-key parent-row)`  
     row's foreign key will be changed and parent-row's changes will be merged.
   * `(assoc row :one-to-many-relationship-key sequence-of-rows)`  
     sorry, not allowed..."
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
           (map #(.entryAt this %))
           (not-empty)))
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
                                                      modified? (assoc :--modified? true))
                    :changes (cond-> (assoc-in changes [table-key (:key row-data) key] val)
                               modified? (-> (assoc-in [table-key (:key row-data) :--modified?] true)))))))))
    IDataHolder
    (get-data [_]
      row-data)
    IDatabaseElement
    (get-changes [_]
      changes)))

(defn- table
  "Creates a table object.

   For inserting a new row, please use below code.  
   `(assoc table (new-key) map)`  
   don't associate the key to map. relationship-key and physical-column-key are both allowed in map.

   For deleting a row, please use below code.  
   `(dissoc table row-key)`"
  [database table-key {:keys [many-to-one-relationships] :as table-schema} table-data & {:keys [changes]}]
  (reify
    clojure.lang.IPersistentMap
    (valAt [_ key]
      (if-let [row-data (get table-data key)]
        (if-not (:--deleted? row-data)
          (row database table-key table-schema row-data))))
    (entryAt [this key]
      (if-let [val (.valAt this key)]
        (clojure.lang.MapEntry. key val)))
    (seq [this]
      (->> (keys table-data)
           (keep #(.entryAt this %))
           (not-empty)))
    (assoc [_ key val]
      (assert (or (= (:key val) nil) (= (:key val) key)) "changing key is not supported.")
      (if (:key val)
        (table database table-key table-schema (assoc table-data key (get-data val)) :changes (merge-changes changes (get-changes val)))  ; TODO: table-dataにもchangesをマージする。
        (let [val (assoc (reduce-kv #(apply assoc
                                       %1 (cond-> [%2 %3]
                                            (contains? many-to-one-relationships %2) ((fn [[relationship-key row]]
                                                                                        [(many-to-one-relationship-key-to-physical-column-key relationship-key) (:key row)]))))
                                    {}
                                    val)
                         :key         key
                         :--inserted? (not (contains? table-data key))
                         :--modified? true)]
          (table database table-key table-schema (assoc table-data key val) :changes (assoc-in changes [table-key key] val)))))
    (without [this key]
      (if-let [row-data (get table-data key)]
        (table database table-key table-schema (dissoc table-data key) :changes (update-in changes [table-key key] #(assoc % :--deleted? true, :--modified true)))
        this))
    (count [_]
      (count table-data))
    (iterator [this]
      (.iterator (or (seq this) {})))
    IDataHolder
    (get-data [_]
      table-data)
    IDatabaseElement
    (get-changes [_]
      changes)))

(defprotocol IDatabase
  "The database in memory."
  (get-condition-matched-rows [_ table-key] "Returns rows that matches conditions.")
  (get-inserted-rows          [_]           "Returns inserted rows.")
  (get-modified-rows          [_]           "Returns modified rows.")
  (get-deleted-rows           [_]           "Returns deleted rows."))

(defn database
  "Creates a database object. Plese create database-data by database-data function."
  [database-schema & [database-data]]
  (letfn [(get-updated-rows [pred]
            (reduce-kv #(assoc %1 %2 (not-empty (reduce-kv (fn [result row-key row]
                                                             (cond-> result
                                                               (pred row) (assoc row-key row)))
                                                           {}
                                                           %3)))
                       {}
                       database-data))]
    (reify
      clojure.lang.IPersistentMap
      (valAt [this key]
        (table this key (get database-schema key) (get database-data key)))
      (entryAt [this key]
        (clojure.lang.MapEntry. key (.valAt this key)))
      (seq [this]
        (->> (keys database-schema)
             (map #(.entryAt this %))
             (not-empty)))
      (assoc [_ key val]
        (database database-schema (merge-changes database-data (get-changes val))))
      (iterator [this]
        (.iterator (or (seq this) {})))
      IDataHolder
      (get-data [this]
        database-data)
      IDatabase
      (get-condition-matched-rows [this table-key]
        (filter :--match-condition? (vals (get this table-key))))
      (get-inserted-rows [this]
        (get-updated-rows (every-pred :--inserted? (complement :--deleted?))))
      (get-modified-rows [this]
        (get-updated-rows (every-pred (complement :--inserted?) :--modified? (complement :--deleted?))))
      (get-deleted-rows [this]
        (get-updated-rows (every-pred (complement :--inserted?) :--deleted?))))))

;; Operators that can be used in get-data condition DSL.
(def ^:private operators
  (atom {}))

(defmacro ^:private defoperator
  "Define a new operator."
  [operator & [where-clause-fn sql-parameters-fn]]
  `(do (swap! operators assoc '~operator {:where-clause-fn   (or ~where-clause-fn   (fn [parameters#] (string/join ~(format " %s " (subs (string/upper-case (name operator)) 1)) parameters#)))
                                          :sql-parameters-fn (or ~sql-parameters-fn (fn [parameters#] (mapcat identity parameters#)))})
       (defn ~operator
         [& xs#]
         (apply vector '~operator xs#))))

;; Define operators.
(defoperator $and)
(defoperator $or)
(defoperator $not
  #(format "NOT %s" (first %))
  #(first %))
(defoperator $<)  ; TODO: dateとtimezoneを比較する場合を考慮する。timezoneの情報が抜けおちているので、簡単にはいかなそう。。。
(defoperator $>)
(defoperator $<=)
(defoperator $>=)
(defoperator $=)
(defoperator $<>)
(defoperator $is)
(defoperator $like)
(defoperator $in
  #(format "%s IN (%s)" (first %) (string/join ", " (second %)))
  #(second %))

(defn- wrap-select-*
  "Wrap one more select. For normalizing SQL to 'SELECT T.* FROM ...'. It's easy to use so many cases."
  [table-key sql]
  (<< "SELECT ~(sql-name table-key).* FROM (~{sql}) AS ~(sql-name table-key)"))

(defn- select-sql
  "Generates SELECT SQL for getting rows on a table. Defined operators can be used in condition.
   
   * If you want to select products whose name is 'x', use `($= :name \"x\")`.
   * If you want to select by another table's column value, use `($= :parent-key.children-key.column-key \"x\")`."
  [database-schema table-key & [condition]]
  (let [alias-number (atom 0)]
    (letfn [(normalize-property-key [table-key alias-key [property-key & more]]
              (if-let [[column-key next-table-key next-column-key] (or (if-let [relationship-schema (get-in database-schema [table-key :many-to-one-relationships property-key])]
                                                                         [(many-to-one-relationship-key-to-physical-column-key property-key) (:table-key relationship-schema) :key])
                                                                       (if-let [relationship-schema (get-in database-schema [table-key :one-to-many-relationships property-key])]
                                                                         [:key (:table-key relationship-schema) (many-to-one-relationship-key-to-physical-column-key (:many-to-one-relationship-key relationship-schema))]))]
                (let [next-alias-key (keyword (<< "--t-~(swap! alias-number inc)"))]
                  (-> (normalize-property-key next-table-key next-alias-key more)
                      (update-in [:join-clause] #(str (<< "LEFT JOIN ~(sql-name next-table-key) AS ~(sql-name next-alias-key) "
                                                          "ON (~(sql-name next-alias-key).~(sql-name next-column-key) = ~(sql-name alias-key).~(sql-name column-key)) ") %))))
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
                (coll? condition) (if-let [where-clause-fn (get-in @operators [(first condition) :where-clause-fn])]
                                    (format "(%s)" (where-clause-fn (map where-clause (next condition))))
                                    (map where-clause condition))
                :else             (if-not (nil? condition)
                                    "?"
                                    "NULL")))
            (sql-parameters [condition]
              (cond
                (map?  condition) nil
                (coll? condition) (if-let [sql-parameters-fn (get-in @operators [(first condition) :sql-parameters-fn])]
                                    (sql-parameters-fn (map sql-parameters (next condition)))
                                    (mapcat sql-parameters condition))
                :else             (if-not (nil? condition)
                                    [condition])))]
      (let [[join-clause where-clause sql-parameters] ((juxt join-clause where-clause sql-parameters) (normalize-condition (or condition true)))]  ; conditionが指定されない場合はWHERE NULLになってしまうので、必ず真になる値に置き換えます。
        (apply vector
          (wrap-select-* table-key (<< "SELECT DISTINCT ~(sql-name table-key).*, true AS \"--match-condition?\" FROM ~(sql-name table-key) ~{join-clause} WHERE ~{where-clause}"))
          sql-parameters)))))

(defn merge-map-to-database-data
  "Merge the jdbc/query result map to the database-data map."
  [database-data table-key map]
  (update-in database-data [table-key] #(reduce (fn [result row]
                                                  (cond-> result
                                                    (not (contains? result (:key row))) (assoc (:key row) row)))
                                                %
                                                map)))

(defn database-data
  "Getting data from RDBMS.

   Tracking back one-to-many-relationships and tracking back ALL many-to-one-relationships.
   
   When table-key is :products, the result contains order-details (children of product) and categories (parent of product), orders (parent of order-detail) and maybe more categories (if category has parent category).

   For watching executed sql, please set INFO to log4j log level."
  [database-schema database-spec table-key & [condition other-data]]
  (letfn [(as-recursive-select-sql [table-key sql]
            (let [recursive-relationship-keys (->> (get-in database-schema [table-key :many-to-one-relationships])
                                                   (keep #(if (= (:table-key (second %)) table-key)
                                                            (first %))))]
              (letfn [(to-recursive-sql [sql]
                        (let [table                   (sql-name table-key)
                              recursive-table         (sql-name (keyword (<< "--r-~(name table-key)")))
                              recursive-table-columns (->> (physical-column-keys (get database-schema table-key))
                                                           (map (fn [physical-column-key] (<< "~{recursive-table}.~(sql-name physical-column-key)")))
                                                           (string/join ", "))
                              join-on-clause          (->> recursive-relationship-keys
                                                           (map (fn [relationship-key] (<< "~{recursive-table}.~(sql-name (many-to-one-relationship-key-to-physical-column-key relationship-key)) = ~{table}.\"key\"")))
                                                           (string/join " OR "))]
                          (wrap-select-* table-key (<< "WITH RECURSIVE ~{recursive-table} AS ("
                                                       "SELECT ~{table}.*, array_agg(~{table}.\"key\") over() AS \"--visited\" FROM (~{sql}) AS ~{table} "
                                                       "UNION ALL "
                                                       "SELECT DISTINCT ~{table}.*, false, ~{recursive-table}.\"--visited\" || array_agg(~{table}.\"key\") over() "
                                                       "FROM ~{table} JOIN ~{recursive-table} ON ~{join-on-clause} "
                                                       "WHERE ~{table}.\"key\" <> ALL(\"--visited\")) "
                                                       "SELECT ~{recursive-table-columns}, ~{recursive-table}.\"--match-condition?\" FROM ~{recursive-table}"))))]
                (cond-> sql
                  (not-empty recursive-relationship-keys) (to-recursive-sql)))))
          (relationship-table-key-and-sqls [table-key sql relationship-type continue?-fn column-keys-fn]
            (->> (get-in database-schema [table-key relationship-type])
                 (keep (fn [[relationship-key relationship-schema]]
                         (if (continue?-fn relationship-key relationship-schema)
                           (let [[next-column-key previous-column-key] (column-keys-fn relationship-key relationship-schema)
                                 next-table-key                        (:table-key relationship-schema)
                                 next-table                            (sql-name next-table-key)
                                 next-sql                              (->> (wrap-select-* next-table-key (<< "SELECT ~{next-table}.*, false AS \"--match-condition?\" "
                                                                                                              "FROM ~{next-table} "
                                                                                                              "WHERE ~{next-table}.~(sql-name next-column-key) IN (~(string/replace-first sql \"*\" (sql-name previous-column-key)))"))
                                                                            (as-recursive-select-sql next-table-key))]
                             (cons [next-table-key next-sql] (many-to-one-relationship-table-key-and-sqls next-table-key next-sql))))))  ; 親方向を再帰で辿ります。マスター・テーブルが不足すると、データとして不完全すぎるためです。で、子方向は、データ量が大きくなりすぎるので、無視します……。
                 (apply concat)))
          (many-to-one-relationship-table-key-and-sqls [table-key sql]
            (relationship-table-key-and-sqls table-key sql
                                             :many-to-one-relationships
                                             (fn [relationship-key relationship-schema] (not= (:table-key relationship-schema) table-key))  ; 自己参照はWITH RECURSIVEで実現しますので、除外します。
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
  "Save all updates to RDBMS."
  [database-schema database database-spec]
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

;; TODO: typeにchoiceを追加する。

;; Types in database-schema.
(def ^:private ^:const database-types
  {:string    "text"
   :text      "text"
   :integer   "integer"
   :decimal   "decimal"
   :boolean   "boolean"
   :date      "timestamp"
   :timestamp "timestamp"})

(defn create-tables
  "Create tables."
  [database-schema database-spec]
  (letfn [(column-spec [[column-key {:keys [type precision scale constraint]}]]
            [column-key (format "%s %s"
                                (cond
                                  (= type :decimal) (format "%s(%d, %d)" (get database-types type) (or precision 30) (or scale 10))
                                  :else             (get database-types type))
                     (or constraint ""))])
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
  "Drop tables"
  [database-schema database-spec]
  (doseq [[table-key _] database-schema]
    (jdbc/db-do-commands database-spec (jdbc/drop-table-ddl table-key :entities (jdbc/quoted \")))))
