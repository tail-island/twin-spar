# twin-spar

Easy data access library for Clojure.

### No SQL.

```clojure
(let [products   (jdbc/query database-spec ["SELECT *
                                             FROM   \"products\"
                                             WHERE  \"products\".\"price\" > ?"
                                            1000])
      categories (jdbc/query database-spec ["SELECT DISTINCT \"categories\".*
                                             FROM   \"categories\"
                                             JOIN   \"products\" ON \"products\".\"category-key\" = \"categories\".\"key\"
                                             WHERE  \"products\".\"price\" > ?"
                                            1000])]
```

↓↓↓

```clojure
(let [database (my-database (my-database-data database-spec :products ($> :price 1000)))]
```

### No matching logics.

```clojure
(let [product (user-selected-product)]
  (println (:name product) (:name (some #(if (= (:key %) (:category-key product))
                                           %)
                                        categories))))
```

↓↓↓

```clojure
(let [product (user-selected-product)]
  (println (:name product) (:name (:category product))))
```

### Easy update.

```clojure
(let [product (user-selected-product)]
  (->> (map #(cond-> %
               (= (:key %) (:category-key product)) (assoc :name "NEW Category"))
            categories)
       (map :name)
       (println)))
```

↓↓↓

```clojure
(let [product (user-selected-product)]
  (->> (assoc-in database [:products (:key product) :category :name] "NEW Category")
       (:categories)
       (vals)
       (map :name)
       (println)))
```

### Automatic insert/update/delete.

```clojure
(let [product (user-updated-product)]
  (jdbc/update! database-spec :products product ["\"key\" = ?" (:key product)] :entities (jdbc/quoted \")))
```

↓↓↓

```clojure
(let [database (user-updated-database)]
  (my-save! database-spec database))
```

## Getting started

Add following to your <code>project.clj</code>.

```clojure
[com.tail-island/twin-spar "0.1.0"]
```

twin-spar uses log4j and writes so many logs... Please add following into <code>src/log4j.properties</code>.

```
log4j.rootLogger=WARN,X
log4j.appender.X=org.apache.log4j.ConsoleAppender
log4j.appender.X.layout=org.apache.log4j.PatternLayout
log4j.appender.X.layout.ConversionPattern=%d{HH:mm:ss} %p %t %c: %m%n
```

And sorry, twin-spar supperts only PostgreSQL... Please setup PostgreSQL.

## Usage

Import twin-spar to your namespace.

```clojure
(:require (twin-spar [core :refer :all]))
```

Define database schema.

```clojure
(def database-schema
  {:products   {:columns                   {:name                {:type      :string}
                                            :price               {:type      :decimal}}
                :many-to-one-relationships {:category            {:table-key :categories}}}
   :categories {:columns                   {:name                {:type      :string}}
                :many-to-one-relationships {:superior-category   {:table-key :categories}}
                :one-to-many-relationships {:inferior-categories {:table-key :categories, :many-to-one-relationship-key :superior-category}
                                            :products            {:table-key :products,   :many-to-one-relationship-key :category}}}})
```

Bind your database-schema to twin-spar functions.

```clojure
(def my-database-data
  (partial database-data database-schema))

(def my-database
  (partial database      database-schema))

(def my-save!
  (partial save!         database-schema))
```

Prepare RDBMS connection.

```clojure
(def database-spec
 {:subprotocol "postgresql"
  :subname     "database name"
  :user        "user"
  :password    "password"})
```

Create tables.

```
;; I drop/create tables on my test fixture.
(use-fixtures :each (fn [test-function]
                      (try
                        (drop-tables database-schema database-spec)
                        (catch Exception ex
                          (.printStackTrace ex)))
                      (create-table database-schema database-spec)

                      ;; Insert dummy data here.

                      (test-function)))
```

Get data from RDBMS.

```clojure
;; Getting products rows whose price larger than 1,000 and whose category's superior-category's superior-category's name is "foo".
(let [database (my-database (my-database-data database-spec :products ($and ($> :price 1000)
                                                                            ($= :category.superior-category.superior-category.name "foo"))))]

;; Necessary categories had been selected automatically. You can disconnect from RDBMS now.
```

Using data. You can use get, get-in, assoc, assoc-in, update-in and dissoc.

```clojure
(vals (get-in database [:products]))  ; Get all products.
(get-in database [:products (:key selected-product) :category :products])  ; Get same category products.

(-> database
    (assoc-in [:products (:key selected-product) :name] "FOO")  ; Change product's name.
    (assoc-in [:products (:key selected-product) :category :name] "BAR"))  ; Change product's category's name.

(get-in database [:products (:key selected-product) :name])  ; NOT "FOO". Original name is returned, because everything is immutable.
```

And save.

```clojure
(->> (-> databse
         (update-in ...)
         (update-in ...))
     (my-save! database-spec))

;; twin-spar execute insert/update/delete automatically.
```

## Documentation

Please wailt a while...

## License

Copyright © 2015 OJIMA Ryoji

Distributed under the Eclipse Public License either version 1.0 or any later version.
