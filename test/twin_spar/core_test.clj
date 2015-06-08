(ns twin-spar.core-test
  (:use     (twin-spar    core))
  (:require (clojure      [pprint :refer :all]
                          [string :as    string]
                          [test   :refer :all])
            (clojure.java [jdbc   :as    jdbc])
            (clj-time     [coerce :as    time.coerce])))

(deftest foo
  (pprint (#'twin-spar.core/rdbms-database-schema {:a {:columns         {:a1 {:type :string}}}
                                                   :b {:super-table-key :a
                                                       :columns         (array-map :b1 {:type :string}
                                                                                   :b2 {:type :string})}
                                                   :c {:super-table-key :a
                                                       :columns         {:c1 {:type :string}}}
                                                   :d {:super-table-key :b
                                                       :columns         {:d1 {:type :string}}}})))

;; TODO: productsにnote:textを追加する。

(def ^:private database-schema
  {:order-details                  {:columns                   {:quantity                       {:type      :integer}}
                                    :many-to-one-relationships {:order                          {:table-key :orders}
                                                                :product                        {:table-key :products}}}
   :orders                         {:columns                   {:at                             {:type      :timestamp}}
                                    :many-to-one-relationships {:customer                       {:table-key :customers}}
                                    :one-to-many-relationships {:order-details                  {:table-key :order-details, :many-to-one-relationship-key :order}}}
   :favorites                      {:many-to-one-relationships {:product                        {:table-key :products}
                                                                :customer                       {:table-key :customers}}}
   :charges                        {:many-to-one-relationships {:product                        {:table-key :products}
                                                                :employee                       {:table-key :employees}}}
   :products                       {:columns                   {:name                           {:type      :string}
                                                                :price                          {:type      :decimal,       :precision 10, :scale 2}}
                                    :many-to-one-relationships {:category                       {:table-key :categories}}
                                    :one-to-many-relationships {:charges                        {:table-key :charges,       :many-to-one-relationship-key :product}
                                                                :order-details                  {:table-key :order-details, :many-to-one-relationship-key :product}
                                                                :favorites                      {:table-key :favorites,     :many-to-one-relationship-key :product}}}
   :software-products              {:super-table-key           :products
                                    :many-to-one-relationships {:os                             {:table-key :os}}}
   :download-software-products     {:super-table-key           :software-products
                                    :columns                   {:uri                            {:type      :string}}}
   :hardware-products              {:super-table-key           :products
                                    :columns                   {:size                           {:type      :string}}}
   :composite-products             {:super-table-key           :products
                                    :one-to-many-relationships {:composite-products-to-products {:table-key :composite-products-to-products, :many-to-one-relationship-key :composite-product}}}
   :os                             {:columns                   {:name                           {:type      :string}}}
   :composite-products-to-products {:many-to-one-relationships {:composite-product              {:table-key :composite-products}
                                                                :prouct                         {:table-key :products}}}
   :customers                      {:columns                   {:name                           {:type      :string}
                                                                :vip?                           {:type      :boolean}}
                                    :one-to-many-relationships {:orders                         {:table-key :orders,                         :many-to-one-relationship-key :customer}
                                                                :favorites                      {:table-key :favorites,                      :many-to-one-relationship-key :customer}}}
   :employees                      {:columns                   {:name                           {:type      :string}}
                                    :many-to-one-relationships {:superior                       {:table-key :employees}
                                                                :tutor                          {:table-key :employees}
                                                                :organization                   {:table-key :organizations}}
                                    :one-to-many-relationships {:subordinates                   {:table-key :employees,                      :many-to-one-relationship-key :superior}
                                                                :tutees                         {:table-key :employees,                      :many-to-one-relationship-key :tutor}
                                                                :charges                        {:table-key :charges,                        :many-to-one-relationship-key :employee}}}
   :categories                     {:columns                   {:name                           {:type      :string}}
                                    :many-to-one-relationships {:superior-category              {:table-key :categories}}
                                    :one-to-many-relationships {:inferior-categories            {:table-key :categories,                     :many-to-one-relationship-key :superior-category}
                                                                :products                       {:table-key :products,                       :many-to-one-relationship-key :category}}}
   :organizations                  {:columns                   {:name                           {:type      :string}}
                                    :many-to-one-relationships {:superior-organization          {:table-key :organizations}}
                                    :one-to-many-relationships {:inferior-organizations         {:table-key :organizations,                  :many-to-one-relationship-key :superior-organization}
                                                                :employees                      {:table-key :employees,                      :many-to-one-relationship-key :organization}}}})

(def ^:private database-spec
  {:subprotocol "postgresql"
   :subname     "twin-spar"
   :user        "twin-spar"
   :password    "P@ssw0rd"})

(def ^:private ts-database-data
  (partial database-data database-schema))

(def ^:private ts-database
  (partial database database-schema))

(def ^:private ts-save!
  (partial save! database-schema))

(def ^:private default-modified-at
  (time.coerce/from-date #inst "2015-01-01T00:00:00+09:00"))

(def ^:private row-keys
  (repeatedly new-key))

(defn- row-key
  [index]
  (nth row-keys index))

(use-fixtures :each (fn [test-function]
                      (try
                        (drop-tables database-schema database-spec)
                        (catch Exception ex
                          (.printStackTrace ex)))
                          ;; ))
                      (create-tables database-schema database-spec)
                      (jdbc/with-db-transaction [transaction database-spec]
                        (jdbc/insert! transaction :organizations
                                      {:key (row-key 10), :name "o0", :superior-organization-key nil,          :modified-at default-modified-at}
                                      {:key (row-key 11), :name "o1", :superior-organization-key (row-key 10), :modified-at default-modified-at}
                                      {:key (row-key 12), :name "o2", :superior-organization-key (row-key 10), :modified-at default-modified-at}
                                      :entities (jdbc/quoted \"))
                        (jdbc/insert! transaction :categories
                                      {:key (row-key 20), :name "c0", :superior-category-key nil,          :modified-at default-modified-at}
                                      {:key (row-key 21), :name "c1", :superior-category-key (row-key 20), :modified-at default-modified-at}
                                      {:key (row-key 22), :name "c2", :superior-category-key (row-key 20), :modified-at default-modified-at}
                                      :entities (jdbc/quoted \"))
                        (jdbc/insert! transaction :employees
                                      {:key (row-key 30), :name "e0", :superior-key nil,          :tutor-key (row-key 34), :organization-key (row-key 10), :modified-at default-modified-at}
                                      {:key (row-key 31), :name "e1", :superior-key (row-key 30), :tutor-key (row-key 34), :organization-key (row-key 11), :modified-at default-modified-at}
                                      {:key (row-key 32), :name "e2", :superior-key (row-key 30), :tutor-key (row-key 34), :organization-key (row-key 12), :modified-at default-modified-at}
                                      {:key (row-key 33), :name "e3", :superior-key (row-key 31), :tutor-key (row-key 34), :organization-key (row-key 11), :modified-at default-modified-at}
                                      {:key (row-key 34), :name "e4", :superior-key (row-key 31), :tutor-key nil,          :organization-key (row-key 11), :modified-at default-modified-at}
                                      :entities (jdbc/quoted \"))
                        (jdbc/insert! transaction :customers
                                      {:key (row-key 40), :name "c0", :vip? true,  :modified-at default-modified-at}
                                      {:key (row-key 41), :name "c1", :vip? false, :modified-at default-modified-at}
                                      {:key (row-key 42), :name "c2", :vip? true,  :modified-at default-modified-at}
                                      {:key (row-key 43), :name "c3", :vip? false, :modified-at default-modified-at}
                                      {:key (row-key 44), :name "c4", :vip? true,  :modified-at default-modified-at}
                                      :entities (jdbc/quoted \"))
                        (jdbc/insert! transaction :products
                                      {:key (row-key 50), :name "p0", :price 1000, :category-key (row-key 20), :modified-at default-modified-at}
                                      {:key (row-key 51), :name "p1", :price 2000, :category-key (row-key 21), :modified-at default-modified-at}
                                      {:key (row-key 52), :name "p2", :price 3000, :category-key (row-key 21), :modified-at default-modified-at}
                                      {:key (row-key 53), :name "p3", :price 4000, :category-key (row-key 22), :modified-at default-modified-at}
                                      {:key (row-key 54), :name "p4", :price 5000, :category-key (row-key 22), :modified-at default-modified-at}
                                      :entities (jdbc/quoted \"))
                        (jdbc/insert! transaction :charges
                                      {:key (row-key 60), :product-key (row-key 50), :employee-key (row-key 33), :modified-at default-modified-at}
                                      {:key (row-key 61), :product-key (row-key 51), :employee-key (row-key 33), :modified-at default-modified-at}
                                      {:key (row-key 62), :product-key (row-key 52), :employee-key (row-key 33), :modified-at default-modified-at}
                                      {:key (row-key 63), :product-key (row-key 53), :employee-key (row-key 34), :modified-at default-modified-at}
                                      {:key (row-key 64), :product-key (row-key 54), :employee-key (row-key 34), :modified-at default-modified-at}
                                      :entities (jdbc/quoted \"))
                        (jdbc/insert! transaction :favorites
                                      {:key (row-key 70), :product-key (row-key 50), :customer-key (row-key 40), :modified-at default-modified-at}
                                      {:key (row-key 71), :product-key (row-key 51), :customer-key (row-key 41), :modified-at default-modified-at}
                                      :entities (jdbc/quoted \"))
                        (jdbc/insert! transaction :orders
                                      {:key (row-key 80), :at default-modified-at, :customer-key (row-key 42), :modified-at default-modified-at}
                                      {:key (row-key 81), :at (time.coerce/from-date #inst "2015-01-01T00:00:01+09:00"), :customer-key (row-key 43), :modified-at default-modified-at}
                                      :entities (jdbc/quoted \"))
                        (jdbc/insert! transaction :order-details
                                      {:key (row-key 90), :quantity 10, :order-key (row-key 80), :product-key (row-key 52), :modified-at default-modified-at}
                                      {:key (row-key 91), :quantity 11, :order-key (row-key 80), :product-key (row-key 53), :modified-at default-modified-at}
                                      {:key (row-key 92), :quantity 12, :order-key (row-key 81), :product-key (row-key 54), :modified-at default-modified-at}
                                      :entities (jdbc/quoted \")))
                      (test-function)))

(deftest on-memory-test
  (let [database (ts-database (jdbc/with-db-transaction [transaction database-spec]
                                (reduce #(merge-map-to-database-data %1 %2 (jdbc/query transaction [(format "SELECT * FROM %s" (#'twin-spar.core/sql-name %2))]))
                                        {}
                                        [:organizations :employees :categories :products :charges :orders :order-details])))]
    
    (testing "reading values"
      (is (= "e0" (get-in database [:employees (row-key 30) :name])))
      (is (= "e0" (get-in database [:employees (row-key 31) :superior :name])))
      (is (= "e0" (get-in database [:employees (row-key 33) :superior :superior :name])))
      (is (= ["e1" "e2"]
             (sort (map :name (get-in database [:employees (row-key 30) :subordinates])))))
      (is (= "o0" (get-in database [:employees (row-key 30) :organization :name])))
      (is (= ["e0"]
             (sort (map :name (get-in database [:organizations (row-key 10) :employees]))))))
    
    (testing "writing values"
      (let [database (-> database
                         (assoc-in  [:products (row-key 50) :name]  "P0")
                         (assoc-in  [:products (row-key 50) :price] 1001)
                         (update-in [:products (row-key 51)]        #(assoc % :name "P1" :price 2001)))]
        (is (= "P0" (get-in database [:products (row-key 50) :name])))
        (is (= 1001 (get-in database [:products (row-key 50) :price])))
        (is (= "P1" (get-in database [:products (row-key 51) :name])))
        (is (= 2001 (get-in database [:products (row-key 51) :price])))
        (is (= "p2" (get-in database [:products (row-key 52) :name]))))
      (is (= "p0" (get-in database [:products (row-key 50) :name])))

      (let [database (assoc-in database [:employees (row-key 31) :superior :name] "E0")]
        (is (= "E0"
               (get-in database [:employees (row-key 31) :superior :name])
               (get-in database [:employees (row-key 30) :name]))))
      
      (let [database (assoc-in database [:employees (row-key 30) :organization :name] "O0")]
        (is (= "O0"
               (get-in database [:employees (row-key 30) :organization :name])
               (get-in database [:organizations (row-key 10) :name]))))
      
      (let [database (-> database
                         (dissoc-in [:employees     (row-key 30)])  ; この操作は、参照整合性を壊します。実際のアプリケーションでは、参照整合性を壊さないように更新してください。
                         (dissoc-in [:organizations (row-key 10)])
                         (dissoc-in [:organizations (row-key 11)]))]
        
        (is (nil?      (get-in database [:employees     (row-key 30)])))
        (is (not (nil? (get-in database [:employees     (row-key 31)]))))
        (is (nil?      (get-in database [:organizations (row-key 10)])))
        (is (nil?      (get-in database [:organizations (row-key 11)])))
        (is (not (nil? (get-in database [:organizations (row-key 12)]))))

        (is (nil? (some #(= % (row-key 30)) (keys (get-in database [:employees])))))
        (is (nil? (some #(= % (row-key 10)) (keys (get-in database [:organizations])))))
        (is (nil? (some #(= % (row-key 11)) (keys (get-in database [:organizations]))))))
      
      ;; one-to-many-relationshipは更新できません（relationshipが切れてしまうrowが発生してしまうため）。以下のようなコードで更新してください。
      (let [subordinates (->> (get-in database [:employees (row-key 31) :subordinates])
                              (map #(update-in % [:name] string/upper-case)))
            tutees       (->> (get-in database [:employees (row-key 34) :tutees])
                              (map #(update-in % [:name] string/upper-case)))
            database     (update-in database [:employees] #(reduce (fn [result subordinate]
                                                                     (assoc result (:key subordinate) subordinate))
                                                                   %
                                                                   subordinates))]
        (is (= "E3" (get-in database [:employees (row-key 33) :name])))
        (is (= "E4" (get-in database [:employees (row-key 34) :name])))
        (is (= "e2" (get-in database [:employees (row-key 32) :name]))))  ; tuteesの変更結果はdatabaseにassocしていませんから、変更は反省されません。
      
      ;; relationshipの変更は、databaseまで戻らないと反映されません（mapの場合と同じ動作です）。
      (let [employee (assoc-in (:employees database) [(row-key 31) :superior :name] "E0")]
        (is (= "e0"  ; NOT "E0"
               (get-in database [:employees (row-key 31) :superior :name])
               (get-in database [:employees (row-key 30) :name])))))))

  (deftest reading-test
    (jdbc/with-db-transaction [transaction database-spec]
      ;; (pprint (ts-database-data transaction :products))
      ;; (pprint (ts-database-data transaction :products ($in :charges.employee.tutees.superior.key [(row-key 30) (row-key 31)])))
      ;; (pprint (ts-database-data transaction :products ($is :name nil)))
      ;; (pprint (ts-database-data transaction :products ($not ($is :name nil))))
      ;; (pprint (ts-database-data transaction :orders ($<= :at (time.coerce/from-date #inst "2015-01-01T00:00:01+09:00")))) 
      (let [database (ts-database (ts-database-data transaction :employees ($= :name "e1")))]
        (is (= "e1" (get-in database [:employees (row-key 31) :name])))
        (is (= "e0" (get-in database [:employees (row-key 31) :superior :name])))
        (is (= "o0" (get-in database [:employees (row-key 31) :superior :organization :name])))
        (is (= ["e3" "e4"]
               (sort (map :name (get-in database [:employees (row-key 31) :subordinates])))))
        (is (empty? (-> (->> (-> database  ; 子を取得するのは、対象テーブルのみです。だから、データベース上にデータがあっても、空になります。
                                 (get-in [:employees (row-key 31) :subordinates]))
                             (sort-by :name)
                             (first))
                        (get-in [:charges]))))
        (is (= "e4"  ; 子の親は、再帰的に辿って取得されます。
               (-> (->> (-> database
                            (get-in [:employees (row-key 31) :subordinates]))
                        (sort-by :name)
                        (first))
                   (get-in [:tutor :name]))))
        (is (= ["e1"]  ; 親の子は取得されないので、対象そのもののe1だけが取得でき、e2は取得できません。
               (sort (map :name (get-in database [:employees (row-key 31) :superior :subordinates]))))))
      (let [database (ts-database (->> (ts-database-data transaction :customers ($= :name "c0"))
                                       (ts-database-data transaction :customers ($= :name "c1"))))]
        (is (= "c0" (get-in database [:customers (row-key 40) :name])))
        (is (= "c1" (get-in database [:customers (row-key 41) :name])))
        (is (nil?   (get-in database [:customers (row-key 42) :name]))))
      (let [database (ts-database (ts-database-data transaction :products ($or ($= :favorites.customer.name "c1")
                                                                               :order-details.order.customer.vip?)))]
        (is (= ["p1" "p2" "p3"]  ; customer c1 -> favorites p1, customer c2(vip) -> ordered p2 and p3, customer c3(NOT vip) -> ordered p4.
               (sort (map :name (vals (get-in database [:products])))))))
      (let [database (ts-database (ts-database-data transaction :employees ($or ($= :name "e1") ($= :name "e2"))))]
        (is (= ["e1" "e2"]
               (sort (map :name (get-condition-matched-rows database :employees)))))
        (is (= ["e0" "e1" "e2" "e3" "e4"]
               (sort (map :name (vals (:employees database)))))))))

  (deftest writing-test
    (jdbc/with-db-transaction [transaction database-spec]
      (let [database (ts-database (ts-database-data transaction :employees))]
        (is (= "e0" (get-in database [:employees (row-key 30) :name])))
        (is (= "e0" (get-in database [:employees (row-key 32) :superior :name])))))
    (jdbc/with-db-transaction [transaction database-spec]
      (let [database (ts-database (ts-database-data transaction :employees))]
        (-> database
            (assoc-in [:employees (row-key 30) :name]     "E0")
            (assoc-in [:employees (row-key 32) :superior] (get-in database [:employees (row-key 31)]))
            (assoc-in [:employees (row-key 33)] {:name             "E3"  ; 実際のコードでは、row-keyではなくnew-keyを使用してください。
                                                 :superior-key     (row-key 32)
                                                 :tutor-key        (row-key 34)
                                                 :organization-key (row-key 12)})
            (assoc-in [:employees (row-key 34)] {:name             "E4"
                                                 :superior         (get-in database [:employees     (row-key 32)])
                                                 :tutor            nil
                                                 :organization     (get-in database [:organizations (row-key 12)])})
            (assoc-in [:employees (row-key 35)] {:name             "e5"  ; 実際のコードでは、row-keyではなくnew-keyを使用してください。
                                                 :superior         (get-in database [:employees     (row-key 32)])
                                                 :tutor            (get-in database [:employees     (row-key 31)])
                                                 :organization     (get-in database [:organizations (row-key 12)])})
            (assoc-in [:employees (row-key 36)] {:name             "e6"
                                                 :superior-key     (row-key 32)
                                                 :tutor-key        (row-key 31)
                                                 :organization-key (row-key 12)})
            (ts-save! transaction))))
    (jdbc/with-db-transaction [transaction database-spec]
      (let [database (ts-database (ts-database-data transaction :employees))]
        (is (= "E0" (get-in database [:employees (row-key 30) :name])))
        (is (= "e1" (get-in database [:employees (row-key 32) :superior :name])))
        (is (= "E3" (get-in database [:employees (row-key 33) :name])))
        (is (= "e2" (get-in database [:employees (row-key 33) :superior :name])))
        (is (= "E4" (get-in database [:employees (row-key 34) :name])))
        (is (= "e2" (get-in database [:employees (row-key 34) :superior :name])))
        (is (= "e5" (get-in database [:employees (row-key 35) :name])))
        (is (= "e2" (get-in database [:employees (row-key 35) :superior :name])))
        (is (= "e6" (get-in database [:employees (row-key 36) :name])))
        (is (= "e2" (get-in database [:employees (row-key 36) :superior :name]))))))

;; TODO: 削除のテストを追加する！

  ;; (deftest without-twin-spar-sample
  ;;   (let [products   (jdbc/query database-spec ["SELECT *
  ;;                                                FROM   \"products\"
  ;;                                                WHERE  \"products\".\"price\" > ?"
  ;;                                               1000])
  ;;         categories (jdbc/query database-spec ["SELECT DISTINCT \"categories\".*
  ;;                                                FROM   \"categories\"
  ;;                                                JOIN   \"products\" ON \"products\".\"category-key\" = \"categories\".\"key\"
  ;;                                                WHERE  \"products\".\"price\" > ?"
  ;;                                               1000])]
  
  ;;     (let [product (first products)]
  ;;       (println (:name product) (:name (some #(and (= (:key %) (:category-key product)) %)
  ;;                                             categories))))

  ;;     (let [product (first products)]
  ;;       (->> (map #(cond-> %
  ;;                    (= (:key %) (:category-key product)) (assoc :name "NEW Category"))
  ;;                 categories)
  ;;            (map :name)
  ;;            (println)))

  ;;     (let [product (assoc (first products) :name "XXX")]
  ;;       (jdbc/update! database-spec :products product ["\"key\" = ?" (:key product)] :entities (jdbc/quoted \")))
  ;;     )

  ;;   (pprint (jdbc/query database-spec ["SELECT * FROM \"products\""])))

  ;; (deftest with-twin-spar-sample
  ;;   (let [my-database ts-database
  ;;         my-database-data ts-database-data
  ;;         my-save! ts-save!]
  
  ;;     (let [database (my-database (my-database-data database-spec :products ($> :price 1000)))]

  ;;       (let [product (first (vals (:products database)))]
  ;;         (println (:name product) (:name (:category product))))
  
  ;;       (let [product (first (vals (:products database)))]
  ;;         (->> (assoc-in database [:products (:key product) :category :name] "NEW Category")
  ;;              (:categories)
  ;;              (vals)
  ;;              (map :name)
  ;;              (println)))

  ;;       (let [product  (first (vals (:products database)))
  ;;             database (assoc-in database [:products (:key product) :name] "XXX")]
  ;;         (my-save! database database-spec))
  ;;       )

  ;;     (pprint (jdbc/query database-spec ["SELECT * FROM \"products\""]))))
