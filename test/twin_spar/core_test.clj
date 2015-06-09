(ns twin-spar.core-test
  (:use     (twin-spar    core))
  (:require (clojure      [pprint :refer :all]
                          [string :as    string]
                          [test   :refer :all])
            (clojure.java [jdbc   :as    jdbc])
            (clj-time     [coerce :as    time.coerce])
            (stem-bearing [core   :refer :all]))
  (:import  (java.sql     SQLException)))

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
   :composite-products-to-products {:many-to-one-relationships {:composite-product              {:table-key :composite-products}
                                                                :product                        {:table-key :products}}}
   :products                       {:columns                   {:name                           {:type      :string}
                                                                :price                          {:type      :decimal,       :precision 10, :scale 2}}
                                    :many-to-one-relationships {:category                       {:table-key :categories}}
                                    :one-to-many-relationships {:charges                        {:table-key :charges,       :many-to-one-relationship-key :product}
                                                                :order-details                  {:table-key :order-details, :many-to-one-relationship-key :product}
                                                                :favorites                      {:table-key :favorites,     :many-to-one-relationship-key :product}}}
   :software-products              {:super-table-key           :products
                                    :many-to-one-relationships {:operating-system               {:table-key :operating-systems}}}
   :download-software-products     {:super-table-key           :software-products
                                    :columns                   {:uri                            {:type      :string}}}
   :hardware-products              {:super-table-key           :products
                                    :columns                   {:size                           {:type      :string}}}
   :composite-products             {:super-table-key           :products
                                    :one-to-many-relationships {:composite-products-to-products {:table-key :composite-products-to-products, :many-to-one-relationship-key :composite-product}}}
   :operating-systems              {:columns                   {:name                           {:type      :string}}}
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
   :programmers                    {:super-table-key           :employees
                                    :columns                   {:favorite-language              {:type      :string}}}
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
                        (catch SQLException ex))
                      (create-tables database-schema database-spec)
                      (jdbc/with-db-transaction [transaction database-spec]
                        (jdbc/insert! transaction :organizations
                                      {:key (row-key 100), :name "organization 0", :superior-organization-key nil,           :modified-at default-modified-at}
                                      {:key (row-key 101), :name "organization 1", :superior-organization-key (row-key 100), :modified-at default-modified-at}
                                      {:key (row-key 102), :name "organization 2", :superior-organization-key (row-key 100), :modified-at default-modified-at}
                                      :entities (jdbc/quoted \"))
                        (jdbc/insert! transaction :categories
                                      {:key (row-key 110), :name "customer 0", :superior-category-key nil,           :modified-at default-modified-at}
                                      {:key (row-key 111), :name "customer 1", :superior-category-key (row-key 110), :modified-at default-modified-at}
                                      {:key (row-key 112), :name "customer 2", :superior-category-key (row-key 110), :modified-at default-modified-at}
                                      :entities (jdbc/quoted \"))
                        (jdbc/insert! transaction :employees
                                      {:key (row-key 120), :type "employees",   :name "employee 0", :superior-key nil,           :tutor-key (row-key 124), :organization-key (row-key 100), :modified-at default-modified-at}
                                      {:key (row-key 121), :type "employees",   :name "employee 1", :superior-key (row-key 120), :tutor-key (row-key 124), :organization-key (row-key 101), :modified-at default-modified-at}
                                      {:key (row-key 122), :type "programmers", :name "employee 2", :superior-key (row-key 120), :tutor-key (row-key 124), :organization-key (row-key 102), :modified-at default-modified-at}
                                      {:key (row-key 123), :type "programmers", :name "employee 3", :superior-key (row-key 121), :tutor-key (row-key 124), :organization-key (row-key 101), :modified-at default-modified-at}
                                      {:key (row-key 124), :type "programmers", :name "employee 4", :superior-key (row-key 121), :tutor-key nil,           :organization-key (row-key 101), :modified-at default-modified-at}
                                      :entities (jdbc/quoted \"))
                        (jdbc/insert! transaction :customers
                                      {:key (row-key 130), :name "customer 0", :vip? true,  :modified-at default-modified-at}
                                      {:key (row-key 131), :name "customer 1", :vip? false, :modified-at default-modified-at}
                                      {:key (row-key 132), :name "customer 2", :vip? true,  :modified-at default-modified-at}
                                      {:key (row-key 133), :name "customer 3", :vip? false, :modified-at default-modified-at}
                                      {:key (row-key 134), :name "customer 4", :vip? true,  :modified-at default-modified-at}
                                      :entities (jdbc/quoted \"))
                        (jdbc/insert! transaction :operating-systems
                                      {:key (row-key 140), :name "operating-system 0"}
                                      {:key (row-key 141), :name "operating-system 1"}
                                      {:key (row-key 142), :name "operating-system 2"}
                                      :entities (jdbc/quoted \"))
                        (jdbc/insert! transaction :products
                                      {:key (row-key 150), :type "software-products",          :name "product 0", :price 1000, :category-key (row-key 110), :operating-system-key (row-key 140),                                                     :modified-at default-modified-at}
                                      {:key (row-key 151), :type "software-products",          :name "product 1", :price 2000, :category-key (row-key 111), :operating-system-key (row-key 141),                                                     :modified-at default-modified-at}
                                      {:key (row-key 152), :type "download-software-products", :name "product 2", :price 3000, :category-key (row-key 111), :operating-system-key (row-key 142), :uri "http://tail-island.com",                      :modified-at default-modified-at}
                                      {:key (row-key 153), :type "hardware-products",          :name "product 3", :price 4000, :category-key (row-key 112),                                                                     :size "100x200 1kg", :modified-at default-modified-at}
                                      {:key (row-key 154), :type "composite-products",         :name "product 4", :price 5000, :category-key (row-key 112),                                                                                          :modified-at default-modified-at}
                                      :entities (jdbc/quoted \"))
                        (jdbc/insert! transaction :composite-products-to-products
                                      {:key (row-key 160), :composite-product-key (row-key 154), :product-key (row-key 151), :modified-at default-modified-at}
                                      {:key (row-key 161), :composite-product-key (row-key 154), :product-key (row-key 153), :modified-at default-modified-at}
                                      :entities (jdbc/quoted \"))
                        (jdbc/insert! transaction :charges
                                      {:key (row-key 170), :product-key (row-key 150), :employee-key (row-key 123), :modified-at default-modified-at}
                                      {:key (row-key 171), :product-key (row-key 151), :employee-key (row-key 123), :modified-at default-modified-at}
                                      {:key (row-key 172), :product-key (row-key 152), :employee-key (row-key 123), :modified-at default-modified-at}
                                      {:key (row-key 173), :product-key (row-key 153), :employee-key (row-key 124), :modified-at default-modified-at}
                                      {:key (row-key 174), :product-key (row-key 154), :employee-key (row-key 124), :modified-at default-modified-at}
                                      :entities (jdbc/quoted \"))
                        (jdbc/insert! transaction :favorites
                                      {:key (row-key 180), :product-key (row-key 150), :customer-key (row-key 130), :modified-at default-modified-at}
                                      {:key (row-key 181), :product-key (row-key 151), :customer-key (row-key 131), :modified-at default-modified-at}
                                      :entities (jdbc/quoted \"))
                        (jdbc/insert! transaction :orders
                                      {:key (row-key 190), :at (time.coerce/from-date #inst "2015-01-02T00:00:00+09:00")  :customer-key (row-key 132), :modified-at default-modified-at}
                                      {:key (row-key 191), :at (time.coerce/from-date #inst "2015-01-02T00:00:01+09:00"), :customer-key (row-key 133), :modified-at default-modified-at}
                                      :entities (jdbc/quoted \"))
                        (jdbc/insert! transaction :order-details
                                      {:key (row-key 190), :quantity 10, :order-key (row-key 190), :product-key (row-key 152), :modified-at default-modified-at}
                                      {:key (row-key 191), :quantity 11, :order-key (row-key 190), :product-key (row-key 153), :modified-at default-modified-at}
                                      {:key (row-key 192), :quantity 12, :order-key (row-key 191), :product-key (row-key 154), :modified-at default-modified-at}
                                      :entities (jdbc/quoted \")))
                      (test-function)))

(deftest on-memory-test
  (let [database (ts-database (jdbc/with-db-transaction [transaction database-spec]
                                (reduce #(merge-rows-to-database-data %1 %2 (jdbc/query transaction [(format "SELECT * FROM %s" (#'twin-spar.core/sql-name %2))]))
                                        {}
                                        [:organizations :categories :employees :customers :operating-systems :products :composite-products-to-products :charges :favorites :orders :order-details])))]
    
    (testing "reading values"
      (is (= "employee 0"
             (get-in database [:employees (row-key 120) :name])))
      (is (= "employee 0"
             (get-in database [:employees (row-key 121) :superior :name])))
      (is (= "employee 0"
             (get-in database [:employees (row-key 123) :superior :superior :name])))
      (is (= ["employee 1" "employee 2"]
             (sort (map :name (get-in database [:employees (row-key 120) :subordinates])))))
      (is (= "organization 0"
             (get-in database [:employees (row-key 120) :organization :name])))
      (is (= ["employee 0"]
             (sort (map :name (get-in database [:organizations (row-key 100) :employees]))))))
    
    (testing "writing values"
      (let [database (-> database
                         (assoc-in  [:products (row-key 150) :name]  "PRODUCT 0")
                         (assoc-in  [:products (row-key 150) :price] 1001)
                         (update-in [:products (row-key 151)]        #(assoc % :name "PRODUCT 1" :price 2001)))]
        (is (= "PRODUCT 0"
               (get-in database [:products (row-key 150) :name])))
        (is (= 1001
               (get-in database [:products (row-key 150) :price])))
        (is (= "PRODUCT 1"
               (get-in database [:products (row-key 151) :name])))
        (is (= 2001
               (get-in database [:products (row-key 151) :price])))
        (is (= "product 2"
               (get-in database [:products (row-key 152) :name]))))
      (is (= "product 0"
             (get-in database [:products (row-key 150) :name])))

      (let [database (assoc-in database [:employees (row-key 121) :superior :name] "EMPLOYEE 1")]
        (is (= "EMPLOYEE 1"
               (get-in database [:employees (row-key 121) :superior :name])
               (get-in database [:employees (row-key 120) :name]))))
      
      (let [database (assoc-in database [:employees (row-key 120) :organization :name] "ORGANIZATION 0")]
        (is (= "ORGANIZATION 0"
               (get-in database [:employees (row-key 120) :organization :name])
               (get-in database [:organizations (row-key 100) :name]))))
      
      (let [database (-> database
                         (dissoc-in [:employees     (row-key 120)])  ; この操作は、参照整合性を壊します。実際のアプリケーションでは、参照整合性を壊さないように更新してください。
                         (dissoc-in [:organizations (row-key 100)])
                         (dissoc-in [:organizations (row-key 101)]))]
        
        (is (nil?      (get-in database [:employees     (row-key 120)])))
        (is (not (nil? (get-in database [:employees     (row-key 121)]))))
        (is (nil?      (get-in database [:organizations (row-key 100)])))
        (is (nil?      (get-in database [:organizations (row-key 101)])))
        (is (not (nil? (get-in database [:organizations (row-key 102)]))))
        
        (is (nil? (some #(= % (row-key 120)) (keys (get-in database [:employees])))))
        (is (nil? (some #(= % (row-key 100)) (keys (get-in database [:organizations])))))
        (is (nil? (some #(= % (row-key 101)) (keys (get-in database [:organizations]))))))
      
      ;; one-to-many-relationshipは更新できません（relationshipが切れてしまうrowが発生してしまうため）。以下のようなコードで更新してください。
      (let [subordinates (->> (get-in database [:employees (row-key 121) :subordinates])
                              (map #(update-in % [:name] string/upper-case)))
            tutees       (->> (get-in database [:employees (row-key 124) :tutees])
                              (map #(update-in % [:name] string/upper-case)))
            database     (update-in database [:employees] #(reduce (fn [result subordinate]
                                                                     (assoc result (:key subordinate) subordinate))
                                                                   %
                                                                   subordinates))]
        (is (= "EMPLOYEE 3"
               (get-in database [:employees (row-key 123) :name])))
        (is (= "EMPLOYEE 4"
               (get-in database [:employees (row-key 124) :name])))
        (is (= "employee 2"
               (get-in database [:employees (row-key 122) :name]))))  ; tuteesの変更結果はdatabaseにassocしていませんから、変更は反省されません。
      
      ;; relationshipの変更は、databaseまで戻らないと反映されません（mapの場合と同じ動作です）。
      (let [employee (assoc-in (:employees database) [(row-key 121) :superior :name] "EMPLOYEE 0")]
        (is (= "employee 0"  ; NOT "EMPLOYEE 0"
               (get-in database [:employees (row-key 121) :superior :name])
               (get-in database [:employees (row-key 120) :name])))))

    (testing "single table inheritance"
      (is (= ["product 0" "product 1" "product 2" "product 3" "product 4"]
             (sort (map :name (vals (get-in database [:products]))))))
      (is (= ["product 0" "product 1" "product 2"]
             (sort (map :name (vals (get-in database [:software-products]))))))
      (is (= ["product 2"]
             (sort (map :name (vals (get-in database [:download-software-products]))))))
      
      (is (= "software-products"
             (-> database
                 (assoc-in [:software-products (row-key 155)] {:name "product 5", :price 6000, :category-key (row-key 110), :operating-system-key (row-key 140)})
                 (get-in   [:software-products (row-key 155) :type]))))
      
      (is (= "HTTP://TAIL-ISLAND.COM"
             (-> database
                 (assoc-in [:download-software-products (row-key 152) :uri] "HTTP://TAIL-ISLAND.COM")
                 (get-in   [:download-software-products (row-key 152) :uri]))))
      (is (thrown-with-msg? java.lang.AssertionError #"row can assoc with only columns and many-to-one-relationships"
            (assoc-in database [:software-products (row-key 152) :uri] "HTTP://TAIL-ISLAND.COM")))
      (is (= "OPERATING-SYSTEM 0"
             (-> database
                 (assoc-in [:software-products (row-key 150) :operating-system :name] "OPERATING-SYSTEM 0")
                 (get-in   [:software-products (row-key 150) :operating-system :name]))))
      (is (thrown-with-msg? java.lang.AssertionError #"row can assoc with only columns and many-to-one-relationships"
            (assoc-in database [:products (row-key 150) :operating-system :name] "OPERATING-SYSTEM 0")))
      
      
      (is (= "http://tail-island.com"
             (get-in database [:software-products (row-key 152) :uri])))                      ; カラムへのアクセスに特別な制限をかけていないので、値が取れてしまいます。
      (is (= "operating-system 0"
             (get-in database [:software-products (row-key 150) :operating-system :name])))
      (is (= nil
             (get-in database [:products          (row-key 150) :operating-system :name])))   ; ただし、リレーションシップは別。リレーションシップ向けの特別な処理が動かないためです。
      (is (= (row-key 140)
             (get-in database [:products          (row-key 150) :operating-system-key]))))))  ; キーは、カラム同様に値を取れてしまいます。

(deftest reading-test
  (jdbc/with-db-transaction [transaction database-spec]
    ;; (pprint (ts-database-data transaction :products))
    ;; (pprint (ts-database-data transaction :products ($in :charges.employee.tutees.superior.key [(row-key 120) (row-key 121)])))
    ;; (pprint (ts-database-data transaction :products ($is :name nil)))
    ;; (pprint (ts-database-data transaction :products ($not ($is :name nil))))
    ;; (pprint (ts-database-data transaction :orders ($<= :at (time.coerce/from-date #inst "2015-01-02T00:00:01+09:00")))) 
    (let [database (ts-database (ts-database-data transaction :employees ($= :name "employee 1")))]
      (is (= "employee 1"
             (get-in database [:employees (row-key 121) :name])))
      (is (= "employee 0"
             (get-in database [:employees (row-key 121) :superior :name])))
      (is (= "organization 0"
             (get-in database [:employees (row-key 121) :superior :organization :name])))
      (is (= ["employee 3" "employee 4"]
             (sort (map :name (get-in database [:employees (row-key 121) :subordinates])))))
      (is (empty? (-> (->> (-> database  ; 子を取得するのは、対象テーブルのみです。だから、データベース上にデータがあっても、空になります。
                               (get-in [:employees (row-key 121) :subordinates]))
                           (sort-by :name)
                           (first))
                      (get-in [:charges]))))
      (is (= "employee 4"  ; 子の親は、再帰的に辿って取得されます。
             (-> (->> (-> database
                          (get-in [:employees (row-key 121) :subordinates]))
                      (sort-by :name)
                      (first))
                 (get-in [:tutor :name]))))
      (is (= ["employee 1"]  ; 親の子は取得されないので、対象そのもののe1だけが取得でき、e2は取得できません。
             (sort (map :name (get-in database [:employees (row-key 121) :superior :subordinates]))))))
    (let [database (ts-database (->> (ts-database-data transaction :customers ($= :name "customer 0"))
                                     (ts-database-data transaction :customers ($= :name "customer 1"))))]
      (is (= "customer 0"
             (get-in database [:customers (row-key 130) :name])))
      (is (= "customer 1"
             (get-in database [:customers (row-key 131) :name])))
      (is (= nil
             (get-in database [:customers (row-key 132) :name]))))
    (let [database (ts-database (ts-database-data transaction :products ($or ($= :favorites.customer.name "customer 1")
                                                                             :order-details.order.customer.vip?)))]
      (is (= ["product 1" "product 2" "product 3"]  ; customer 1 -> favorites product 1, customer 2(vip) -> ordered product 2 and product 3, customer 3(NOT vip) -> ordered product 4.
             (sort (map :name (vals (get-in database [:products])))))))
    (let [database (ts-database (ts-database-data transaction :employees ($or ($= :name "employee 1") ($= :name "employee 2"))))]
      (is (= ["employee 1" "employee 2"]
             (sort (map :name (get-condition-matched-rows database :employees)))))
      (is (= ["employee 0" "employee 1" "employee 2" "employee 3" "employee 4"]
             (sort (map :name (vals (:employees database)))))))))

(deftest writing-test
  (jdbc/with-db-transaction [transaction database-spec]
    (let [database (ts-database (ts-database-data transaction :employees))]
      (is (= "employee 0"
             (get-in database [:employees (row-key 120) :name])))
      (is (= "employee 0"
             (get-in database [:employees (row-key 122) :superior :name])))))
  (jdbc/with-db-transaction [transaction database-spec]
    (let [database (ts-database (ts-database-data transaction :employees))]
      (-> database
          (assoc-in [:employees (row-key 120) :name]     "EMPLOYEE 0")
          (assoc-in [:employees (row-key 122) :superior] (get-in database [:employees (row-key 121)]))
          (assoc-in [:employees (row-key 123)] {:name             "EMPLOYEE 3"  ; 実際のコードでは、row-keyではなくnew-keyを使用してください。
                                                :superior-key     (row-key 122)
                                                :tutor-key        (row-key 124)
                                                :organization-key (row-key 102)})
          (assoc-in [:employees (row-key 124)] {:name             "EMPLOYEE 4"
                                                :superior         (get-in database [:employees     (row-key 122)])
                                                :tutor            nil
                                                :organization     (get-in database [:organizations (row-key 102)])})
          (assoc-in [:employees (row-key 125)] {:name             "employee 5"  ; 実際のコードでは、row-keyではなくnew-keyを使用してください。
                                                :superior         (get-in database [:employees     (row-key 122)])
                                                :tutor            (get-in database [:employees     (row-key 121)])
                                                :organization     (get-in database [:organizations (row-key 102)])})
          (assoc-in [:employees (row-key 126)] {:name             "employee 6"
                                                :superior-key     (row-key 122)
                                                :tutor-key        (row-key 121)
                                                :organization-key (row-key 102)})
          (ts-save! transaction))))
  (jdbc/with-db-transaction [transaction database-spec]
    (let [database (ts-database (ts-database-data transaction :employees))]
      (is (= "EMPLOYEE 0"
             (get-in database [:employees (row-key 120) :name])))
      (is (= "employee 1"
             (get-in database [:employees (row-key 122) :superior :name])))
      (is (= "EMPLOYEE 3"
             (get-in database [:employees (row-key 123) :name])))
      (is (= "employee 2"
             (get-in database [:employees (row-key 123) :superior :name])))
      (is (= "EMPLOYEE 4"
             (get-in database [:employees (row-key 124) :name])))
      (is (= "employee 2"
             (get-in database [:employees (row-key 124) :superior :name])))
      (is (= "employee 5"
             (get-in database [:employees (row-key 125) :name])))
      (is (= "employee 2"
             (get-in database [:employees (row-key 125) :superior :name])))
      (is (= "employee 6"
             (get-in database [:employees (row-key 126) :name])))
      (is (= "employee 2"
             (get-in database [:employees (row-key 126) :superior :name]))))))

(deftest sti-test
  (jdbc/with-db-transaction [transaction database-spec]
    ;; (pprint (ts-database-data transaction :programmers))
    ;; (pprint (ts-database-data transaction :software-products))
    ;; (pprint (ts-database-data transaction :software-products ($= :operating-system.name "operating-system 0")))
    ;; (pprint (ts-database-data transaction :programmers ($= :name "employee 3")))
    ;; (pprint (ts-database-data transaction :composite-products))
    ))

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
