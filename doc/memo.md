# メモ

## SELECT

親方向は、どこまでも辿る。以下のSQLを再帰で繰り返す。

```sql
SELECT
  "parents".*
FROM
  "parents"
WHERE
  "parents"."key" IN (
    SELECT "children"."parent-key" FROM "children" WHERE ...
  )
```

子方向へは、最初の一回だけ辿る。で、その後、親方向にどこまでも辿る。

```sql
SELECT
  "children".*
FROM
  "children"
WHERE
  "children"."parent-key" IN (
    SELECT "parents"."key" FROM "parents" WHERE ...
  )
```

＃INは遅くてダサい気もするけど、後述する再帰テーブルと組み合わせられる他の方法を思いつかなかった……。

### 再帰テーブル

WITH RECURSIVEを使って、親方向に辿る。

```sql
SELECT "employees".* FROM (
  WITH RECURSIVE
    "recursive-employees" AS (
      SELECT
        "employees".*
      FROM
        "employees" -- この部分は、再帰ではない場合と同じ。
      UNION
      SELECT
        "employees".*
      FROM
        "employees"
          JOIN "recursive-employees" ON
            "recursive-employees"."tutor-key"    = "employees"."key"
            OR
            "recursive-employees"."superior-key" = "employees"."key"
    ) 
    SELECT * FROM "recursive-employees"
) "employees"
```

＃外側に余計なSELECTをつけているのは、他テーブルを辿る際に最初の「*」を変えるだけというシンプルなやり方にしたいため。

### JOINとWHERE

キーワードの単位でJOINする。同じテーブルが複数回JOINされることもある。

```clojure
:products ($or ($= :order-details.order.customer.name "x") ($= :favorites.customer.name "y"))
```

```sql
SELECT DISTINCT
  "products".*
FROM
  products
    LEFT JOIN "order-details" AS "t-1" ON (
      "t-1"."product-key" = "products"."key"
    )
    LEFT JOIN "orders" AS "t-2" ON (
      "t-2"."key" = "t-1"."order-key"
    )
    LEFT JOIN "customer" AS "t-3" ON (
      "t-3"."key" = "t-2"."customer-key"
    )
    LEFT JOIN "favoties" AS "t-4" on (
      "t-4"."product-key" = "products"."key"
    )
    LEFT JOIN "customers" AS "t-5" on (
      "t-5"."key" = "t-4"."customer-key"
    )
WHERE
  "t-3"."name" = 'x'
  OR
  "t-5"."name" = 'y'
```

最悪、以下になる。効率が悪いような気がするけれど、とりあえず無視。

```clojure
:products ($or ($= :category.name "x")
               ($= :category.name "y"))
```

```sql
SELECT DISTINCT
  "products".*
FROM
  products
    LEFT JOIN "categories" AS "t-1" ON (
      "t-1"."key" = "products"."category-key"
    )
    LEFT JOIN "categories" AS "t-2" ON (
      "t-2"."key" = "products"."category-key"
    )
WHERE
  "t-1"."name" = 'x'
  OR
  "t-2"."name" = 'y'
```
