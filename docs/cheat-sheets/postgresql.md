# Cheat Sheet

## PostgreSQL

### 创建数据库表

```sql
CREATE TABLE IF NOT EXISTS stg_products (
    id VARCHAR NOT NULL UNIQUE,
    title VARCHAR,
    category VARCHAR,
    price DECIMAL,
    processed_time timestamp
);

CREATE TABLE IF NOT EXISTS dim_products (
    id VARCHAR NOT NULL,
    title VARCHAR,
    category VARCHAR,
    price DECIMAL,
    processed_time timestamp,
    start_time timestamp,
    end_time timestamp,
UNIQUE(id, start_time)
);
```

### 清空表

```sql
truncate stg_products;
```

### 创建时间维度

简化版本： https://wiki.postgresql.org/wiki/Date_and_Time_dimensions

```sql
CREATE TABLE IF NOT EXISTS dim_dates (
    id VARCHAR NOT NULL UNIQUE,
    datum date,
    day_of_month INT,
    day_of_year INT,
    month INT,
    quarter INT,
    year INT,
    first_day_of_month DATE NOT NULL,
    last_day_of_month DATE NOT NULL,
    first_day_of_next_month DATE NOT NULL
);

INSERT INTO dim_dates
SELECT 
    TO_CHAR(datum, 'yyyymmdd')::INT AS id,
    datum as datum,
    EXTRACT(DAY FROM datum) AS day_of_month,
    EXTRACT(DOY FROM datum) AS day_of_year,
    EXTRACT(MONTH FROM datum) AS month,
    EXTRACT(QUARTER FROM datum) AS quarter,
    EXTRACT(YEAR FROM datum) AS year,
    datum + (1 - EXTRACT(DAY FROM datum))::INT AS first_day_of_month,
    (DATE_TRUNC('MONTH', datum) + INTERVAL '1 MONTH - 1 day')::DATE AS last_day_of_month,
    (DATE_TRUNC('MONTH', datum) + INTERVAL '1 MONTH')::DATE AS first_day_of_next_month
FROM (SELECT '1970-01-01'::DATE + SEQUENCE.DAY AS datum
    FROM GENERATE_SERIES(0, 29219) AS SEQUENCE (DAY)
    GROUP BY SEQUENCE.DAY) DQ
ORDER BY 1
ON CONFLICT (id) DO NOTHING;
```

更多字段见 https://wiki.postgresql.org/wiki/Date_and_Time_dimensions

### 更新全量数据的缓慢变化维

```sql
UPDATE dim_products
    SET end_time = '{{ ts }}'
FROM stg_products
WHERE 
    stg_products.id = dim_products.id
    AND '{{ ts }}' >= dim_products.start_time AND '{{ ts }}' < dim_products.end_time
    AND (dim_products.title <> stg_products.title OR dim_products.category <> stg_products.category OR dim_products.price <> stg_products.price);

WITH sc as (
    SELECT * FROM dim_products
    WHERE '{{ ts }}' >= dim_products.start_time and '{{ ts }}' < dim_products.end_time
)
INSERT INTO dim_products(id, title, category, price, processed_time, start_time, end_time)
SELECT stg_products.id as id,
    stg_products.title,
    stg_products.category,
    stg_products.price,
    '{{ ts }}' AS processed_time,
    '{{ ts }}' AS start_time,
    '2999-12-31 23:59:59' AS end_time
FROM stg_products
WHERE stg_products.id NOT IN (select id from sc);
```

注意这里用了`2999-12-31 23:59:59`，而不是`9999-12-31 23:59:59`是因为在查询时，PostgreSQL会报类型转换溢出的错误。

### 当前订单数，按订单状态

```sql
select count(*), dim_orders.status as status from orders
inner join dim_orders on dim_orders.order_id = orders.order_id and CURRENT_TIMESTAMP >= dim_orders.start_time and CURRENT_TIMESTAMP < dim_orders.end_time 
group by dim_orders.status
```

### 订单创建数，按每个季度

```sql
select 
dim_dates.year::varchar || '_' || dim_dates.quarter::varchar quarter,
count(*)
from orders
inner join dim_dates on dim_dates.id = orders.created_date_id
group by dim_dates.year, dim_dates.quarter
order by  dim_dates.year, dim_dates.quarter
```

### 订单创建数，按每个季度每个产品类别查看

```sql
select 
dim_dates.year::varchar || '_' || dim_dates.quarter::varchar quarter,
dim_products.category,
count(*)
from orders
inner join dim_dates on dim_dates.id = orders.created_date_id
inner join dim_products on dim_products.id = orders.product_id and orders.created_time >= dim_products.start_time and orders.created_time < dim_products.end_time
group by dim_dates.year, dim_dates.quarter,dim_products.category
order by  dim_dates.year, dim_dates.quarter,dim_products.category
```

### 本月之前创建，但还未完成的订单数

```sql
select 
count(*)
from orders
inner join dim_dates on dim_dates.id = orders.created_date_id
inner join dim_orders on dim_orders.order_id = orders.order_id and current_timestamp >= dim_orders.start_time and current_timestamp < dim_orders.end_time
where orders.created_time < date_trunc('MONTH',now())::DATE
and dim_orders.status <> 'completed'
```