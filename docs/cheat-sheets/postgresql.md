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

```sql
CREATE TABLE IF NOT EXISTS DIM_DATES (
    id VARCHAR NOT NULL UNIQUE,
    datum date,
    day_of_year INT,
    month INT,
    quater INT,
    year INT
);

INSERT INTO DIM_DATES
SELECT 
    TO_CHAR(datum, 'yyyymmdd')::INT AS id,
    datum as datum,
    EXTRACT(DOY FROM datum) AS day_of_year,
    EXTRACT(MONTH FROM datum) AS month,
    EXTRACT(QUARTER FROM datum) AS quater,
    EXTRACT(YEAR FROM datum) AS year
FROM (SELECT '1970-01-01'::DATE + SEQUENCE.DAY AS datum
    FROM GENERATE_SERIES(0, 29219) AS SEQUENCE (DAY)
    GROUP BY SEQUENCE.DAY) DQ
ORDER BY 1
ON CONFLICT (id) DO NOTHING; 
```

### 更新缓慢变化维

```sql
UPDATE dim_products
SET title = stg_products.title,
    category = stg_products.category,
    price = stg_products.price,
    processed_time = '{{ ts }}',
    end_time = '{{ ts }}'
FROM stg_products
WHERE stg_products.id = dim_products.id
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
    '9999-12-31 23:59:59' AS end_time
FROM stg_products
WHERE stg_products.id NOT IN (select id from sc);
```