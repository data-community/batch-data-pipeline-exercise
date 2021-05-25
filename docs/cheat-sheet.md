# Cheat Sheet

## PostgreSQL


### 创建数据库表

```sql
CREATE TABLE IF NOT EXISTS STG_PRODUCTS (
    id VARCHAR NOT NULL UNIQUE,
    title VARCHAR,
    category VARCHAR,
    price VARCHAR
);

CREATE TABLE IF NOT EXISTS DIM_PRODUCTS (
    id VARCHAR NOT NULL,
    title VARCHAR,
    category VARCHAR,
    price DECIMAL,
    processed_date date,
    start_date date,
    end_date date,
    UNIQUE(id, start_date)
);
```

### 清空表

```sql
truncate STG_PRODUCTS;
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
UPDATE DIM_PRODUCTS
SET title = STG_PRODUCTS.title,
    category = STG_PRODUCTS.category,
    price = STG_PRODUCTS.price,
    end_date = '{{ ds }}' -- 当前时间。比如 2021-05-05
FROM STG_PRODUCTS
WHERE STG_PRODUCTS.id = DIM_PRODUCTS.id
AND '{{ ds }}' >= DIM_PRODUCTS.start_date and '{{ ds }}' < DIM_PRODUCTS.end_date;

WITH dim as (
    SELECT * FROM DIM_PRODUCTS
    WHERE '{{ ds }}' >= start_date and '{{ ds }}' < end_date
)
INSERT INTO DIM_PRODUCTS
SELECT 
    STG_PRODUCTS.id as id,
    STG_PRODUCTS.title,
    STG_PRODUCTS.category,
    STG_PRODUCTS.price,
    '{{ ds }}' AS processed_date,
    '{{ ds }}' AS start_date,
    '9999-12-31' AS end_date
FROM STG_PRODUCTS
WHERE STG_PRODUCTS.id NOT IN (select id from dim);
```