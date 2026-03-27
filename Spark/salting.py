from pyspark import SparkConf
from pyspark.sql import SparkSession

conf = SparkConf()
conf.set('spark.app.name', 'Spark Salting')
conf.set('spark.master', 'local[6]')
conf.set('spark.driver.memory', '12g')
conf.set('spark.sql.adaptive.enabled', False)
# 내 로컬 Warehouse로 지정할 위치
conf.set('spark.sql.warehouse.dir', 'C:\Spark\warehouse')

spark = SparkSession.builder\
        .config(conf = conf)\
        .enableHiveSupport()\
        .getOrCreate()

# sales 테이블: item_id가 100인 경우가 80%를 차지하는 데이터 생성
#spark.sql("""
#    CREATE TABLE sales USING parquet AS
#    SELECT
#        CASE
#            WHEN RAND() < 0.8 THEN 100
#            ELSE CAST(RAND() * 30000000 AS INT)
#        END AS item_id,
#        CAST(RAND() * 100 AS INT) AS quantity,
#        DATE_ADD(CURRENT_DATE(), - CAST(RAND() * 360 AS INT)) AS date
#    FROM RANGE(1000000000)
#""")

# items 테이블
#spark.sql("""
#    CREATE TABLE items USING parquet AS
#    SELECT
#        id,
#        CAST(RAND() * 1000 AS INT) AS price
#    FROM RANGE(30000000)
#""")

case = 0
### Aggregation
# 원본 코드
if case == 0:
    spark.sql("""
        SELECT item_id, COUNT(1)
        FROM sales
        GROUP BY 1
        ORDER BY 2 DESC
    """).show(5)

# Salting을 이용한 집계
if case == 1:
    spark.sql("""
        SELECT item_id, SUM(cnt)
        FROM (
            SELECT item_id, salt, COUNT(1) AS cnt
            FROM (
                SELECT FLOOR(RAND() * 200) AS salt, item_id
                FROM sales
            )
            GROUP BY 1, 2
        )
        GROUP BY 1
        ORDER BY 2 DESC
    """).show(5)

### Join
# 원본 코드
if case == 2:
    spark.sql("""
        SELECT date, sum(quantity * price) AS total_sales
        FROM sales s
        JOIN items i ON s.item_id = i.id
        GROUP BY 1
        ORDER BY 2 DESC
    """).show(5)

# Salting을 이용한 조인 1, item_id = 100인 경우에만 Salt
if case == 3:
    spark.sql("""
        SELECT date, sum(quantity * price) AS total_sales
        FROM (
            SELECT
                *,
                CASE
                    WHEN item_id = 100 THEN FLOOR(RAND() * 20)
                    ELSE 1
                END AS salt
            FROM sales
        ) s
        JOIN (
            SELECT
                *,
                EXPLODE(ARRAY(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19)) AS salt
            FROM items
            WHERE id = 100
              
            UNION
              
            SELECT
                *,
                1 AS salt
            FROM items
            WHERE id <> 100
        ) i ON s.item_id = i.id and s.salt = i.salt
        GROUP BY 1
        ORDER BY 2 DESC
    """).show()

# Salting을 사용한 조인 2. 전체 레코드에 salt
if case == 4:
    spark.sql("""
        SELECT date, sum(quantity * price) AS total_sales
        FROM (
            SELECT
                *,
                FLOOR(RAND() * 20) AS salt
            FROM sales      
        ) s
        JOIN (
            SELECT
                *,
                EXPLODE(ARRAY(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19)) AS salt
            FROM items
        ) i ON s.item_id = i.id AND s.salt = i.salt
        GROUP BY 1
        ORDER BY 2 DESC
    """).show()

print('Sleeping Spark Session...')
while True:
    pass
