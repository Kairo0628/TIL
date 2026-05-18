from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

import time

conf = SparkConf()
conf.set('spark.app.name', 'Spark Perform Tuning 01')
conf.set('spark.master', 'local[4]')

spark = SparkSession.builder\
        .config(conf = conf)\
        .enableHiveSupport()\
        .getOrCreate()

if not spark.catalog.tableExists('tuning_standard'):
    df = spark.range(1, 10_000_000)\
            .withColumn('id', f.col('id') % 100)\
            .withColumn('val1', f.rand(seed = 42))\
            .withColumn('val2', f.when(f.col('val1') < 0.2, 'a')\
                                .when(f.col('val1') < 0.4, 'b')\
                                .when(f.col('val1') < 0.6, 'c')\
                                .when(f.col('val1') < 0.8, 'd')\
                                .otherwise('e'))
    
    df.repartition(12).write.mode('overwrite').saveAsTable('tuning_standard')
    df.write.mode('overwrite').saveAsTable('tuning_partitioning', partitionBy = 'id')
    df.repartition(12, 'id').write.mode('overwrite').bucketBy(12, 'id').saveAsTable('tuning_bucketing')

    df2 = spark.range(1, 1_000)\
            .withColumn('id', f.col('id') % 100)\
            .withColumn('val1', f.rand(seed = 0))\
            .withColumn('val2', f.when(f.col('val1') < 0.2, 'a')\
                                .when(f.col('val1') < 0.4, 'b')\
                                .when(f.col('val1') < 0.6, 'c')\
                                .when(f.col('val1') < 0.8, 'd')\
                                .otherwise('e'))
    df2.repartition(1).write.mode('overwrite').saveAsTable('tuning_small')

# 1. 단순 저장된 테이블
standard = spark.sql("""
        SELECT id, SUM(val1), SUM(val2)
        FROM tuning_standard
        WHERE id IN (1, 2, 3, 4, 5)
        GROUP BY id
""").count()

# 2. 버켓팅으로 저장된 테이블
bucketing = spark.sql("""
        SELECT id, SUM(val1), SUM(val2)
        FROM tuning_bucketing
        WHERE id IN (1, 2, 3, 4, 5)
        GROUP BY id
""").count()

# 3. 파티셔닝으로 저장된 테이블
partitioning = spark.sql("""
        SELECT id, SUM(val1), SUM(val2)
        FROM tuning_partitioning
        WHERE id IN (1, 2, 3, 4, 5)
        GROUP BY id
""").count()

# DPP 1: DPP 적용 X (파티셔닝 되어있지 않은 테이블)
dpp1 = spark.sql("""
        SELECT *
        FROM tuning_standard l
        JOIN tuning_small r
        ON l.id = r.id
        WHERE r.id IN (11, 12, 13, 14, 15)
""").count()

# DPP 2: DPP 적용, DPP를 사용하려면 반드시 조인 키로 파티셔닝 되어야 함
dpp2 = spark.sql("""
        SELECT *
        FROM tuning_partitioning l
        JOIN tuning_small r
        ON l.id = r.id
        WHERE r.id IN (11, 12, 13, 14, 15)
""").count()

try:
    while True:
        time.sleep(60)
        print('Waiting...')

except KeyboardInterrupt:
    spark.stop()
finally:
    print('Stopping...')
