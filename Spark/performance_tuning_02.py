from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

import pandas as pd
import time

conf = SparkConf()
conf.set('spark.app.name', 'Spark Perform Tuning 02')
conf.set('spark.master', 'local[4]')
conf.set('spark.driver.memory', '4g')

spark = SparkSession.builder\
        .config(conf = conf)\
        .enableHiveSupport()\
        .getOrCreate()

if not spark.catalog.tableExists('tuning_skew'):
    df = spark.range(1, 10_000_000)\
            .withColumn('id', f.col('id') % 400)\
            .withColumn('val1', f.rand(seed = 42))\
            .withColumn('val2', f.when(f.col('val1') < 0.2, 'a')\
                                .when(f.col('val1') < 0.4, 'b')\
                                .when(f.col('val1') < 0.6, 'c')\
                                .when(f.col('val1') < 0.8, 'd')\
                                .otherwise('e'))\
            .withColumn('id', f.when(f.col('id') > 100, 1)\
                                .otherwise(f.col('id')))\
            .select('id', 'val1', 'val2')

    df.repartition(12).write.mode('overwrite').saveAsTable('tuning_skew')

# 쿼리 실행 유형 선택
# 1: Cache, 그 외: AQE
tuning_type = 2
if tuning_type == 1:
    # 캐시 없음
    spark.sql("""
        SELECT *
        FROM tuning_standard
    """).show()

    spark.sql("""
        SELECT id, SUM(val1)
        FROM tuning_standard
        GROUP BY 1
    """).show()

    # 캐시
    spark.sql("""
        CACHE TABLE tuning_standard       
    """)

    # 캐시 후
    spark.sql("""
        SELECT *
        FROM tuning_standard
    """).show()

    spark.sql("""
        SELECT id, SUM(val1)
        FROM tuning_standard
        GROUP BY 1
    """).show()

else:
    spark.sql("""
        CACHE TABLE tuning_skew       
    """)

    # 1. Dynamically Coalescing Shuffle Partitions
    spark.conf.set('spark.sql.adaptive.enabled', False)
    spark.sql("""
        SELECT id, SUM(val1)
        FROM tuning_skew
        GROUP BY 1     
    """).show()

    spark.conf.set('spark.sql.adaptive.enabled', True)
    spark.conf.set('spark.sql.adaptive.coalescePartitions.parallelismFirst', False)
    spark.sql("""
        SELECT id, SUM(val1)
        FROM tuning_skew
        GROUP BY 1     
    """).show()

    # 2.Dynamically Switching Join Strategies
    def drop_one(pdf: pd.DataFrame) -> pd.DataFrame:
        return pdf[pdf['id'].between(11, 15)]
    
    df = spark.table('tuning_skew')
    filtered_df = df.groupBy('id').applyInPandas(drop_one, schema = df.schema)\
                    .groupBy('id').agg(f.sum('val1'))

    df2 = spark.table('tuning_partitioning')
    df2.join(filtered_df, 'id').show()
 
try:
    while True:
        time.sleep(60)
        print('Waiting...')

except KeyboardInterrupt:
    spark.stop()
finally:
    print('Stopping...')
