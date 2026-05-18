from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

import time

conf = SparkConf()
conf.set('spark.app.name', 'Spark Perform Tuning 02')
conf.set('spark.master', 'local[4]')

spark = SparkSession.builder\
        .config(conf = conf)\
        .enableHiveSupport()\
        .getOrCreate()

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

try:
    while True:
        time.sleep(60)
        print('Waiting...')

except KeyboardInterrupt:
    spark.stop()
finally:
    print('Stopping...')
