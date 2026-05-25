from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

import time

conf = SparkConf()
conf.set('spark.app.name', 'PySpark Salting')
conf.set('spark.master', 'local[4]')

spark = SparkSession.builder\
        .config(conf = conf)\
        .enableHiveSupport()\
        .getOrCreate()

if not spark.catalog.tableExists('salting_test'):
    df = spark.range(10_000_000)\
            .withColumn('id', f.when(f.rand(seed = 42) < 0.8, 100)\
                                .otherwise(f.rand(seed = 42) * 10000).cast('int'))\
            .withColumn('value', f.rand(seed = 42) * 100)
    
    df.write.mode('overwrite').saveAsTable('salting_test')

spark.conf.set('spark.sql.adaptive.enabled', False)

spark.sql('CACHE TABLE salting_test')

df = spark.table('salting_test')

df_tmp = df.repartition('id')
df_tmp.cache()
df_tmp.count()

df_tmp.groupBy('id').count().show()
df_tmp.unpersist()

df_salt = spark.sql("""
    SELECT id, SUM(cnt)
    FROM (
        SELECT id, salt, COUNT(*) AS cnt
        FROM (
            SELECT *, FLOOR(RAND() * 8) AS salt
            FROM salting_test
        )
        GROUP BY 1, 2
    )
    GROUP BY 1
""")

df_salt.show()

try:
    while True:
        time.sleep(60)
        print('waiting...')
except KeyboardInterrupt:
    print('stopping...')
finally:
    spark.stop()
