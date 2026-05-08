from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import IntegerType

import pandas as pd
import time

conf = SparkConf()
conf.set('spark.app.name', 'PySpark UDF')
conf.set('spark.master', 'local[*]')

spark = SparkSession.builder\
        .config(conf = conf)\
        .getOrCreate()

df = spark.createDataFrame(
    [(1, 1), (2, 2), (1, 3), (2, 4)],
    ['id', 'num']
)

df.createOrReplaceTempView('df')
df.cache()
df.count()


# 일반 UDF
def plus_one(x):
    return x + 1

plus_one_udf = f.udf(plus_one, IntegerType())

print('일반 UDF')
df.withColumn('plus_one', plus_one_udf('num')).show()


# SQL Register
spark.udf.register('plus_one_sql', plus_one, IntegerType())

print('SQL Register 1')
spark.sql("""
    SELECT
        num,
        plus_one_sql(num) AS plus_one
    FROM df
""").show()

print('SQL Register 2')
df.withColumn('plus_one', f.expr('plus_one_sql(num)')).show()


# Pandas UDF
@f.pandas_udf(returnType = IntegerType())
def plus_one_pandas(x: pd.Series) -> pd.Series:
    return x + 1

print('Pandas UDF')
df.withColumn('plus_one', plus_one_pandas('num')).show()


# applyInPandas
def group_sum(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf['num'] = pdf['num'].sum()
    return pdf

print('applyInPandas')
df.groupBy('id').applyInPandas(group_sum, schema = 'id integer, num integer').show()


# 비교용 1. 단순 + 1 쿼리
print('Spark SQL plus_one')
spark.sql("""
    SELECT
        num,
        num + 1 AS plus_one
    FROM df
""").show()

# 비교용 2. Group By + Sum
print('Spark SQL Groupping')
spark.sql("""
    SELECT
        id,
        SUM(num)
    FROM df
    GROUP BY id       
""").show()

try:
    while True:
        time.sleep(60)
        print('waiting...')

except KeyboardInterrupt:
    print('Session Stopping...')

finally:
    spark.stop()
