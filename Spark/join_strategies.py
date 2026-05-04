from pyspark.sql import SparkSession
from pyspark import SparkConf
import pyspark.sql.functions as f

import time

WAREHOUSE_PATH = 'C:\\Users\\user\\Desktop\\Data Engineering\\spark_warehouse'

conf = SparkConf()
conf.set('spark.app.name', 'spark join strategies')
conf.set('spark.master', 'local[*]')
conf.set('spark.sql.warehouse.dir', WAREHOUSE_PATH)

spark = SparkSession.builder\
        .config(conf = conf)\
        .enableHiveSupport()\
        .getOrCreate()

def is_table_exists(table):
    return spark.catalog._jcatalog.tableExists(table)

if not is_table_exists('small_dept'):
    print('Create Example Table')

    dept = [(i, f'Dept_{i}', i * 100) for i in range(1, 101)]
    spark.createDataFrame(dept, ['dept_id', 'dept_name', 'min_salary'])\
        .write.mode('overwrite').format('parquet').saveAsTable('small_dept')
    
    spark.range(0, 1_000_000).select(f.col('id').alias('emp_id'),
                                     f.when(f.rand() < 0.3, f.lit(1)).otherwise((f.rand() * 100).cast('int')).alias('dept_id'),
                                     (f.rand() * 10_000).alias('salary')
                                     ).write.mode('overwrite').format('parquet').saveAsTable('large_emp')
    
    print('Table Create Complete')
else:
    print('Table Already Exists')

small_df = spark.table('small_dept')
large_df = spark.table('large_emp')

# AQE Disable
spark.conf.set("spark.sql.adaptive.enabled", "false")

# Test case (1: Broadcast, 2: Shuffle Hash, 3: Sort Merge, 4: Boradcast Nested Loop)
test_case = 4

if test_case == 1:
    result_df = large_df.join(f.broadcast(small_df), 'dept_id')
    result_df.explain()

    #spark.sql("""
    #    SELECT /* BROADCAST(s) */ * 
    #    FROM small_dept s
    #    JOIN large_emp l ON s.dept_id = l.dept_id
    #""")

    result_df.count()

elif test_case == 2:
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark.conf.set("spark.sql.join.preferSortMergeJoin", "false")

    result_df = large_df.join(small_df.hint('SHUFFLE_HASH'), 'dept_id')
    result_df.explain()

    #spark.sql("""
    #    SELECT /* SHUFFLE_HASH(s) */ * 
    #    FROM small_dept s
    #    JOIN large_emp l ON s.dept_id = l.dept_id
    #""")

    result_df.count()

elif test_case == 3:
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    result_df = large_df.join(small_df.hint('MERGE'), 'dept_id')
    result_df.explain()

    #spark.sql("""
    #    SELECT /* MERGE(s) */ * 
    #    FROM small_dept s
    #    JOIN large_emp l ON s.dept_id = l.dept_id
    #""")

    result_df.count()

else:
    #result_df = large_df.join(
    #    f.broadcast(small_df), 
    #    f.col("l.salary") > f.col("s.min_salary")
    #)
    #result_df.explain()

    #spark.sql("""
    #    SELECT /* SHUFFLE_REPLICATE_NL(s) */ * 
    #    FROM small_dept s
    #    JOIN large_emp l ON s.min_salary < l.salary
    #""")

    #result_df.count()
    pass

try:
    while True:
        print('waiting...')
        time.sleep(30)
except KeyboardInterrupt:
    pass
finally:
    spark.stop()
    print('spark session close')
