from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

import time

conf = SparkConf()
conf.set('spark.app.name', 'Execution Plan')
conf.set('spark.master', 'local[*]')

spark = SparkSession.builder\
        .config(conf = conf)\
        .enableHiveSupport()\
        .getOrCreate()

if not spark.catalog.tableExists('execution_plan_small'):
    df = spark.range(1, 101)\
        .withColumn('value', f.rand(seed = 42))
    df = df.repartitionByRange(3, 'id')
    df.write.mode('overwrite').saveAsTable('execution_plan_small')
    del(df)

    df = spark.range(1, 1001)\
        .withColumn('value', f.rand(seed = 42))
    df.write.mode('overwrite').saveAsTable('execution_plan_medium')
    del(df)

    df = spark.range(1, 10001)\
        .withColumn('value', f.rand(seed = 42))
    df.write.mode('overwrite').saveAsTable('execution_plan_large')
    del(df)


check_predicate_pushdown = False
check_column_purning = False
check_join_reorder = False
visualizaion = True

# Predicate Pushdown
if check_predicate_pushdown:
    df1 = spark.table('execution_plan_small')
    df1.explain()

    df2 = spark.table('execution_plan_small').filter('id < 35')
    df2.explain()

    print()

# Column Pruning
if check_column_purning:
    df1 = spark.table('execution_plan_small')
    df1.explain(True)

    df2 = spark.table('execution_plan_small').select('id')
    df2.explain(True)

    print()

# Join Reorder
if check_join_reorder:
    spark.conf.set("spark.sql.cbo.enabled", "true")
    spark.conf.set("spark.sql.cbo.joinReorder.enabled", "true")

    spark.sql("ANALYZE TABLE execution_plan_large COMPUTE STATISTICS FOR ALL COLUMNS")
    spark.sql("ANALYZE TABLE execution_plan_medium COMPUTE STATISTICS FOR ALL COLUMNS")
    spark.sql("ANALYZE TABLE execution_plan_small COMPUTE STATISTICS FOR ALL COLUMNS")

    df = spark.sql("""
        SELECT *
        FROM execution_plan_large l
        JOIN execution_plan_medium m ON l.id = m.id
        JOIN execution_plan_small s ON m.id = s.id
    """)
    df.explain(True)

# Visualizaion
if visualizaion:
    df = spark.sql("""
        SELECT
            m.id,
            COUNT(*) OVER (PARTITION BY m.id) AS count
        FROM execution_plan_small s
        JOIN execution_plan_medium m ON s.id = m.id
        WHERE m.id < 100
        GROUP BY 1            
    """)

    df.show()

    try:
        while True:
            time.sleep(60)
            print('waiting...')
    except KeyboardInterrupt:
        spark.stop()
        print('Spark Stopping...')
