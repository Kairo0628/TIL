from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StructField, FloatType, StringType

if __name__ == '__main__':
    conf = SparkConf()
    conf.set('spark.app.name', 'PySpark Structured Streaming')
    conf.set('spark.master', 'local[4]')
    conf.set('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1')

    spark = SparkSession.builder\
            .config(conf = conf)\
            .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    df = spark.readStream\
            .format('kafka')\
            .option('kafka.bootstrap.servers', 'localhost:19092')\
            .option('subscribe', 'structured_streaming_tutorial')\
            .load()
    
    schema = StructType([
        StructField('val1', FloatType(), True),
        StructField('val2', StringType(), True)
    ])
    
    query = df.select(f.col('key').cast('string'),
                      f.from_json(f.col('value').cast('string'), schema = schema).alias('value'))\
                            .select(f.col('key'), f.col('value.val1'), f.col('value.val2'))\
                            .groupBy('val2')\
                            .agg(f.sum('val1').alias('total_val1'))

    output = query\
        .writeStream\
            .outputMode('complete')\
                .format('console')\
                    .start()

    output.awaitTermination()
