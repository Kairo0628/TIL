from pyspark import SparkConf
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, FloatType, TimestampType, StringType
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
import pyspark.sql.functions as f

from typing import Iterator
from datetime import datetime

class CustomProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        self.handle = handle

        state_schema = StructType([
            StructField('value', FloatType(), True),
            StructField('timestamp', TimestampType(), True)
        ])

        self.state = handle.getValueState('state', state_schema)

    def handleInputRows(self, key, rows, timerValues) -> Iterator[Row]:
        max_row = max(rows, key = lambda row: row.timestamp)
        max_value = max_row.value
        max_timestamp = max_row.timestamp.replace(tzinfo = None)

        if self.state.exists():
            latest_from_existing = self.state.get()[1]
        else:
            latest_from_existing = datetime.fromtimestamp(0)

        if latest_from_existing < max_timestamp:
            for timer in self.handle.listTimers():
                self.handle.deleteTimer(timer)
            self.state.update((max_value, max_timestamp))

        self.handle.registerTimer(timerValues.getCurrentProcessingTimeInMs() + 3000)

        return iter([])
    
    def handleExpiredTimer(self, key, timerValues, expiredTimerInfo) -> Iterator[Row]:
        latest_value, latest_timestamp = self.state.get()

        downtime_duration = timerValues.getCurrentProcessingTimeInMs() - int(latest_timestamp.timestamp() * 1000)

        self.handle.registerTimer(timerValues.getCurrentProcessingTimeInMs() + 5000)

        yield Row(key = key[0], value = latest_value, timeValues = str(downtime_duration))

if __name__ == '__main__':
    conf = SparkConf()
    conf.set('spark.app.name', 'Stateful Processing')
    conf.set('spark.master', 'local[4]')
    conf.set('spark.sql.shuffle.partitions', 4)
    conf.set('spark.sql.streaming.stateStore.providerClass', 'org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider')

    spark = SparkSession.builder\
            .config(conf = conf)\
            .getOrCreate()
    
    spark.sparkContext.setLogLevel('WARN')

    df = spark.readStream\
            .format('kafka')\
            .option('kafka.bootstrap.servers', 'localhost:19092')\
            .option('subscribe', 'stateful_processing')\
            .option('startingOffsets', 'earliest')\
            .load()
    
    input_schema = StructType([
        StructField('value', FloatType(), True),
        StructField('timestamp', TimestampType(), True)
    ])

    output_schema = StructType([
        StructField('key', StringType(), True),
        StructField('value', FloatType(), True),
        StructField('timeValues', StringType(), True)
    ])
    
    query = df.select(f.col('key').cast('string'),
                      f.from_json(f.col('value').cast('string'), schema = input_schema).alias('values'))\
                .select(f.col('key'), f.col('values.value'), f.col('values.timestamp'))\
                .groupBy('key')\
                .transformWithState(
                    statefulProcessor = CustomProcessor(),
                    outputStructType = output_schema,
                    outputMode = 'update',
                    timeMode = 'processingTime'
                )\
                .writeStream\
                .format('console')\
                .start()
    
    query.awaitTermination()
