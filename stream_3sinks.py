import findspark

findspark.init("/opt/manual/spark")

from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("File Source to Multiple Sinks").enableHiveSupport() \
.config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0") \
.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
.getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

iot_schema = "ts float, device string, co float, humidity float, light boolean, lpg float, motion boolean, smoke float, temp float"

lines = (spark
         .readStream.format("csv")
         .schema(iot_schema)
         .option("header", True)
         .load("file:///home/train/data-generator/output"))

# Postgresql connection string
jdbcUrl = "jdbc:postgresql://localhost/traindb?user=train&password=Ankara06"


def write_to_multiple_sinks(df, batchId):
    df.show(5)

    df_postgres = df.filter(F.col("device") == '00:0f:00:70:91:0a')

    # write postgresql
    df_postgres.write.jdbc(url=jdbcUrl,
                  table="sensor_0a",
                  mode="append",
                  properties={"driver": 'org.postgresql.Driver'})

    df_hive = df.filter(F.col("device") == 'b8:27:eb:bf:9d:51')

    # write to hive
    df_hive.write.mode("overwrite").saveAsTable("test1.sensor_51")


    # write to delta
    df_delta = df.filter(F.col("device") == '1c:bf:ce:15:ec:4d')

    deltaPath = "hdfs://localhost:9000/user/train/ik_delta"

    df_delta.write.mode("overwrite").format("delta").save(deltaPath)

CheckpointDir = "file:///home/train/stream_3sinks_checkpoint"

# start streaming
streamingQuery = (lines
                  .writeStream
                  .foreachBatch(write_to_multiple_sinks)
                  .option("checkpointLocation", CheckpointDir)
                  .start())

#
streamingQuery.awaitTermination()
