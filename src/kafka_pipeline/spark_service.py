from pyspark import SparkContext
from pyspark.sql import SparkSession


class SparkService:
    def __init__(self):
        if SparkContext._active_spark_context:
            SparkContext._active_spark_context.stop()
        jars = [
            "/opt/spark/jars/spark-sql-kafka-0-10_2.13-4.0.1.jar",
            "/opt/spark/jars/postgresql-42.7.9.jar",
        ]
        spark: SparkSession = (SparkSession.builder
                               .master("spark://spark-master:7077")
                               .config("spark.jars", ",".join(jars))
                               .appName("kafka-pipeline")
                               .getOrCreate()
                               )
        self.spark = spark

spark_service = SparkService()