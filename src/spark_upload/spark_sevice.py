import os
from pyspark.sql import SparkSession

def get_spark_session():
    jars = [
        "/opt/spark/jars/hadoop-aws-3.4.1.jar",
        "/opt/spark/jars/bundle-2.24.6.jar",
        "/opt/spark/jars/postgresql-42.7.9.jar",
    ]
    spark: SparkSession = (
        SparkSession.builder
        .master(os.getenv('SPARK_MASTER_URL'))
        .config("spark.jars", ",".join(jars))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv('SPARK_HADOOP_FS_S3_A_ACCESS_KEY'))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv('SPARK_HADOOP_FS_S3_A_SECRET_KEY'))
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv('SPARK_HADOOP_FS_S3_A_ENDPOINT'))
        .config("spark.hadoop.fs.s3a.path.style.access", os.getenv('SPARK_HADOOP_FS_S3_A_PATH_STYLE_ACCESS'))
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                os.getenv('SPARK_HADOOP_FS_S3_A_AWS_CREDENTIALS_PROVIDER'))
        .appName("app")
        .getOrCreate()
    )
    return spark