import pandas as pd
import findspark
findspark.init("/opt/spark")

from pyspark.sql.functions import explode, from_json, explode_outer
from pyspark.sql.functions import col, max, min
from pyspark.sql.types import *
from delta.tables import *

accessKeyId='yourkey'
secretAccessKey='yourpass'

# create a SparkSession
spark = SparkSession.builder \
.appName("schema_credits") \
.master("local[2]") \
.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0,io.delta:delta-core_2.12:2.4.0") \
.config("fs.s3a.access.key", accessKeyId) \
.config("fs.s3a.secret.key", secretAccessKey) \
.config("fs.s3a.path.style.access", True) \
.config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
.config("fs.s3a.endpoint", "http://minio:9000") \
.config("spark.sql.debug.maxToStringFields", 1000) \
.getOrCreate()

cast_schema = StructType([
    StructField("movie_id", StringType(), nullable=True),
    StructField("title", StringType(), nullable=True),
    StructField("cast_id", IntegerType(), nullable=True),
    StructField("character", StringType(), nullable=True),
    StructField("credit_id", StringType(), nullable=True),
    StructField("gender", IntegerType(), nullable=True),
    StructField("id", IntegerType(), nullable=True),
    StructField("name", StringType(), nullable=True)
])

cast_table = spark.createDataFrame([], cast_schema)
cast_table.write.format("delta").mode("overwrite").save('s3a://tmdb-silver/cast')

crew_schema = StructType([
    StructField("movie_id", StringType(), nullable=True),
    StructField("title", StringType(), nullable=True),
    StructField("credit_id", StringType(), nullable=True),
    StructField("department", StringType(), nullable=True),
    StructField("gender", IntegerType(), nullable=True),
    StructField("id", IntegerType(), nullable=True),
    StructField("job", StringType(), nullable=True),
    StructField("name", StringType(), nullable=True)
])

crew_table = spark.createDataFrame([], crew_schema)
crew_table.write.format("delta").mode("overwrite").save('s3a://tmdb-silver/crew')