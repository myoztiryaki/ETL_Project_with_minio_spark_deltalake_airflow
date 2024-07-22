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
.appName("schema_movies") \
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

movies_schema = StructType([
    StructField("movie_id", StringType(), nullable=True),
    StructField("title", StringType(), nullable=True),
    StructField("budget", DoubleType(), nullable=True),
    StructField("homepage", StringType(), nullable=True),
    StructField("original_language", StringType(), nullable=True),
    StructField("original_title", StringType(), nullable=True),
    StructField("overview", StringType(), nullable=True),
    StructField("popularity", FloatType(), nullable=True),
    StructField("release_date", DateType(), nullable=True),
    StructField("revenue", DoubleType(), nullable=True),
    StructField("runtime", IntegerType(), nullable=True),
    StructField("status", StringType(), nullable=True),
    StructField("tagline", StringType(), nullable=True),
    StructField("vote_average", FloatType(), nullable=True),
    StructField("vote_count", IntegerType(), nullable=True)
])

movies_table = spark.createDataFrame([], movies_schema)
movies_table.write.format("delta").mode("overwrite").save('s3a://tmdb-silver/movies')

genres_schema = StructType([
    StructField("movie_id", StringType(), nullable=True),
    StructField("id", IntegerType(), nullable=True),
    StructField("name", StringType(), nullable=True)
])

genres_table = spark.createDataFrame([], genres_schema)
genres_table.write.format("delta").mode("overwrite").save('s3a://tmdb-silver/genres')

keywords_schema = StructType([
    StructField("movie_id", StringType(), nullable=True),
    StructField("id", IntegerType(), nullable=True),
    StructField("name", StringType(), nullable=True)
])
keywords_table = spark.createDataFrame([], keywords_schema)
keywords_table.write.format("delta").mode("overwrite").save('s3a://tmdb-silver/keywords')


production_companies_schema = StructType([
    StructField("movie_id", StringType(), nullable=True),
    StructField("id", IntegerType(), nullable=True),
    StructField("name", StringType(), nullable=True)
])
production_companies_table = spark.createDataFrame([], production_companies_schema)
production_companies_table.write.format("delta").mode("overwrite").save('s3a://tmdb-silver/production_companies')

production_countries_schema = StructType([
    StructField("movie_id", StringType(), nullable=True),
    StructField("iso_3166_1", StringType(), nullable=True),
    StructField("name", StringType(), nullable=True)
])
production_countries_table = spark.createDataFrame([], production_countries_schema)
production_countries_table.write.format("delta").mode("overwrite").save('s3a://tmdb-silver/production_countries')

spoken_languages_schema = StructType([
    StructField("movie_id", StringType(), nullable=True),
    StructField("iso_639_1", StringType(), nullable=True),
    StructField("name", StringType(), nullable=True)
])
spoken_languages_table = spark.createDataFrame([], spoken_languages_schema)
spoken_languages_table.write.format("delta").mode("overwrite").save('s3a://tmdb-silver/spoken_languages')