import findspark
findspark.init("/opt/spark")

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import explode, from_json, explode_outer
from pyspark.sql.functions import col
from pyspark.sql.types import *
from delta.tables import *

accessKeyId='yourkey'
secretAccessKey='yourpass'

# create a SparkSession
spark = SparkSession.builder \
.appName("credits-silver") \
.master("local[2]") \
.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0,io.delta:delta-core_2.12:2.4.0") \
.config("fs.s3a.access.key", accessKeyId) \
.config("fs.s3a.secret.key", secretAccessKey) \
.config("fs.s3a.path.style.access", True) \
.config("fs.s3a.endpoint", "http://minio:9000") \
.config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
.config("spark.sql.debug.maxToStringFields", 1000) \
.getOrCreate()


df_credits = spark.read.format("parquet") \
.option("header", True) \
.option("inferSchema", True) \
.option("quote", "\"") \
.option("escape", "\"") \
.load("s3a://tmdb-bronze/credits/")

df_credits.printSchema()

df_credits1 = df_credits

json_cast = ArrayType(StructType([
    StructField("cast_id", IntegerType()),
    StructField("character", StringType()),
    StructField("credit_id", StringType()),
    StructField("gender", IntegerType()),
    StructField("id", IntegerType()),
    StructField("name", StringType())
]))
#cast sütunundaki json değerlerini from_json ile  yukarıda belirtilen Json şemasına göre çözüp yeni bir sütun oluşturdum.
df_credits1 = df_credits1.withColumn("cast", from_json(col("cast"), json_cast))


json_crew = ArrayType(StructType([
    StructField("credit_id", StringType()),
    StructField("department", StringType()),
    StructField("gender", IntegerType()),
    StructField("id", IntegerType()),
    StructField("job", StringType()),
    StructField("name", StringType())
]))

#crew sütunundaki json değerlerini from_json ile  yukarıda belirtilen Json şemasına göre çözüp yeni bir sütun oluşturdum.
df_credits1 = df_credits1.withColumn("crew", from_json(col("crew"), json_crew))

df_credits1.printSchema()

#explode_outer ile iç içe geçmiş arrayi düzleştirdim veistenen değerleri credits_cast tablosunda gösterdim/ explode_outerda boş veya null durumundaki satırlar korunur
credits_cast = df_credits1.select("movie_id", "title", explode_outer("cast").alias("cast"))
credits_cast = credits_cast.select("movie_id", "title", "cast.cast_id", "cast.character", "cast.credit_id", "cast.gender", "cast.id", "cast.name")
credits_cast = credits_cast.withColumn("movie_id", col("movie_id").cast("string"))

# id nulls must be imputed with -9999.
credits_cast = credits_cast.fillna({'credit_id': 0000000000})

# tekrarlanan satırları sil
un_credits_cast = credits_cast.dropDuplicates(['movie_id', 'title','cast_id', 'character', 'credit_id','id', 'name'])


#explode_outer ile iç içe geçmiş arrayi düzleştirdim veistenen değerleri credits_crew tablosunda gösterdim/ explode_outerda boş veya null durumundaki satırlar korunur
credits_crew = df_credits1.select("movie_id", "title", explode_outer("crew").alias("crew"))
credits_crew = credits_crew.select("movie_id", "title", "crew.credit_id", "crew.department", "crew.gender", "crew.id", "crew.job", "crew.name")
credits_crew = credits_crew.withColumn("movie_id", col("movie_id").cast("string"))

# id nulls must be imputed with -9999.
credits_crew = credits_crew.fillna({'credit_id': 0000000000})

# tekrarlanan satırları sil
un_credits_crew = credits_crew.dropDuplicates(['movie_id', 'id','credit_id'])



cast_deltaPath = "s3a://tmdb-silver/cast"
cast_delta = DeltaTable.forPath(spark, cast_deltaPath)

crew_deltaPath = 's3a://tmdb-silver/crew'
crew_delta = DeltaTable.forPath(spark, crew_deltaPath)

#minio upsert
cast_delta.alias("cast") \
    .merge(un_credits_cast.alias("cast_new"), "cast.movie_id = cast_new.movie_id AND cast.credit_id = cast_new.credit_id") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

#minio upsert
crew_delta.alias("crew") \
    .merge(un_credits_crew.alias("crew_new"), "crew.movie_id = crew_new.movie_id AND crew.credit_id = crew_new.credit_id") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

cast_delta.toDF().printSchema()
crew_delta.toDF().printSchema()