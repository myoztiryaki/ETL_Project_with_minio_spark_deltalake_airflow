import pandas as pd
import findspark
findspark.init("/opt/spark")

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import explode, from_json, explode_outer
from pyspark.sql.functions import col
from pyspark.sql.types import ArrayType, StructType, StructField, IntegerType, StringType
from delta.tables import *

accessKeyId='yourkey'
secretAccessKey='yourpass'

# create a SparkSession
spark = SparkSession.builder \
.appName("movies-silver") \
.master("local[2]") \
.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0,io.delta:delta-core_2.12:2.4.0") \
.config("fs.s3a.access.key", accessKeyId) \
.config("fs.s3a.secret.key", secretAccessKey) \
.config("fs.s3a.path.style.access", True) \
.config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
.config("fs.s3a.endpoint", "http://minio:9000") \
.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
.config("spark.sql.debug.maxToStringFields", 1000) \
.getOrCreate()

df_movies = spark.read.format("parquet") \
.option("header", True) \
.option("inferSchema", True) \
.option("quote", "\"") \
.option("escape", "\"") \
.load('s3a://tmdb-bronze/movies/')

df_movies =df_movies.withColumnRenamed("id", "movie_id")


json_genres = ArrayType(StructType([
        StructField("id", IntegerType()),
        StructField("name", StringType())    
]))
#genres sütunundaki json değerlerini from_json ile  yukarıda belirtilen Json şemasına göre çözüp yeni bir sütun oluşturdum.
df_movies = df_movies.withColumn("genres", from_json(col("genres"), json_genres))


json_keywords = ArrayType(StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType())    
]))
df_movies = df_movies.withColumn("keywords", from_json(col("keywords"), json_keywords))

json_pcompanies = ArrayType(StructType([
    StructField("name", StringType()),
    StructField("id", IntegerType())
]))
df_movies = df_movies.withColumn("production_companies", from_json(col("production_companies"), json_pcompanies))

json_pcountries  = ArrayType(StructType([
    StructField("iso_3166_1", StringType()),
    StructField("name", StringType())
]))
df_movies = df_movies.withColumn("production_countries", from_json(col("production_countries"), json_pcountries))

json_slang = ArrayType(StructType([
    StructField("iso_639_1", StringType()),
    StructField("name", StringType())
]))
df_movies = df_movies.withColumn("spoken_languages", from_json(col("spoken_languages"), json_slang))

#movie tablosu kolonları seçildi
selected_columns = ["movie_id", "title", "budget", "homepage", "original_language", "original_title",
                    "overview", "popularity", "release_date", "revenue", "runtime", "status",
                    "tagline", "vote_average", "vote_count"]

df_movies_tf = df_movies.select(selected_columns)

schema = [
    ("movie_id", "string"),
    ("title", "string"),
    ("budget", "double"),
    ("homepage", "string"),
    ("original_language", "string"),
    ("original_title", "string"),
    ("overview", "string"),
    ("popularity", "float"),
    ("release_date", "date"),
    ("revenue", "double"),
    ("runtime", "integer"),
    ("status", "string"),
    ("tagline", "string"),
    ("vote_average", "float"),
    ("vote_count", "integer")
]

#döngü kullanarak her sütunun veri türünü belirlenen şemaya uygun olarak değiştirdim.
for col_name, col_type in schema:
    df_movies_tf = df_movies_tf.withColumn(col_name, col(col_name).cast(col_type))

#tekrarlanan degerler silindi
un_df_movies_tf = df_movies_tf.dropDuplicates(['movie_id'])


#explode_outer ile iç içe geçmiş arrayi düzleştirdim ve istenen değerleri genres tablosunda gösterdim/ explode_outerda boş veya null durumundaki satırlar korunur
movies_genres = df_movies.select("movie_id", explode_outer("genres").alias("genres"))
movies_genres = movies_genres.select("movie_id", "genres.id", "genres.name")
movies_genres = movies_genres.withColumn("movie_id", col("movie_id").cast("string"))
# id nulls must be imputed with -9999.
movies_genres = movies_genres.fillna({'id': -9999})

#tekrarlanan degerler silindi
un_movies_genres = movies_genres.dropDuplicates(['movie_id','id','name'])


# keywords tablosu donusuumu
movies_keywords = df_movies.select("movie_id", explode_outer("keywords").alias("keywords"))
movies_keywords = movies_keywords.select("movie_id", "keywords.id", "keywords.name")
movies_keywords = movies_keywords.withColumn("movie_id", col("movie_id").cast("string"))

# id nulls must be imputed with -9999.
movies_keywords = movies_keywords.fillna({'id': -9999})

#tekrarlanan degerler silindi
un_movies_keywords = movies_keywords.dropDuplicates(['movie_id', 'id', 'name'])

# production_companies tablosu
movies_prod_comp = df_movies.select("movie_id", explode_outer("production_companies").alias("production_companies"))
movies_prod_comp = movies_prod_comp.select("movie_id", "production_companies.id", "production_companies.name")
movies_prod_comp = movies_prod_comp.withColumn("movie_id", col("movie_id").cast("string"))

# id nulls must be imputed with -9999.
movies_prod_comp = movies_prod_comp.fillna({'id': -9999})

#tekrarlanan degerler silindi
un_movies_prod_comp =movies_prod_comp.dropDuplicates(['movie_id', 'id', 'name'])


# production_countries tablosu dönusumu
movies_prod_country = df_movies.select("movie_id", explode_outer("production_countries").alias("production_countries"))
movies_prod_country = movies_prod_country.select("movie_id", "production_countries.iso_3166_1", "production_countries.name")
movies_prod_country = movies_prod_country.withColumn("movie_id", col("movie_id").cast("string"))

# id nulls must be imputed with -xx.
movies_prod_country = movies_prod_country.fillna({'iso_3166_1': 'XX'})

#tekrarlanan degerler silindi
un_movies_prod_country = movies_prod_country.dropDuplicates(['movie_id','iso_3166_1'])


#spoken_languages tablosu
movies_spoken_lang = df_movies.select("movie_id", explode_outer("spoken_languages").alias("spoken_languages"))
movies_spoken_lang = movies_spoken_lang.select("movie_id", "spoken_languages.iso_639_1", "spoken_languages.name")
movies_spoken_lang = movies_spoken_lang.withColumn("movie_id", col("movie_id").cast("string"))

# id nulls must be imputed with -xx.
movies_spoken_lang = movies_spoken_lang.fillna({'iso_639_1': 'XX'})

#tekrarlanan degerler silindi
un_movies_spoken_lang = movies_spoken_lang.dropDuplicates(["movie_id", "iso_639_1"])



# silver layerdaki  boş tabloları delta formatında oku
movies_deltaPath = "s3a://tmdb-silver/movies"
movies_delta = DeltaTable.forPath(spark, movies_deltaPath)

genres_deltaPath = "s3a://tmdb-silver/genres"
genres_delta = DeltaTable.forPath(spark, genres_deltaPath)

keywords_deltaPath = "s3a://tmdb-silver/keywords"
keywords_delta = DeltaTable.forPath(spark, keywords_deltaPath)

prod_comp_deltaPath = "s3a://tmdb-silver/production_companies"
prod_comp_delta = DeltaTable.forPath(spark, prod_comp_deltaPath)

prod_countries_deltaPath = "s3a://tmdb-silver/production_countries"
prod_countries_delta = DeltaTable.forPath(spark, prod_countries_deltaPath)

movies_spoken_lang_deltaPath = "s3a://tmdb-silver/spoken_languages"
movies_spoken_lang_delta = DeltaTable.forPath(spark, movies_spoken_lang_deltaPath)

movies_delta.alias("movies") \
    .merge(un_df_movies_tf.alias("movies_new"), "movies.movie_id = movies_new.movie_id") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()


#xxminio yazdırma
genres_delta.alias("genres") \
    .merge(un_movies_genres.alias("genres_new"), "genres.movie_id = genres_new.movie_id AND genres.id = genres_new.id") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

#xxminio upsert
keywords_delta.alias("keywords") \
    .merge(un_movies_keywords.alias("keywords_new"), "keywords.movie_id = keywords_new.movie_id AND keywords.id = keywords_new.id") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

#xxminio upsert
prod_comp_delta.alias("prod_comp") \
    .merge(un_movies_prod_comp.alias("prod_comp_new"), "prod_comp.movie_id = prod_comp_new.movie_id AND prod_comp.id = prod_comp_new.id") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

prod_countries_delta.alias("prod_countries") \
    .merge(un_movies_prod_country.alias("prod_countries_new"), "prod_countries.movie_id = prod_countries_new.movie_id AND prod_countries.iso_3166_1 = prod_countries_new.iso_3166_1") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

#xxminio upsert
movies_spoken_lang_delta.alias("spoken_lang") \
    .merge(un_movies_spoken_lang.alias("spoken_lang_new"), "spoken_lang.movie_id = spoken_lang_new.movie_id AND spoken_lang.iso_639_1 = spoken_lang_new.iso_639_1") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

movies_delta.toDF().printSchema()
genres_delta.toDF().printSchema()
keywords_delta.toDF().printSchema()
prod_comp_delta.toDF().printSchema()
prod_countries_delta.toDF().printSchema()
movies_spoken_lang_delta.toDF().printSchema()
