from pyspark.sql import SparkSession
from pyspark.sql.functions import lower
from datetime import datetime
import pandas as pd

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("pyspark-gcs-project-saksit") \
    .getOrCreate()

#path config
bucket_name = "project-etl-saksit"
input_file_name = "raw/movie_review.csv"
output_file_name = "clean/movie_review.parquet"
path = f"gs://{bucket_name}/{input_file_name}"

positive_word = [
    "amazing",
    "excellent",
    "incredible",
    "fantastic",
    "outstanding",
    "brilliant",
    "good",
    "superb",
    "wonderful",
    "extraordinary"
]

positive_regex = "|".join(positive_word)

#create dataframe
df = spark.read.csv(path, header=True, inferSchema=True)

#transform review text to lower
df_transform = df.select("cid", lower("review_str").alias("review_str_lower"))

#regex(rlike)
df_final = df_transform.select("cid", df_transform.review_str_lower.rlike(positive_regex).alias("positive_review"))

# เขียน DataFrame เป็นไฟล์ Parquet
output_path = f"gs://{bucket_name}/{output_file_name}"
df_final.toPandas().to_parquet(output_path)
