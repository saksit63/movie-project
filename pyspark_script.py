from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer
from pyspark.sql.functions import array_contains, lit
from pyspark.ml.feature import StopWordsRemover
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

#create dataframe
df = spark.read.csv(path, header=True, inferSchema=True)

#Tokenize text >> Ex. "This movie is good" แปลงเป็น ["this", "movie", "is", "good"]
tokenizer = Tokenizer(inputCol="review_str", outputCol="review_token")
df_tokens = tokenizer.transform(df).select("cid", "review_token")

# Remove stop words >> ช่วยในการลบคำที่ไม่สำคัญ Ex. "is", "the", "and" เป็นต้น
remover = StopWordsRemover(inputCol="review_token", outputCol="review_clean")
df_clean = remover.transform(df_tokens).select("cid", "review_clean")

# array_contains ใช้เพื่อตรวจสอบว่ามีค่าวัตถุที่กำหนดอยู่ใน array หรือไม่ ถ้าอยู่คืนค่า True
df_out = df_clean.select(df_clean.cid, array_contains("review_clean", "good").alias("positive_review"))

#lit ใช้ในการสร้างคอลัมน์ใหม่ที่มีค่าเป็นค่านิ่ง (literal value) ฟังก์ชันนี้มีประโยชน์เมื่อคุณต้องการเพิ่มคอลัมน์ใหม่ที่มีค่าคงที่ใน DataFrame
df_final = df_out.withColumn("insert_date", lit(datetime.now().date()))

# เขียน DataFrame เป็นไฟล์ Parquet
output_path = f"gs://{bucket_name}/{output_file_name}"
df_final.toPandas().to_parquet(output_path)
#coalesce(1).write.mode("overwrite").parquet(output_path)
