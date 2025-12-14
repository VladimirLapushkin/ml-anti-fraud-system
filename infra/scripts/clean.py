import findspark
import warnings
from itertools import groupby  # сейчас не используется, можно убрать
import pyspark
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from dotenv import dotenv_values

findspark.init()
warnings.filterwarnings("ignore")

app_name = "Otus"

spark = (
    pyspark.sql.SparkSession
        .builder
        .appName(app_name)
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
)

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

schema = StructType([
    StructField("tranaction_id", IntegerType(), True),
    StructField("tx_datetime", TimestampType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("terminal_id", IntegerType(), True),
    StructField("tx_amount", DoubleType(), True),
    StructField("tx_time_seconds", IntegerType(), True),
    StructField("tx_time_days", IntegerType(), True),
    StructField("tx_fraud", IntegerType(), True),
    StructField("tx_fraud_scenario", IntegerType(), True),
])

df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("comment", "#")
    .option("inferSchema", "false")
    .schema(schema)
    .load("hdfs:///user/ubuntu/data/*")
)

df.show(5, truncate=False)
df.printSchema()

# Сохранение в Parquet
(
    df.write
      .mode("overwrite")
      .parquet("hdfs:///user/ubuntu/tx_parquet")
)

# 1) Некорректная сумма операций
bad_cond = (F.col("tx_amount") <= 0)
bad_count = df.filter(bad_cond).count()
print("Строк с некорректной суммой операций:", bad_count)
df.filter(bad_cond).show(20, truncate=False)

df_clean = df.filter(~bad_cond)
print("Строк после очистки (tx_amount):", df_clean.count())
df = df_clean

# 2) Плохие customer_id / terminal_id
bad_cond = (
    (F.col("customer_id") == 0) |
    (F.col("terminal_id") == 0) |
    F.col("customer_id").isNull() |
    F.col("terminal_id").isNull()
)

bad_count = df.filter(bad_cond).count()
print("Строк с плохими customer_id/terminal_id:", bad_count)
df.filter(bad_cond).show(20, truncate=False)

df_clean = df.filter(~bad_cond)
print("Строк после очистки (ids):", df_clean.count())
df = df_clean

# 3) Некорректные datetime (NaT / NULL)
bad_cond = F.col("tx_datetime").isNull()
bad_count = df.filter(bad_cond).count()
print("Строк с некорректными datetime:", bad_count)
df.filter(bad_cond).show(20, truncate=False)

df_clean = df.filter(~bad_cond)
print("Строк после очистки (datetime):", df_clean.count())
df = df_clean

# ==== Размещение в бакете ====

sc = spark.sparkContext
hconf = sc._jsc.hadoopConfiguration()

creds = dotenv_values("/home/ubuntu/s3_bucket_clean.env")
access_key = creds["S3_ACCESS_KEY"]
secret_key = creds["S3_SECRET_KEY"]
target_path = creds["S3_PATH"] 

hconf.set("fs.s3a.endpoint", "storage.yandexcloud.net")
hconf.set("fs.s3a.access.key", access_key)
hconf.set("fs.s3a.secret.key", secret_key)
hconf.set("fs.s3a.path.style.access", "true")
hconf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

(df.write
   .mode("overwrite")
   .parquet(target_path))
