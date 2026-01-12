import argparse
import warnings
import sys
import traceback
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampType
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from datetime import datetime

warnings.filterwarnings("ignore")

def main():


    print("ARGS:", sys.argv)

    print("=== DEBUG ARGS ===")
    for i, arg in enumerate(sys.argv):
        print(f"ARG {i}: {arg}")
    print("==================")

    parser = argparse.ArgumentParser()
    parser.add_argument("--source-bucket")
    parser.add_argument("--source-key")
    parser.add_argument("--clean-access-key")
    parser.add_argument("--clean-secret-key")
    parser.add_argument("--clean-path")
    args, unknown = parser.parse_known_args()

    print(f"PARSED source_key='{args.source_key}' clean='{args.clean_path}'")


    print("OtusDataPipeline START")
    spark = SparkSession.builder.appName("OtusDataPipeline").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    total_rows = 0
    final_count = 0
    target_path = ""

    try:
        print(f"Source: s3a://{args.source_bucket}/{args.source_key}")
        print(f"Target: {args.clean_path}")
        print(f"Clean bucket keys: {args.clean_access_key[:10]}...")

        # 1. ЧТЕНИЕ ДАННЫХ
        s3a_source = f"s3a://{args.source_bucket}/{args.source_key}"
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

        df = spark.read.option("header", "true").schema(schema).csv(s3a_source)
        total_rows = df.count()
        print(f"RAW DATA: {total_rows:,} rows")
        df.show(5, truncate=50)

        # 2. ОЧИСТКА
        print("CLEANING...")
        df_clean = df.filter(F.col("tx_amount") > 0)
        df_clean = df_clean.filter((F.col("customer_id") > 0) & (F.col("terminal_id") > 0))
        df_clean = df_clean.filter(F.col("tx_datetime").isNotNull())
        final_count = df_clean.count()
        print(f"CLEAN DATA: {final_count:,} rows ({total_rows - final_count:,} removed)")

        # 3. S3 CONFIG для целевого бакета
        sc = spark.sparkContext
        hconf = sc._jsc.hadoopConfiguration()
        hconf.set("fs.s3a.endpoint", "https://storage.yandexcloud.net")
        hconf.set("fs.s3a.access.key", args.clean_access_key)
        hconf.set("fs.s3a.secret.key", args.clean_secret_key)
        hconf.set("fs.s3a.path.style.access", "true")
        hconf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hconf.set("fs.s3a.connection.ssl.enabled", "true")
        print("S3 CONFIG OK")

        # 4. ЗАПИСЬ ОЧИЩЕННЫХ ДАННЫХ
        target_path = args.clean_path.rstrip('/')
        print(f"WRITING TO: {target_path}")
        df_clean.coalesce(4).write.mode("overwrite").parquet(target_path)
        print("PARQUET WRITTEN")

        # 5. ПРОВЕРКА
        check_df = spark.read.parquet(target_path)
        check_count = check_df.count()
        print(f"VERIFY: {check_count:,} rows in target ✓")

        # 6. ЛОГИ -> mlops-2025-dz3
        log_bucket = "mlops-2025-dz3"
        logs_path = f"s3a://{log_bucket}/logs/result_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        spark.range(1).selectExpr(
            f"'SUCCESS {datetime.now().isoformat()}' as timestamp",
            f"'{total_rows} -> {final_count} rows written to {target_path}' as message"
        ).coalesce(1).write.mode("overwrite").json(logs_path)
        print(f"LOGS SAVED: {logs_path}")

        print("PIPELINE COMPLETED SUCCESSFULLY!")

    except Exception as e:
        print(f"ERROR: {str(e)}")
        traceback.print_exc()
        sys.exit(1)
    finally:
        print(f"SUMMARY: {total_rows:,} -> {final_count:,} rows")
        spark.stop()

if __name__ == "__main__":
    main()
