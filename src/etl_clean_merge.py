import argparse
import warnings
import sys
import traceback
from datetime import datetime, date, timedelta
from urllib.parse import urlparse
from typing import Tuple, Optional, List

import boto3
import botocore

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampType
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

warnings.filterwarnings("ignore")


def parse_s3a_uri(uri: str) -> Tuple[str, str]:
    u = urlparse(uri)
    if u.scheme not in ("s3a", "s3"):
        raise ValueError("Expected s3a:// or s3://, got: {}".format(uri))
    return u.netloc, u.path.lstrip("/")


def parse_ymd(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def make_s3_client(endpoint: str, access_key: str, secret_key: str):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )


def read_watermark_date(s3, bucket: str, key: str) -> Optional[date]:
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        txt = obj["Body"].read().decode("utf-8").strip()
        if not txt:
            return None
        if txt.endswith(".txt"):
            txt = txt[:-4]
        return parse_ymd(txt)
    except s3.exceptions.NoSuchKey:
        return None
    except botocore.exceptions.ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            return None
        raise


def write_watermark_date(s3, bucket: str, key: str, d: date) -> None:
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=(d.strftime("%Y-%m-%d") + "\n").encode("utf-8"),
    )


def configure_s3a_for_yc(spark, endpoint, access_key, secret_key):
    sc = spark.sparkContext
    hconf = sc._jsc.hadoopConfiguration()
    hconf.set("fs.s3a.endpoint", endpoint.replace("https://", "").replace("http://", ""))
    hconf.set("fs.s3a.access.key", access_key)
    hconf.set("fs.s3a.secret.key", secret_key)
    hconf.set("fs.s3a.path.style.access", "true")
    hconf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hconf.set("fs.s3a.connection.ssl.enabled", "true")


def try_read_one_row(spark: SparkSession, path: str, schema) -> bool:
    try:
        spark.read.option("header", "true").schema(schema).csv(path).limit(1).count()
        return True
    except Exception:
        return False


def main():
    print("ARGS:", sys.argv)

    parser = argparse.ArgumentParser()
    parser.add_argument("--source-bucket", required=True)

    parser.add_argument("--start-date", default="2019-08-01")
    parser.add_argument("--take-n", type=int, default=3)
    parser.add_argument("--max-scan-days", type=int, default=30)

    parser.add_argument("--s3-endpoint", default="https://storage.yandexcloud.net")
    parser.add_argument("--clean-access-key", required=True)
    parser.add_argument("--clean-secret-key", required=True)

    parser.add_argument("--clean-path", required=True)  # s3a://mlops-2025-dz3/clean/history/
    parser.add_argument("--watermark-relkey", default="_watermark/last_date.txt")

    args, _ = parser.parse_known_args()

    spark = SparkSession.builder.appName("OtusDataPipeline").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
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

    try:
        configure_s3a_for_yc(spark, args.s3_endpoint, args.clean_access_key, args.clean_secret_key)

        clean_bucket, clean_prefix = parse_s3a_uri(args.clean_path.rstrip("/") + "/")
        watermark_bucket = clean_bucket
        watermark_key = clean_prefix.rstrip("/") + "/" + args.watermark_relkey.lstrip("/")

        s3_clean = make_s3_client(args.s3_endpoint, args.clean_access_key, args.clean_secret_key)

        last_date = read_watermark_date(s3_clean, watermark_bucket, watermark_key)
        if last_date is None:
            last_date = parse_ymd(args.start_date)

        print("WATERMARK_BUCKET=", watermark_bucket)
        print("WATERMARK_KEY=", watermark_key)
        print("WATERMARK_LAST_DATE=", last_date.strftime("%Y-%m-%d"))

        found = []  # type: List[Tuple[str, date]]
        d = last_date + timedelta(days=1)
        scanned = 0
        while len(found) < args.take_n and scanned < args.max_scan_days:
            key = d.strftime("%Y-%m-%d") + ".txt"
            path = "s3a://{}/{}".format(args.source_bucket, key)
            if try_read_one_row(spark, path, schema):
                found.append((key, d))
            d += timedelta(days=1)
            scanned += 1

        if not found:
            print("No new existing source files found; exiting OK")
            return

        run_id = "clean_{}".format(datetime.now().strftime("%Y%m%d_%H%M%S"))
        base_out = args.clean_path.rstrip("/")

        total_rows = 0
        final_rows = 0

        for i, (key, d) in enumerate(found, start=1):
            src = "s3a://{}/{}".format(args.source_bucket, key)
            out = "{}/by_file/{}".format(base_out, d.strftime("%Y-%m-%d"))

            print("[{}/{}] READ: {}".format(i, len(found), src))
            df = spark.read.option("header", "true").schema(schema).csv(src)

            cnt_raw = df.count()
            df_clean = (df
                        .filter(F.col("tx_amount") > 0)
                        .filter((F.col("customer_id") > 0) & (F.col("terminal_id") > 0))
                        .filter(F.col("tx_datetime").isNotNull())
                        )
            cnt_clean = df_clean.count()

            total_rows += cnt_raw
            final_rows += cnt_clean

            df_out = (df_clean
                      .withColumn("_source_key", F.lit(key))
                      .withColumn("_ingest_run_id", F.lit(run_id))
                      .withColumn("_ingest_ts", F.current_timestamp())
                      )

            print("[{}/{}] WRITE: {}".format(i, len(found), out))
            df_out.coalesce(4).write.mode("overwrite").parquet(out)

        last_processed_date = found[-1][1]
        write_watermark_date(s3_clean, watermark_bucket, watermark_key, last_processed_date)
        print("WATERMARK_UPDATED_TO=", last_processed_date.strftime("%Y-%m-%d"))
        print("SUMMARY: {:,} -> {:,} rows".format(total_rows, final_rows))

    except Exception as e:
        print("ERROR:", str(e), file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
