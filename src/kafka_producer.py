#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import sys
import time
from typing import Dict, List, Tuple

from pyspark.sql import SparkSession, functions as F


def parse_args():
    p = argparse.ArgumentParser("Parquet -> Kafka producer (throttled)")
    p.add_argument("--s3-endpoint", required=True)
    p.add_argument("--access-key", required=True)
    p.add_argument("--secret-key", required=True)

    p.add_argument("--input", required=True, help="s3a://.../history/by_file/")
    p.add_argument("--last-n", type=int, default=1)

    p.add_argument("--bootstrap", required=True)
    p.add_argument("--topic", required=True)

    p.add_argument("--kafka-user", required=True)
    p.add_argument("--kafka-password", required=True)
    p.add_argument("--security-protocol", default="SASL_PLAINTEXT")
    p.add_argument("--sasl-mechanism", default="SCRAM-SHA-256")

    p.add_argument("--tps", type=float, default=50.0)
    p.add_argument("--duration-sec", type=int, default=600)

    p.add_argument("--tx-id-col", default=None)
    p.add_argument("--event-ts-col", default=None)
    p.add_argument("--max-rows", type=int, default=200000)
    p.add_argument("--exp-tag", required=True)
    
    return p.parse_args()


def create_spark() -> SparkSession:
    return SparkSession.builder.appName("FraudKafkaProducer").getOrCreate()


def set_s3a_conf(spark: SparkSession, endpoint: str, access_key: str, secret_key: str):
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    hconf.set("fs.s3a.access.key", access_key)
    hconf.set("fs.s3a.secret.key", secret_key)
    hconf.set("fs.s3a.endpoint", endpoint)
    hconf.set("fs.s3a.path.style.access", "true")
    hconf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")


def resolve_id_ts_cols(cols: List[str], tx_id_col: str = None, event_ts_col: str = None) -> Tuple[str, str]:
    if tx_id_col is None:
        for c in ["tx_id", "transaction_id", "tranaction_id"]:
            if c in cols:
                tx_id_col = c
                break
    if event_ts_col is None:
        for c in ["event_ts", "tx_datetime", "event_time", "timestamp"]:
            if c in cols:
                event_ts_col = c
                break
    if tx_id_col is None:
        raise ValueError(f"Cannot resolve tx id column; parquet columns={cols}")
    if event_ts_col is None:
        raise ValueError(f"Cannot resolve event ts column; parquet columns={cols}")
    return tx_id_col, event_ts_col


def kafka_options(args) -> Dict[str, str]:
    jaas = (
        'org.apache.kafka.common.security.scram.ScramLoginModule required '
        f'username="{args.kafka_user}" password="{args.kafka_password}";'
    )
    return {
        "kafka.bootstrap.servers": args.bootstrap,
        "kafka.security.protocol": args.security_protocol,
        "kafka.sasl.mechanism": args.sasl_mechanism,
        "kafka.sasl.jaas.config": jaas,
        "topic": args.topic,
        "kafka.request.timeout.ms": "3000",
        "kafka.delivery.timeout.ms": "6000",
        "kafka.max.block.ms": "6000",
    }


def list_last_n_date_dirs(spark: SparkSession, root: str, last_n: int) -> List[str]:
    if last_n is None or last_n <= 0:
        return [root.rstrip("/") + "/"]
    jsc = spark.sparkContext._jsc
    jvm = spark.sparkContext._jvm
    root = root.rstrip("/") + "/"
    root_path = jvm.org.apache.hadoop.fs.Path(root)
    fs = root_path.getFileSystem(jsc.hadoopConfiguration())
    statuses = fs.listStatus(root_path)

    dirs = []
    for st in statuses:
        if st.isDirectory():
            name = st.getPath().getName()
            if len(name) == 10 and name[4] == "-" and name[7] == "-":
                dirs.append(root + name + "/")
    dirs.sort()
    return dirs[-last_n:] if dirs else [root]


def main():
    args = parse_args()
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    set_s3a_conf(spark, args.s3_endpoint, args.access_key, args.secret_key)

    paths = list_last_n_date_dirs(spark, args.input, args.last_n)
    df = spark.read.option("recursiveFileLookup", "true").parquet(*paths)

    tx_id_src, event_ts_src = resolve_id_ts_cols(df.columns, args.tx_id_col, args.event_ts_col)

    df2 = df
    if tx_id_src != "tx_id":
        df2 = df2.withColumnRenamed(tx_id_src, "tx_id")
    if event_ts_src != "event_ts":
        df2 = df2.withColumnRenamed(event_ts_src, "event_ts")

    # event_ts string for stable JSON parsing downstream
    df2 = df2.withColumn("event_ts", F.date_format(F.col("event_ts"), "yyyy-MM-dd HH:mm:ss"))
    
    df2 = df2.withColumn("exp_tag", F.lit(args.exp_tag))
    df2 = df2.withColumn("tps", F.lit(float(args.tps)))


    drop_cols = {"tx_fraud", "tx_fraud_scenario"}
    keep_cols = [c for c in df2.columns if c not in drop_cols]

    payload = (
        df2.select(*[F.col(c) for c in keep_cols])
           .withColumn("value", F.to_json(F.struct(*[F.col(c) for c in keep_cols])))
           .withColumn("key", F.col("tx_id").cast("string"))
           .selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value")
           .filter(F.col("key").isNotNull() & (F.length(F.col("key")) > 0) & F.col("value").isNotNull())
           #.dropDuplicates(["key"])   # важно для анти-джойна
           #.cache()
    )
    
    payload = payload.limit(args.max_rows)
    
    has_rows = payload.limit(1).count()
    print(f"HAS_ROWS={has_rows}", flush=True)
    if has_rows == 0:
        raise ValueError("No rows to send from parquet (payload is empty)")

      
    
    duration = int(args.duration_sec)
    per_tick = max(int(args.tps), 1)
    total = min(per_tick * duration, args.max_rows)


    print(f"PLAN per_tick={per_tick} duration={duration} total={total}", flush=True)

    t0 = time.time()
    payload.limit(total).write.format("kafka").options(**kafka_options(args)).save()
    dt = time.time() - t0

    print(f"SENT={total} rows to topic={args.topic} WRITE_SEC={dt:.1f}", flush=True)
      
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
