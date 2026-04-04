#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
import sys
import time
import json
from typing import Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

import requests

from pyspark.sql import SparkSession, functions as F, types as T

TS_FMT = "yyyy-MM-dd HH:mm:ss"
WRITE_CHUNK_SIZE = 10

thread_local = threading.local()


def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session


def parse_args():
    p = argparse.ArgumentParser("Kafka -> HTTP /predict API -> Kafka streaming inference")
    p.add_argument("--bootstrap", required=True)
    p.add_argument("--topic-in", required=True)
    p.add_argument("--topic-out", required=True)

    p.add_argument("--s3-endpoint", required=True)
    p.add_argument("--access-key", required=True)
    p.add_argument("--secret-key", required=True)

    p.add_argument("--kafka-user", required=True)
    p.add_argument("--kafka-password", required=True)
    p.add_argument("--security-protocol", default="SASL_PLAINTEXT")
    p.add_argument("--sasl-mechanism", default="SCRAM-SHA-256")

    p.add_argument("--checkpoint", required=True)
    p.add_argument("--starting-offsets", default="latest", choices=["latest", "earliest"])
    p.add_argument("--max-offsets-per-trigger", type=int, default=None)

    p.add_argument("--api-url", required=True, help="http://host/predict")

    p.add_argument("--run-seconds", type=int, default=600)
    p.add_argument("--exp-tag", default="")
    p.add_argument("--tps", type=float, default=None)

    # новый параметр
    p.add_argument("--max-workers", type=int, default=32)

    return p.parse_args()


def kafka_common_options(args) -> Dict[str, str]:
    jaas = (
        "org.apache.kafka.common.security.scram.ScramLoginModule required "
        f'username="{args.kafka_user}" password="{args.kafka_password}";'
    )
    return {
        "kafka.bootstrap.servers": args.bootstrap,
        "kafka.security.protocol": args.security_protocol,
        "kafka.sasl.mechanism": args.sasl_mechanism,
        "kafka.sasl.jaas.config": jaas,
    }


def create_spark() -> SparkSession:
    return SparkSession.builder.appName("FraudStreamingInferenceHTTPDebug").getOrCreate()


def build_input_schema() -> T.StructType:
    return T.StructType([
        T.StructField("tx_id", T.StringType(), True),
        T.StructField("event_ts", T.StringType(), True),
        T.StructField("exp_tag", T.StringType(), True),
        T.StructField("tps", T.DoubleType(), True),
        T.StructField("customer_id", T.LongType(), True),
        T.StructField("terminal_id", T.LongType(), True),
        T.StructField("tx_amount", T.DoubleType(), True),
        T.StructField("tx_time_seconds", T.LongType(), True),
        T.StructField("tx_time_days", T.LongType(), True),
        T.StructField("tx_fraud_scenario", T.LongType(), True),
    ])


def call_api(payload, api_url: str):
    # оставляем подробные логи, но используем thread-local Session
    print(f"[STEP] Calling API {api_url} with payload={payload}", file=sys.stderr)
    try:
        s = get_session()
        resp = s.post(api_url, json=payload, timeout=(3.0, 10.0))
        print(f"[API] status_code={resp.status_code}", file=sys.stderr)
        resp.raise_for_status()
        data = resp.json()
        print(f"[API] response JSON={data}", file=sys.stderr)
        return data
    except Exception as e:
        print(f"[ERROR] API call failed: {e}", file=sys.stderr)
        return {"error": str(e)}


def safe_int(v, default=0):
    try:
        if v is None:
            return default
        return int(float(v))
    except Exception:
        return default


def safe_float(v, default=0.0):
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def main():
    args = parse_args()

    print("### SCRIPT VERSION: STREAM_CHUNKED_PAR_V2 ###", file=sys.stderr)
    print(
        f"[ARGS] exp_tag={args.exp_tag} tps={args.tps} "
        f"maxOffsetsPerTrigger={args.max_offsets_per_trigger} "
        f"run_seconds={args.run_seconds} write_chunk_size={WRITE_CHUNK_SIZE} "
        f"max_workers={args.max_workers}",
        file=sys.stderr
    )
    sys.stderr.flush()

    os.environ["AWS_ACCESS_KEY_ID"] = args.access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = args.secret_key
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = args.s3_endpoint

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    schema = build_input_schema()

    read_opts = kafka_common_options(args)
    read_opts.update({
        "subscribe": args.topic_in,
        "startingOffsets": args.starting_offsets,
        "failOnDataLoss": "false",
    })

    if args.max_offsets_per_trigger is not None:
        mot = max(1, args.max_offsets_per_trigger)
    else:
        mot = 100

    read_opts["maxOffsetsPerTrigger"] = str(mot)
    print(f"[CFG] Using maxOffsetsPerTrigger={mot}", file=sys.stderr)

    raw = spark.readStream.format("kafka").options(**read_opts).load()

    base_stream = raw.select(
        F.col("timestamp").alias("kafka_ts"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("key").cast("string").alias("key"),
        F.col("value").cast("string").alias("json"),
    )

    write_opts = kafka_common_options(args)
    write_opts.update({
        "topic": args.topic_out,
    })

    api_url = args.api_url

    total_processed = 0
    start_time = time.time()

    def write_chunk(chunk_rows, batch_id, chunk_no):
        if not chunk_rows:
            return 0

        out_df = spark.createDataFrame(
            chunk_rows,
            schema=T.StructType([
                T.StructField("key", T.StringType(), True),
                T.StructField("value", T.StringType(), True),
            ])
        )

        print(
            f"[BATCH] {batch_id}: writing chunk #{chunk_no} with {len(chunk_rows)} rows to Kafka",
            file=sys.stderr
        )

        (
            out_df.selectExpr(
                "CAST(key AS STRING) AS key",
                "CAST(value AS STRING) AS value"
            )
            .write
            .format("kafka")
            .options(**write_opts)
            .mode("append")
            .save()
        )

        print(
            f"[BATCH] {batch_id}: written chunk #{chunk_no} with {len(chunk_rows)} rows",
            file=sys.stderr
        )
        return len(chunk_rows)

    def build_payloads(row_dict, batch_id, idx):
        try:
            payload_in = {
                "tx_id": row_dict.get("tx_id"),
                "event_ts": row_dict.get("event_ts").strftime(TS_FMT) if row_dict.get("event_ts") else None,
                "exp_tag": row_dict.get("exp_tag"),
                "tps": row_dict.get("tps"),
                "customer_id": row_dict.get("customer_id"),
                "terminal_id": row_dict.get("terminal_id"),
                "tx_amount": row_dict.get("tx_amount"),
                "tx_time_seconds": row_dict.get("tx_time_seconds"),
                "tx_time_days": row_dict.get("tx_time_days"),
                "tx_fraud_scenario": row_dict.get("tx_fraud_scenario"),
            }
        except Exception as e:
            print(f"[WARN] {batch_id}#{idx}: failed to build payload_in: {e}", file=sys.stderr)
            payload_in = {"raw": row_dict}

        api_payload = {
            "transaction_id": safe_int(payload_in.get("tx_id"), 0),
            "customer_id": safe_int(payload_in.get("customer_id"), 1001),
            "terminal_id": safe_int(payload_in.get("terminal_id"), 501),
            "tx_amount": safe_float(payload_in.get("tx_amount"), 0.0),
            "tx_time_seconds": safe_int(payload_in.get("tx_time_seconds"), 0),
            "tx_time_days": safe_int(payload_in.get("tx_time_days"), 0),
            "tx_fraud_scenario": safe_int(payload_in.get("tx_fraud_scenario"), 0),
        }
        return payload_in, api_payload

    def process_row(idx, row_dict, batch_id):
        payload_in, api_payload = build_payloads(row_dict, batch_id, idx)
        api_resp = call_api(api_payload, api_url)

        out_payload = {
            "input": payload_in,
            "api_payload": api_payload,
            "api_response": api_resp,
            "meta": {
                "source_topic": row_dict.get("topic"),
                "source_partition": row_dict.get("partition"),
                "source_offset": row_dict.get("offset"),
                "source_kafka_ts": row_dict.get("kafka_ts").strftime(TS_FMT) if row_dict.get("kafka_ts") else None,
                "exp_tag_arg": args.exp_tag,
                "tps_arg": args.tps,
                "batch_id": batch_id,
            },
        }

        out_json = json.dumps(out_payload, ensure_ascii=False)
        out_key = str(payload_in.get("tx_id") or row_dict.get("key") or "")

        return out_key, out_json

    def foreach_batch(batch_df, batch_id):
        nonlocal total_processed

        raw_count = batch_df.count()
        print(f"[BATCH] {batch_id}: raw rows from Kafka = {raw_count}", file=sys.stderr)
        if raw_count == 0:
            return

        parsed_json_df = batch_df.withColumn("data", F.from_json(F.col("json"), schema))
        parsed_json_count = parsed_json_df.filter(F.col("data").isNotNull()).count()
        print(f"[BATCH] {batch_id}: parsed JSON rows = {parsed_json_count}", file=sys.stderr)

        expanded_df = parsed_json_df.select(
            "kafka_ts", "topic", "partition", "offset", "key", "json",
            F.col("data.tx_id").alias("tx_id"),
            F.col("data.event_ts").alias("event_ts_raw"),
            F.col("data.exp_tag").alias("exp_tag"),
            F.col("data.tps").alias("tps"),
            F.col("data.customer_id").alias("customer_id"),
            F.col("data.terminal_id").alias("terminal_id"),
            F.col("data.tx_amount").alias("tx_amount"),
            F.col("data.tx_time_seconds").alias("tx_time_seconds"),
            F.col("data.tx_time_days").alias("tx_time_days"),
            F.col("data.tx_fraud_scenario").alias("tx_fraud_scenario"),
        )

        tx_id_ok_df = expanded_df.filter(F.col("tx_id").isNotNull())
        tx_id_ok_count = tx_id_ok_df.count()
        print(f"[BATCH] {batch_id}: tx_id not null = {tx_id_ok_count}", file=sys.stderr)

        ts_parsed_df = tx_id_ok_df.withColumn("event_ts", F.to_timestamp(F.col("event_ts_raw"), TS_FMT))
        ts_ok_df = ts_parsed_df.filter(F.col("event_ts").isNotNull())
        ts_ok_count = ts_ok_df.count()
        print(f"[BATCH] {batch_id}: event_ts parsed = {ts_ok_count}", file=sys.stderr)

        if args.exp_tag:
            exp_tag_df = ts_ok_df.filter(F.col("exp_tag") == args.exp_tag)
            exp_tag_count = exp_tag_df.count()
            print(f"[BATCH] {batch_id}: exp_tag matched ({args.exp_tag}) = {exp_tag_count}", file=sys.stderr)

            wrong_tag_df = ts_ok_df.filter(F.col("exp_tag") != args.exp_tag)
            wrong_tag_count = wrong_tag_df.count()
            print(f"[BATCH] {batch_id}: exp_tag mismatched = {wrong_tag_count}", file=sys.stderr)

            final_df = exp_tag_df
        else:
            final_df = ts_ok_df

        final_count = final_df.count()
        print(f"[BATCH] {batch_id}: final rows after all filters = {final_count}", file=sys.stderr)

        sample_rows = final_df.select(
            "offset", "tx_id", "event_ts_raw", "exp_tag"
        ).limit(5).collect()
        print(
            f"[BATCH] {batch_id}: sample final rows = {[r.asDict(recursive=True) for r in sample_rows]}",
            file=sys.stderr
        )

        if final_count == 0:
            print(f"[BATCH] {batch_id}: no rows after filters", file=sys.stderr)
            return

        rows = final_df.collect()
        print(f"[BATCH] {batch_id}: collected final rows = {len(rows)}", file=sys.stderr)

        chunk_rows = []
        chunk_no = 0
        written_in_batch = 0

        # параллелизация HTTP‑вызовов
        with ThreadPoolExecutor(max_workers=max(1, args.max_workers)) as ex:
            futures = []
            for idx, r in enumerate(rows):
                rd = r.asDict(recursive=True)
                fut = ex.submit(process_row, idx, rd, batch_id)
                futures.append(fut)

            for fut in as_completed(futures):
                try:
                    out_key, out_json = fut.result()
                except Exception as e:
                    print(f"[ERROR] {batch_id}: worker failed: {e}", file=sys.stderr)
                    continue

                chunk_rows.append((out_key, out_json))

                if len(chunk_rows) >= WRITE_CHUNK_SIZE:
                    chunk_no += 1
                    written = write_chunk(chunk_rows, batch_id, chunk_no)
                    written_in_batch += written
                    total_processed += written
                    chunk_rows = []

        if chunk_rows:
            print(
                f"[BATCH] {batch_id}: flushing tail chunk with {len(chunk_rows)} rows",
                file=sys.stderr
            )
            chunk_no += 1
            written = write_chunk(chunk_rows, batch_id, chunk_no)
            written_in_batch += written
            total_processed += written

        print(
            f"[BATCH] {batch_id}: completed, written_in_batch={written_in_batch}, total_processed={total_processed}",
            file=sys.stderr
        )

    query = (
        base_stream.writeStream
        .foreachBatch(foreach_batch)
        .outputMode("append")
        .option("checkpointLocation", args.checkpoint)
        .start()
    )

    while True:
        elapsed = time.time() - start_time
        if elapsed >= args.run_seconds:
            break
        time.sleep(1)

    processed_tps = total_processed / (elapsed or 1.0)

    print(
        f"[METRICS] exp_tag={args.exp_tag} tps_param={args.tps} "
        f"total_processed={total_processed} elapsed_sec={elapsed:.1f} "
        f"processed_tps={processed_tps:.2f}",
        file=sys.stderr
    )
    sys.stderr.flush()

    query.stop()
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())