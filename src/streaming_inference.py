#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import os
import sys
import tempfile
from typing import Dict, List, Tuple

import mlflow
import mlflow.spark
from mlflow.tracking import MlflowClient

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.ml.functions import vector_to_array


TS_FMT = "yyyy-MM-dd HH:mm:ss"


def parse_args():
    p = argparse.ArgumentParser("Kafka -> Spark ML (MLflow) -> Kafka streaming inference")
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

    p.add_argument("--mlflow-uri", required=True)
    p.add_argument("--model-uri", required=True, help="models:/<name>@<alias>")

    p.add_argument("--run-seconds", type=int, default=600)
    p.add_argument("--exp-tag", default="")
    p.add_argument("--tps", type=float, default=None)

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
    return SparkSession.builder.appName("FraudStreamingInference").getOrCreate()


def parse_models_uri(uri: str) -> Tuple[str, str]:
    if not uri.startswith("models:/"):
        raise ValueError(f"Unsupported model uri: {uri}")
    rest = uri[len("models:/"):]
    if "@" not in rest:
        raise ValueError("Expected alias form models:/name@alias")
    name, alias = rest.split("@", 1)
    return name, alias


def load_feature_cols_from_mlflow(mlflow_uri: str, model_uri: str) -> List[str]:
    mlflow.set_tracking_uri(mlflow_uri)
    client = MlflowClient()

    model_name, model_alias = parse_models_uri(model_uri)
    mv = client.get_model_version_by_alias(model_name, model_alias)
    run_id = mv.run_id

    with tempfile.TemporaryDirectory() as td:
        local_path = client.download_artifacts(run_id, "schema/feature_cols.json", td)
        with open(local_path, "r", encoding="utf-8") as f:
            cols = json.load(f)

    if not isinstance(cols, list) or not cols:
        raise ValueError("feature_cols.json is empty or invalid")
    return cols


def build_input_schema(feature_cols: List[str]) -> T.StructType:
    fields = [
        T.StructField("tx_id", T.StringType(), True),
        T.StructField("event_ts", T.StringType(), True),
        T.StructField("exp_tag", T.StringType(), True),
        T.StructField("tps", T.DoubleType(), True),
    ]
    for c in feature_cols:
        fields.append(T.StructField(c, T.DoubleType(), True))
    return T.StructType(fields)

def main():
    args = parse_args()

    os.environ["AWS_ACCESS_KEY_ID"] = args.access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = args.secret_key
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = args.s3_endpoint

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    feature_cols = load_feature_cols_from_mlflow(args.mlflow_uri, args.model_uri)
    schema = build_input_schema(feature_cols)

    mlflow.set_tracking_uri(args.mlflow_uri)
    model = mlflow.spark.load_model(args.model_uri)

    read_opts = kafka_common_options(args)
    read_opts.update({
        "subscribe": args.topic_in,
        "startingOffsets": args.starting_offsets,
        "failOnDataLoss": "false",
    })
    if args.max_offsets_per_trigger is not None:
        read_opts["maxOffsetsPerTrigger"] = str(args.max_offsets_per_trigger)

    raw = spark.readStream.format("kafka").options(**read_opts).load()

    parsed = (
        raw.select(
            F.col("timestamp").alias("kafka_ts"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("key").cast("string").alias("key"),
            F.col("value").cast("string").alias("json"),
        )
        .withColumn("data", F.from_json(F.col("json"), schema))
        .select("kafka_ts", "topic", "partition", "offset", "key", F.col("data.*"))
        .withColumn("event_ts", F.to_timestamp(F.col("event_ts"), TS_FMT))
        .withColumn("tranaction_id", F.col("tx_id").cast("double"))
        .withColumn("tx_fraud_scenario", F.lit(0.0))
        .filter(F.col("tx_id").isNotNull() & F.col("event_ts").isNotNull())
    )

    if args.exp_tag:
        parsed = parsed.filter(F.col("exp_tag") == args.exp_tag)

    if feature_cols:
        cond = None
        for c in feature_cols:
            x = F.col(c).isNotNull()
            cond = x if cond is None else (cond & x)
        parsed = parsed.filter(cond)

    pred = model.transform(parsed)

    if "probability" not in pred.columns:
        raise ValueError(f"Model output has no 'probability' column, columns={pred.columns}")

    p = vector_to_array("probability")
    pred = (
        pred
        .withColumn("p0", p[0].cast("double"))
        .withColumn("p1", p[1].cast("double"))
        .withColumn("score", p[1].cast("double"))
    )

    out = (
        pred.withColumn("proc_ts", F.current_timestamp())
            .withColumn(
                "out_json",
                F.to_json(F.struct(
                    F.col("tx_id"),
                    F.col("event_ts"),
                    F.col("p0"),
                    F.col("p1"),
                    F.col("score"),
                    F.col("prediction").cast("double").alias("prediction"),
                    F.col("proc_ts"),
                    F.col("kafka_ts"),
                    F.col("topic"),
                    F.col("partition"),
                    F.col("offset"),
                    F.col("exp_tag"),
                    F.col("tps"),
                ))
            )
            .select(
                F.col("tx_id").cast("string").alias("out_key"),
                F.col("out_json").alias("out_value"),
            )
    )

    write_opts = kafka_common_options(args)
    write_opts.update({
        "topic": args.topic_out,
        "checkpointLocation": args.checkpoint,
    })

    query = (
        out.selectExpr("CAST(out_key AS STRING) AS key", "CAST(out_value AS STRING) AS value")
           .writeStream.format("kafka").options(**write_opts)
           .outputMode("append")
           .start()
    )

    # --- измеряем, сколько реально обработали за run-seconds ---
    import time
    start = time.time()
    total_processed = 0

    while True:
        last = query.lastProgress
        if last is not None:
            # numInputRows — сколько строк прочитано в последнем триггере
            total_processed += last.get("numInputRows", 0)
        if time.time() - start >= args.run_seconds:
            break
        time.sleep(1)

    elapsed = time.time() - start or 1.0
    processed_tps = total_processed / elapsed

    print(f"[METRICS] exp_tag={args.exp_tag} tps_param={args.tps} "
          f"total_processed={total_processed} elapsed_sec={elapsed:.1f} "
          f"processed_tps={processed_tps:.2f}")
    sys.stdout.flush()
    # --- конец блока измерений ---

    query.stop()
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
