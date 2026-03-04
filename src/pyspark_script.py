#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import warnings
import sys
import traceback
from datetime import datetime
import time
import uuid
import json

import mlflow
import mlflow.spark
import boto3

from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType, TimestampType
)
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

warnings.filterwarnings("ignore")


# ---------- S3 EVENT LOGGER (new object per event) ----------
def make_s3_client(access_key: str, secret_key: str):
    session = boto3.session.Session()
    return session.client(
        service_name="s3",
        endpoint_url="https://storage.yandexcloud.net",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )


class S3EventLogger:
    """
    Writes each event as a separate object:
    s3://<bucket>/<prefix>/<run_id>/events/<ts_ms>_<step>_<state>.json
    """
    def __init__(self, s3, bucket: str, prefix: str, run_id: str):
        self.s3 = s3
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.run_id = run_id

    def emit(self, step: str, state: str, message: str = "", extra: dict = None):
        ts_ms = int(time.time() * 1000)
        key = f"{self.prefix}/{self.run_id}/events/{ts_ms}_{step}_{state}.json"
        payload = {
            "ts_ms": ts_ms,
            "run_id": self.run_id,
            "step": step,
            "state": state,
            "message": message,
        }
        if extra:
            payload.update(extra)

        # put_object creates object for the provided Bucket/Key [web:100]
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=(json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8"),
            ContentType="application/json",
        )
        return key


def safe_emit(eventlog, step: str, state: str, message: str = "", extra: dict = None):
    try:
        if eventlog is not None:
            eventlog.emit(step=step, state=state, message=message, extra=extra)
    except Exception:
        pass


def run_step(spark, eventlog, step: str, description: str, fn, extra_start=None, extra_done=None):
    """
    Wrap a step with:
    - S3 event log START/DONE/ERROR
    - optional Spark UI job group (nice if you later open Spark UI)
    """
    sc = spark.sparkContext
    t0 = time.time()
    safe_emit(eventlog, step, "START", message=description, extra=extra_start)

    # Spark job grouping improves visibility in Spark UI [web:31]
    try:
        sc.setJobGroup(step, description)
    except Exception:
        pass

    try:
        res = fn()
        dt = int((time.time() - t0) * 1000)
        extra = {"duration_ms": dt}
        if extra_done:
            extra.update(extra_done)
        safe_emit(eventlog, step, "DONE", message=description, extra=extra)
        return res
    except Exception as e:
        dt = int((time.time() - t0) * 1000)
        safe_emit(eventlog, step, "ERROR", message=str(e), extra={"duration_ms": dt})
        raise
    finally:
        try:
            sc.setJobGroup("", "")
        except Exception:
            pass


# ---------- ARGS ----------
def parse_args():
    p = argparse.ArgumentParser()

    p.add_argument("--source-bucket", required=True)
    p.add_argument("--source-key", required=True)

    p.add_argument("--clean-access-key", required=True)
    p.add_argument("--clean-secret-key", required=True)
    p.add_argument("--clean-path", required=True)

    p.add_argument("--mlflow-tracking-uri", required=True)
    p.add_argument("--mlflow-experiment", default="fraud_spark")
    p.add_argument("--mlflow-run-name", default=None)

    p.add_argument("--train-split", type=float, default=0.5, help="Train split ratio (0..1)")
    p.add_argument("--rf-num-trees", type=int, default=50)
    p.add_argument("--rf-max-depth", type=int, default=8)

    # Performance / observability toggles
    p.add_argument("--debug-show", action="store_true", help="df.show(5) (extra Spark job) [web:129]")
    p.add_argument("--do-counts", action="store_true",
                   help="Compute row counts (extra Spark jobs) [web:11]")
    p.add_argument("--verify-write", action="store_true",
                   help="Read parquet back and count() (extra Spark job)")

    p.add_argument("--write-partitions", type=int, default=0,
                   help="If >0: repartition(N) before parquet write; 0: keep current partitions")
    p.add_argument("--persist-level", default="MEMORY_AND_DISK",
                   help="MEMORY_AND_DISK or DISK_ONLY etc (StorageLevel)")

    args, unknown = p.parse_known_args()
    if unknown:
        print("WARN: Unknown args:", unknown)
    return args


def get_storage_level(name: str):
    name = (name or "").upper().strip()
    return getattr(StorageLevel, name, StorageLevel.MEMORY_AND_DISK)


def configure_s3a_for_yc(spark, access_key, secret_key):
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    hconf.set("fs.s3a.endpoint", "https://storage.yandexcloud.net")
    hconf.set("fs.s3a.access.key", access_key)
    hconf.set("fs.s3a.secret.key", secret_key)
    hconf.set("fs.s3a.path.style.access", "true")
    hconf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hconf.set("fs.s3a.connection.ssl.enabled", "true")


def main():
    print("ARGS:", sys.argv)
    args = parse_args()

    spark = SparkSession.builder.appName("OtusDataPipeline").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    total_rows = None
    final_count = None
    target_path = args.clean_path.rstrip("/")

    # ---- init event logger (writes to mlops-2025-dz3/logs/<run_id>/events/...) ----
    log_bucket = "mlops-2025-dz3"
    run_id = args.mlflow_run_name or f"result_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    eventlog = None
    try:
        s3_client = make_s3_client(args.clean_access_key, args.clean_secret_key)
        eventlog = S3EventLogger(s3_client, bucket=log_bucket, prefix="logs", run_id=run_id)
    except Exception as e:
        print(f"WARN: cannot init S3 event logger: {e}")

    safe_emit(eventlog, "pipeline", "START", extra={
        "run_id": run_id,
        "source_bucket": args.source_bucket,
        "source_key": args.source_key,
        "clean_path": args.clean_path,
        "do_counts": bool(args.do_counts),
        "debug_show": bool(args.debug_show),
        "verify_write": bool(args.verify_write),
        "write_partitions": int(args.write_partitions),
    })

    persist_level = get_storage_level(args.persist_level)

    try:
        run_step(
            spark, eventlog, "s3a_config", "Configure S3A for YC Object Storage",
            lambda: configure_s3a_for_yc(spark, args.clean_access_key, args.clean_secret_key),
        )

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

        df = run_step(
            spark, eventlog, "read_csv", f"Read CSV from {s3a_source}",
            lambda: (spark.read.option("header", "true").schema(schema).csv(s3a_source)),
            extra_start={"source": s3a_source},
        )

        if args.debug_show:
            run_step(spark, eventlog, "debug_show", "Show 5 rows (debug)", lambda: df.show(5, truncate=50))

        # CLEAN + persist because we will use it for write + training
        df_clean = run_step(
            spark, eventlog, "clean", "Filter/clean dataset and persist",
            lambda: (df
                .filter(F.col("tx_amount") > 0)
                .filter((F.col("customer_id") > 0) & (F.col("terminal_id") > 0))
                .filter(F.col("tx_datetime").isNotNull())
                .persist(persist_level)
            ),
        )

        # Materialize persist once (otherwise write + train will each trigger computation)
        run_step(
            spark, eventlog, "materialize_clean", "Materialize persisted cleaned DF",
            lambda: df_clean.count(),  # one deliberate action to “lock in” cache [web:5]
        )

        if args.do_counts:
            total_rows = run_step(spark, eventlog, "raw_count", "Count raw rows", lambda: df.count())
            final_count = run_step(spark, eventlog, "clean_count", "Count clean rows", lambda: df_clean.count())
        else:
            # keep summary lightweight
            total_rows = -1
            final_count = -1

        # WRITE PARQUET (no hard-coded coalesce)
        def write_parquet():
            out_df = df_clean
            if args.write_partitions and int(args.write_partitions) > 0:
                out_df = out_df.repartition(int(args.write_partitions))
            out_df.write.mode("overwrite").parquet(target_path)

        run_step(
            spark, eventlog, "write_parquet", f"Write parquet to {target_path}",
            write_parquet,
            extra_start={"target_path": target_path},
        )

        if args.verify_write:
            run_step(
                spark, eventlog, "verify_parquet", "Read parquet and count (verify)",
                lambda: spark.read.parquet(target_path).count(),
            )

        # TRAIN/EVAL
        def prep_ml():
            return (df_clean
                .withColumn("label", F.col("tx_fraud").cast("double"))
                .withColumn("tx_time_seconds", F.col("tx_time_seconds").cast("double"))
                .withColumn("tx_time_days", F.col("tx_time_days").cast("double"))
                .dropna(subset=["label", "tx_amount", "tx_time_seconds", "tx_time_days"])
                .persist(persist_level)
            )

        ml_df = run_step(spark, eventlog, "train_prep", "Prepare ML dataframe and persist", prep_ml)
        run_step(spark, eventlog, "materialize_ml", "Materialize persisted ML DF", lambda: ml_df.count())

        train_ratio = float(args.train_split)
        if not (0.0 < train_ratio < 1.0):
            raise ValueError("--train-split must be in (0..1)")

        train_df, test_df = run_step(
            spark, eventlog, "train_split", f"randomSplit train={train_ratio}",
            lambda: ml_df.randomSplit([train_ratio, 1.0 - train_ratio], seed=42),
            extra_start={"train_split": train_ratio},
        )

        feature_cols = ["tx_amount", "tx_time_seconds", "tx_time_days"]

        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
        scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)
        rf = RandomForestClassifier(
            labelCol="label",
            featuresCol="features",
            numTrees=int(args.rf_num_trees),
            maxDepth=int(args.rf_max_depth),
            seed=42,
        )
        pipeline = Pipeline(stages=[assembler, scaler, rf])

        model = run_step(
            spark, eventlog, "model_fit", "Fit Spark ML pipeline",
            lambda: pipeline.fit(train_df),
            extra_start={"rf_num_trees": int(args.rf_num_trees), "rf_max_depth": int(args.rf_max_depth)},
        )

        pred = run_step(
            spark, eventlog, "model_predict", "Predict on test and persist",
            lambda: model.transform(test_df).persist(persist_level),
        )
        run_step(spark, eventlog, "materialize_pred", "Materialize predictions DF", lambda: pred.count())

        evaluator_auc = BinaryClassificationEvaluator(
            labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
        )
        evaluator_acc = MulticlassClassificationEvaluator(
            labelCol="label", predictionCol="prediction", metricName="accuracy"
        )
        evaluator_f1 = MulticlassClassificationEvaluator(
            labelCol="label", predictionCol="prediction", metricName="f1"
        )

        def evaluate_all():
            auc = float(evaluator_auc.evaluate(pred))
            acc = float(evaluator_acc.evaluate(pred))
            f1 = float(evaluator_f1.evaluate(pred))
            return auc, acc, f1

        auc, accuracy, f1 = run_step(spark, eventlog, "evaluate", "Evaluate metrics (AUC/ACC/F1)", evaluate_all)

        # MLFLOW
        def mlflow_log():
            mlflow.set_tracking_uri(args.mlflow_tracking_uri)
            mlflow.set_experiment(args.mlflow_experiment)

            run_name = args.mlflow_run_name or f"train_{args.source_key.replace('/', '_')}"
            with mlflow.start_run(run_name=run_name):
                mlflow.log_param("source_bucket", args.source_bucket)
                mlflow.log_param("source_key", args.source_key)
                mlflow.log_param("clean_path", target_path)

                mlflow.log_param("features", ",".join(feature_cols))
                mlflow.log_param("train_split", train_ratio)
                mlflow.log_param("model_type", "RandomForestClassifier")
                mlflow.log_param("rf_num_trees", int(args.rf_num_trees))
                mlflow.log_param("rf_max_depth", int(args.rf_max_depth))

                mlflow.log_metric("auc", auc)
                mlflow.log_metric("accuracy", accuracy)
                mlflow.log_metric("f1", f1)

                mlflow.spark.log_model(model, artifact_path="model")

        run_step(
            spark, eventlog, "mlflow", "Log params/metrics/model to MLflow",
            mlflow_log,
            extra_start={"tracking_uri": args.mlflow_tracking_uri, "experiment": args.mlflow_experiment},
            extra_done={"auc": auc, "accuracy": accuracy, "f1": f1},
        )

        # Keep your final marker (single write)
        def final_marker():
            logs_path = f"s3a://{log_bucket}/logs/result_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            spark.range(1).selectExpr(
                f"'SUCCESS {datetime.now().isoformat()}' as timestamp",
                f"'parquet={target_path}; auc={auc:.6f}; acc={accuracy:.6f}; f1={f1:.6f}' as message"
            ).coalesce(1).write.mode("overwrite").json(logs_path)
            return logs_path

        logs_path = run_step(spark, eventlog, "final_marker", "Write final success marker JSON", final_marker)
        safe_emit(eventlog, "pipeline", "DONE", extra={"auc": auc, "accuracy": accuracy, "f1": f1, "final_marker": logs_path})

        print("PIPELINE COMPLETED SUCCESSFULLY!")

    except Exception as e:
        safe_emit(eventlog, "pipeline", "ERROR", message=str(e))
        print(f"ERROR: {str(e)}")
        traceback.print_exc()
        sys.exit(1)
    finally:
        safe_emit(eventlog, "pipeline", "FINALLY", extra={"total_rows": total_rows, "final_rows": final_count})
        spark.stop()


if __name__ == "__main__":
    main()
