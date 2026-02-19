#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import argparse
import traceback
from datetime import datetime
from typing import List, Dict, Any, Optional

import mlflow
import mlflow.spark
from mlflow.tracking import MlflowClient

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import NumericType
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator


def mask_argv(argv: List[str]) -> List[str]:
    masked = []
    hide_next = False
    for a in argv:
        if hide_next:
            masked.append("***")
            hide_next = False
            continue
        masked.append(a)
        if a in ("--secret-key", "--access-key"):
            hide_next = True
    return masked


def parse_args():
    p = argparse.ArgumentParser("Fraud model training + MLflow")

    p.add_argument("--input", required=True, help="Parquet dataset path (e.g. .../history/by_file/)")
    p.add_argument("--output", required=False, default=None)

    p.add_argument("--tracking-uri", required=True, help="MLflow Tracking URI")
    p.add_argument("--experiment-name", required=True)
    p.add_argument("--run-name", default=None)

    p.add_argument("--s3-endpoint", required=True)
    p.add_argument("--access-key", required=True)
    p.add_argument("--secret-key", required=True)

    p.add_argument("--auto-register", action="store_true")
    p.add_argument("--model-name", default=None, help="Registered model name (default: <experiment-name>_model)")
    p.add_argument("--promote-metric", default="auc", choices=["auc", "f1", "accuracy"])

    # NEW: train only on latest N partitions from by_file
    p.add_argument("--last-n", type=int, default=None,
                   help="Train only on last N date subdirectories under --input (expects YYYY-MM-DD folders)")

    return p.parse_args()


def create_spark() -> SparkSession:
    return SparkSession.builder.appName("FraudTrainMLflow").getOrCreate()


def pick_feature_cols(df) -> List[str]:
    return [
        f.name
        for f in df.schema.fields
        if f.name != "tx_fraud" and isinstance(f.dataType, NumericType)
    ]


def build_pipeline(feature_cols: List[str]) -> Pipeline:
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)
    rf = RandomForestClassifier(labelCol="tx_fraud", featuresCol="features", seed=42)
    return Pipeline(stages=[assembler, scaler, rf])


def eval_metrics(pred) -> Dict[str, float]:
    auc_eval = BinaryClassificationEvaluator(
        labelCol="tx_fraud", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
    )
    acc_eval = MulticlassClassificationEvaluator(
        labelCol="tx_fraud", predictionCol="prediction", metricName="accuracy"
    )
    f1_eval = MulticlassClassificationEvaluator(
        labelCol="tx_fraud", predictionCol="prediction", metricName="f1"
    )
    return {
        "auc": float(auc_eval.evaluate(pred)),
        "accuracy": float(acc_eval.evaluate(pred)),
        "f1": float(f1_eval.evaluate(pred)),
    }


def safe_get_rf_params(rf_model) -> Dict[str, Any]:
    out = {}
    try:
        out["maxDepth"] = int(rf_model.getMaxDepth())
    except Exception:
        pass

    try:
        out["numTrees"] = int(getattr(rf_model, "getNumTrees")())
    except Exception:
        try:
            out["numTrees"] = int(getattr(rf_model, "getNumTrees"))
        except Exception:
            pass
    return out


def list_last_n_date_dirs(spark: SparkSession, root: str, last_n: int) -> List[str]:
    """
    root: path like s3a://bucket/.../history/by_file/
    Expects child directories named YYYY-MM-DD
    Returns full paths to last N directories (sorted lexicographically).
    """
    if last_n is None or last_n <= 0:
        return []

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

    dirs.sort()          # YYYY-MM-DD => chronological
    return dirs[-last_n:]


def main():
    print("ARGV=", mask_argv(sys.argv))
    args = parse_args()

    mlflow.set_tracking_uri(args.tracking_uri)
    mlflow.set_experiment(args.experiment_name)

    os.environ["AWS_ACCESS_KEY_ID"] = args.access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = args.secret_key
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = args.s3_endpoint

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Resolve training input to "last N" folders if requested
        if args.last_n:
            train_paths = list_last_n_date_dirs(spark, args.input, args.last_n)
            if not train_paths:
                raise ValueError("No date subdirs found under input={}".format(args.input))
            print("TRAIN_LAST_N=", args.last_n)
            print("TRAIN_PATHS_COUNT=", len(train_paths))
            print("TRAIN_PATHS=", train_paths)
            df0 = (spark.read
                   .option("recursiveFileLookup", "true")
                   .parquet(*train_paths))
        else:
            df0 = (spark.read
                   .option("recursiveFileLookup", "true")
                   .parquet(args.input))

        df = df0.filter(F.col("tx_fraud").isNotNull()).cache()

        print("SCHEMA=")
        df.printSchema()
        total_cnt = df.count()
        print("COUNT=", total_cnt)
        if total_cnt == 0:
            raise ValueError("Input dataset is empty after tx_fraud filter")

        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

        feature_cols = pick_feature_cols(train_df)
        if not feature_cols:
            raise ValueError("No feature columns selected (numeric cols excluding tx_fraud)")

        pipeline = build_pipeline(feature_cols)

        rf = pipeline.getStages()[-1]
        param_grid = (ParamGridBuilder()
                      .addGrid(rf.numTrees, [20, 50])
                      .addGrid(rf.maxDepth, [5, 10])
                      .build())

        evaluator_auc = BinaryClassificationEvaluator(
            labelCol="tx_fraud", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
        )

        cv = CrossValidator(
            estimator=pipeline,
            estimatorParamMaps=param_grid,
            evaluator=evaluator_auc,
            numFolds=3,
            parallelism=4,
        )

        run_name = args.run_name or "fraud_rf_{}".format(datetime.utcnow().strftime("%Y%m%d_%H%M%S"))

        with mlflow.start_run(run_name=run_name) as run:
            run_id = run.info.run_id

            mlflow.log_param("label_col", "tx_fraud")
            mlflow.log_param("features_count", len(feature_cols))
            mlflow.log_param("numTrees_grid", "20,50")
            mlflow.log_param("maxDepth_grid", "5,10")
            if args.last_n:
                mlflow.log_param("train_last_n", int(args.last_n))

            cv_model = cv.fit(train_df)
            best_model = cv_model.bestModel

            pred = best_model.transform(test_df)
            metrics = eval_metrics(pred)
            for k, v in metrics.items():
                mlflow.log_metric(k, v)

            rf_model = best_model.stages[-1]
            for k, v in safe_get_rf_params(rf_model).items():
                mlflow.log_param("best_{}".format(k), v)

            # Log model to MLflow
            mlflow.spark.log_model(best_model, "model")

            # Optional extra save
            if args.output:
                out_path = args.output.rstrip("/")
                best_model.write().overwrite().save(out_path)
                mlflow.log_param("saved_model_path", out_path)

            # Optional registry + simple promotion
            if args.auto_register:
                model_name = args.model_name or "{}_model".format(args.experiment_name)
                model_uri = "runs:/{}/model".format(run_id)
                mv = mlflow.register_model(model_uri, model_name)

                client = MlflowClient()
                promote_metric = args.promote_metric

                champion = None
                try:
                    for v in client.get_latest_versions(model_name):
                        if hasattr(v, "aliases") and v.aliases and ("champion" in v.aliases):
                            champion = v
                            break
                        if hasattr(v, "tags") and v.tags and (v.tags.get("alias") == "champion"):
                            champion = v
                            break
                except Exception:
                    champion = None

                should_promote = True
                if champion is not None:
                    champ_run = client.get_run(champion.run_id)
                    champ_val = champ_run.data.metrics.get(promote_metric)
                    new_val = metrics.get(promote_metric)
                    if champ_val is not None and new_val is not None:
                        should_promote = (new_val > champ_val)

                if should_promote:
                    if hasattr(client, "set_registered_model_alias"):
                        client.set_registered_model_alias(model_name, "champion", mv.version)
                    else:
                        client.set_model_version_tag(model_name, mv.version, "alias", "champion")
                else:
                    if hasattr(client, "set_registered_model_alias"):
                        client.set_registered_model_alias(model_name, "challenger", mv.version)
                    else:
                        client.set_model_version_tag(model_name, mv.version, "alias", "challenger")

            print("OK run_id=", run_id, "metrics=", metrics)

    except Exception as e:
        print("ERROR:", str(e), file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
