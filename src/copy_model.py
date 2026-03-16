#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import argparse
import traceback

import mlflow
from mlflow.tracking import MlflowClient
from mlflow.spark import load_model as load_spark_model

from pyspark.sql import SparkSession


def parse_args():
    p = argparse.ArgumentParser("Copy MLflow champion model to S3")
    p.add_argument("--s3-model", required=True)
    p.add_argument("--output", required=False, default=None)
    p.add_argument("--tracking-uri", required=True)
    p.add_argument("--experiment-name", required=True)
    p.add_argument("--model-name", default=None)
    p.add_argument("--s3-endpoint", required=True)
    p.add_argument("--access-key", required=True)
    p.add_argument("--secret-key", required=True)
    return p.parse_args()


def main():
    args = parse_args()

    mlflow.set_tracking_uri(args.tracking_uri)
    model_name = args.model_name or f"{args.experiment_name}_model"

    os.environ["AWS_ACCESS_KEY_ID"] = args.access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = args.secret_key
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = args.s3_endpoint

    spark = SparkSession.builder.appName("CopyChampionModel").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    client = MlflowClient()

    try:
        # Ищем версию с алиасом champion
        versions = client.get_latest_versions(model_name, stages=None)
        champion_version = None
        for v in versions:
            if hasattr(v, "aliases") and v.aliases and "champion" in v.aliases:
                champion_version = v
                break

        if not champion_version:
            print("ERROR: No version with alias 'champion' found for", model_name)
            sys.exit(1)

        print("FOUND_CHAMPION:", model_name, champion_version.version, champion_version.run_id)

        # Загружаем через run_id БЕЗ dfs_tmpdir (используется /tmp/mlflow)
        model_uri = f"runs:/{champion_version.run_id}/model"
        print("LOADING_FROM_URI:", model_uri)

        model = load_spark_model(model_uri=model_uri)  # ✅ Без dfs_tmpdir!

        print("WRITE_TO_S3:", args.s3_model)
        model.write().overwrite().save(args.s3_model)

        print("SUCCESS:", args.s3_model)
        sys.exit(0)

    except Exception as e:
        print("ERROR:", str(e), file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
