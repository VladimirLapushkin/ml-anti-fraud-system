import uuid
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.yandex.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)

YC_ZONE = Variable.get("YC_ZONE")
YC_FOLDER_ID = Variable.get("YC_FOLDER_ID")
YC_SUBNET_ID = Variable.get("YC_SUBNET_ID")
YC_SSH_PUBLIC_KEY = Variable.get("YC_SSH_PUBLIC_KEY")

S3_ENDPOINT_URL = Variable.get("S3_ENDPOINT_URL")
S3_BUCKET_NAME = Variable.get("S3_BUCKET_NAME")
S3_SRC_BUCKET = f"s3a://{S3_BUCKET_NAME}/src"
S3_VENV_ARCHIVE = f"s3a://{S3_BUCKET_NAME}/venvs/venv38.tar.gz"
S3_DP_LOGS_BUCKET = Variable.get("S3_DP_LOGS_BUCKET", default_var=S3_BUCKET_NAME + "/airflow_logs/")

S3_CLEAN_PATH = Variable.get("S3_CLEAN_PATH")
S3_CLEAN_ACCESS_KEY = Variable.get("S3_CLEAN_ACCESS_KEY")
S3_CLEAN_SECRET_KEY = Variable.get("S3_CLEAN_SECRET_KEY")

MLFLOW_TRACKING_URI = Variable.get("MLFLOW_TRACKING_URI")
MLFLOW_MODEL_NAME = Variable.get("MLFLOW_MODEL_NAME", default_var="fraud_detection_model")
MLFLOW_MODEL_ALIAS = Variable.get("MLFLOW_MODEL_ALIAS", default_var="champion")

KAFKA_BOOTSTRAP = Variable.get("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC_IN = Variable.get("KAFKA_TOPIC_IN")
KAFKA_TOPIC_OUT = Variable.get("KAFKA_TOPIC_OUT")
KAFKA_USER = Variable.get("KAFKA_USER")
KAFKA_PASSWORD = Variable.get("KAFKA_PASSWORD")
KAFKA_SECURITY_PROTOCOL = Variable.get("KAFKA_SECURITY_PROTOCOL", default_var="SASL_PLAINTEXT")
KAFKA_SASL_MECH = Variable.get("KAFKA_SASL_MECHANISM", default_var="SCRAM-SHA-256")

DP_SA_ID = Variable.get("DP_SA_ID")

default_args = {
    "owner": "Vladimir Lapushkin",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="streaming_inference_pipeline",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2025, 3, 27),
    catchup=False,
    tags=["mlops", "kafka", "streaming"],
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="dp-create",
        folder_id=YC_FOLDER_ID,
        cluster_name=f"tmp-dp-stream-{uuid.uuid4()}",
        subnet_id=YC_SUBNET_ID,
        s3_bucket=S3_DP_LOGS_BUCKET,
        service_account_id=DP_SA_ID,
        ssh_public_keys=YC_SSH_PUBLIC_KEY,
        zone=YC_ZONE,
        cluster_image_version="2.0",
        masternode_resource_preset="s3-c2-m8",
        masternode_disk_type="network-ssd",
        masternode_disk_size=50,
        datanode_resource_preset="s3-c4-m16",
        datanode_disk_type="network-ssd",
        datanode_disk_size=50,
        datanode_count=3,
        services=["YARN", "SPARK", "HDFS", "MAPREDUCE"],
        enable_ui_proxy=True,
        connection_id="yc-sa",
    )

    cluster_id = "{{ ti.xcom_pull(task_ids='dp-create', key='cluster_id') }}"

    TPS = 16000
    DURATION = 30
    EXP_TAG = f"20260303_tps{TPS}"
    
    producer_job = DataprocCreatePysparkJobOperator(
        task_id="kafka-producer",
        cluster_id=cluster_id,
        main_python_file_uri=f"{S3_SRC_BUCKET}/kafka_producer.py",
        connection_id="yc-sa",
        args=[
            "--s3-endpoint", S3_ENDPOINT_URL,
            "--access-key", S3_CLEAN_ACCESS_KEY,
            "--secret-key", S3_CLEAN_SECRET_KEY,
            "--input", f"{S3_CLEAN_PATH.rstrip('/')}/history/by_file/",
            "--last-n", "1",
            "--bootstrap", KAFKA_BOOTSTRAP,
            "--topic", KAFKA_TOPIC_IN,
            "--kafka-user", KAFKA_USER,
            "--kafka-password", KAFKA_PASSWORD,
            "--security-protocol", KAFKA_SECURITY_PROTOCOL,
            "--sasl-mechanism", KAFKA_SASL_MECH,
            "--tps", str(TPS),
            "--duration-sec", str(DURATION),
            "--exp-tag", EXP_TAG,
            
        ],
        properties={
            "spark.submit.deployMode": "cluster",
            "spark.yarn.dist.archives": f"{S3_VENV_ARCHIVE}#.venv",
            "spark.yarn.appMasterEnv.PYSPARK_PYTHON": "./.venv/bin/python",
            "spark.executorEnv.PYSPARK_PYTHON": "./.venv/bin/python",
        },
    )

    # Streaming : Kafka -> model@champion -> Kafka
    inference_job = DataprocCreatePysparkJobOperator(
        task_id="streaming-inference",
        cluster_id=cluster_id,
        main_python_file_uri=f"{S3_SRC_BUCKET}/streaming_inference.py",
        connection_id="yc-sa",
        args=[
            "--bootstrap", KAFKA_BOOTSTRAP,
            "--topic-in", KAFKA_TOPIC_IN,
            "--topic-out", KAFKA_TOPIC_OUT,
            "--kafka-user", KAFKA_USER,
            "--kafka-password", KAFKA_PASSWORD,
            "--s3-endpoint", S3_ENDPOINT_URL,
            "--access-key", S3_CLEAN_ACCESS_KEY,
            "--secret-key", S3_CLEAN_SECRET_KEY,
            "--security-protocol", KAFKA_SECURITY_PROTOCOL,
            "--sasl-mechanism", KAFKA_SASL_MECH,
            "--checkpoint", f"{S3_CLEAN_PATH.rstrip('/')}/checkpoints/streaming_inference/",
            "--mlflow-uri", MLFLOW_TRACKING_URI,
            "--model-uri", f"models:/{MLFLOW_MODEL_NAME}@{MLFLOW_MODEL_ALIAS}",
            "--starting-offsets", "earliest",
            "--run-seconds", str(DURATION),
            "--exp-tag", EXP_TAG,
            "--tps", str(TPS),
         ],
        properties={
            
           "spark.submit.deployMode": "cluster",
            "spark.yarn.dist.archives": f"{S3_VENV_ARCHIVE}#.venv",
            "spark.yarn.appMasterEnv.PYSPARK_PYTHON": "./.venv/bin/python",
            "spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON": "./.venv/bin/python",
            "spark.executorEnv.PYSPARK_PYTHON": "./.venv/bin/python",
            "spark.pyspark.python": "./.venv/bin/python",
            "spark.pyspark.driver.python": "./.venv/bin/python",
            "spark.dynamicAllocation.enabled": "false",
            "spark.executor.instances": "1",
            "spark.executor.cores": "1",
            "spark.executor.memory": "1g",
            "spark.driver.cores": "1",
            "spark.driver.memory": "1g",

            
        },
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="dp-delete",
        cluster_id=cluster_id,
        connection_id="yc-sa",
        trigger_rule=TriggerRule.ALL_DONE,
    )

#    create_cluster >> [producer_job, inference_job] >> delete_cluster

    create_cluster >> producer_job >> inference_job >> delete_cluster
