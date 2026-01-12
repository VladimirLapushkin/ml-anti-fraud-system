"""
DAG: data_pipeline_seq 
"""

import json
import uuid
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import ShortCircuitOperator, PythonOperator
from airflow.settings import Session
from airflow.models import Connection, Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.yandex.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator
)
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.yandex.hooks.dataproc import DataprocHook

logger = logging.getLogger(__name__)

cluster_uuid = str(uuid.uuid4())[-8:]
cluster_name = f"spark-seq-{cluster_uuid}"

YC_ZONE = Variable.get("YC_ZONE")
YC_FOLDER_ID = Variable.get("YC_FOLDER_ID")
YC_SUBNET_ID = Variable.get("YC_SUBNET_ID")
YC_SSH_PUBLIC_KEY = Variable.get("YC_SSH_PUBLIC_KEY")
S3_ENDPOINT_URL = Variable.get("S3_ENDPOINT_URL")
S3_ACCESS_KEY = Variable.get("S3_ACCESS_KEY")
S3_SECRET_KEY = Variable.get("S3_SECRET_KEY")
S3_BUCKET_NAME = Variable.get("S3_BUCKET_NAME")
S3_SRC_BUCKET = S3_BUCKET_NAME
S3_DP_LOGS_BUCKET = Variable.get("S3_DP_LOGS_BUCKET", default_var=S3_BUCKET_NAME + "/airflow_logs/")
DP_SA_AUTH_KEY_PUBLIC_KEY = Variable.get("DP_SA_AUTH_KEY_PUBLIC_KEY")
DP_SA_JSON = Variable.get("DP_SA_JSON")
DP_SA_ID = Variable.get("DP_SA_ID")
S3_CLEAN_ACCESS_KEY = Variable.get("S3_CLEAN_ACCESS_KEY")
S3_CLEAN_SECRET_KEY = Variable.get("S3_CLEAN_SECRET_KEY")
S3_CLEAN_PATH = Variable.get("S3_CLEAN_PATH")

# Connections
YC_S3_CONNECTION = Connection(
    conn_id="yc-s3",
    conn_type="s3",
    host=S3_ENDPOINT_URL,
    login=S3_ACCESS_KEY,
    password=S3_SECRET_KEY,
    extra={"region_name": "us-east-1"},
)

YC_SA_CONNECTION = Connection(
    conn_id="yc-sa",
    conn_type="yandexcloud",
    extra={
        "extra__yandexcloud__public_ssh_key": DP_SA_AUTH_KEY_PUBLIC_KEY,
        "extra__yandexcloud__service_account_json": DP_SA_JSON,
    },
)

def setup_airflow_connections(*connections):
    session = Session()
    try:

        for conn in connections:
            existing = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()
            if not existing:
                session.add(conn)
                logger.info(f" {conn.conn_id} создан")

        session.commit()


        s3_hook = S3Hook('yc-s3')
        bucket_ok = s3_hook.check_for_bucket(S3_BUCKET_NAME)
        logger.info(f" S3 бакет {S3_BUCKET_NAME}: {'OK' if bucket_ok else 'FAIL'}")

    except Exception as e:
        session.rollback()
        logger.error(f" Setup: {e}")
        raise
    finally:
        session.close()

def run_setup(**kwargs):
    setup_airflow_connections(YC_S3_CONNECTION, YC_SA_CONNECTION)
    return True

def init_file_list(**kwargs):
    """Инициализация списка файлов"""
    try:
        file_list = Variable.get("source_file_list", deserialize_json=True)
        logger.info(f"Готово: {len(file_list)} файлов")
        return True
    except:
        logger.info("Сканируем публичный бакет.")
        import requests
        from xml.etree import ElementTree as ET

        url = "https://storage.yandexcloud.net/otus-mlops-source-data/?list-type=2"
        resp = requests.get(url, timeout=30)
        if resp.status_code != 200:
            raise ValueError(f" HTTP {resp.status_code}")

        root = ET.fromstring(resp.content)
        ns = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
        files = sorted([
            el.find('s3:Key', ns).text
            for el in root.findall('.//s3:Contents', ns)
            if el.find('s3:Key', ns) is not None and el.find('s3:Key', ns).text.endswith('.txt')
        ])

        if not files:
            raise ValueError(" Нет  файлов!")

        Variable.set("source_file_list", files, serialize_json=True)
        Variable.set("current_file_index", "0")
        logger.info(f" {len(files)} файлов: {files[:3]}...")
        return True

def check_next_file(**kwargs):
    ti = kwargs['ti']
    try:
        file_list = Variable.get("source_file_list", deserialize_json=True)
        idx = int(Variable.get("current_file_index", default_var="0"))
    except:
        raise ValueError(" Список не готов")

    if idx >= len(file_list):
        logger.info(f" Всего {len(file_list)} обработано!")
        return False

    file_name = file_list[idx]

    # XCom
    ti.xcom_push(key='current_file_path', value=file_name)
    ti.xcom_push(key='current_file', value=file_name)
    ti.xcom_push(key='file_key', value=file_name)
    ti.xcom_push(key='total_files', value=len(file_list))

    logger.info(f"[{idx+1}/{len(file_list)}] {file_name}")
    return True

def generate_args(**kwargs):
    ti = kwargs['ti']
    file_path = ti.xcom_pull(task_ids='check_next_file', key='current_file_path')
    clean_key = file_path.replace('/', '_').replace('.txt', '')

    ti.xcom_push(key='current_file_path', value=file_path)
    ti.xcom_push(key='clean_file_name', value=clean_key)

    logger.info(f" File: {file_path} -> {clean_key}")
    return file_path

def increment_index(**kwargs):
    idx = int(Variable.get("current_file_index", default_var="0")) + 1
    Variable.set("current_file_index", str(idx))
    ti = kwargs['ti']
    total = ti.xcom_pull(task_ids='check_next_file', key='total_files') or 0
    logger.info(f"Индекс: {idx}/{total}")
    return True

def run_pyspark_job(**kwargs):
    ti = kwargs['ti']

    cluster_id = ti.xcom_pull(task_ids='create_cluster', key='return_value')
    logger.info(f"Cluster: {cluster_id}")

    file_path = ti.xcom_pull(task_ids='check_next_file', key='current_file_path')
    clean_key = ti.xcom_pull(task_ids='generate_args', key='clean_file_name')

    args = [
        "--source-bucket", "otus-mlops-source-data",
        "--source-key", file_path,
        "--clean-access-key", S3_CLEAN_ACCESS_KEY,
        "--clean-secret-key", S3_CLEAN_SECRET_KEY,
        "--clean-path", f"s3a://mlops-2025-dz3/clean-data/{clean_key}/"
    ]

    logger.info(f"Args: {' '.join(args)}")

    hook = DataprocHook(yandex_conn_id='yc-sa')
    client = hook.dataproc_client

    job_name = f"clean-data-{kwargs['ds_nodash']}-{clean_key}"

    job_operation = client.create_pyspark_job(
        cluster_id=cluster_id,
        main_python_file_uri=f"s3a://{S3_SRC_BUCKET}/src/pyspark_script.py",
        args=args,
        name=job_name
    )

    logger.info(f"PySpark job '{job_name}' завершен")
    return job_name


with DAG(
    dag_id="data_pipeline_seq",
    start_date=datetime(2025, 6, 10),
    schedule_interval=timedelta(minutes=60),
    catchup=False,
    max_active_runs=1,
    default_args={'retries': 2, 'retry_delay': timedelta(minutes=5)},
) as dag:

    setup = PythonOperator(task_id='setup', python_callable=run_setup)
    init_files = PythonOperator(task_id='init_files', python_callable=init_file_list)
    check_file = ShortCircuitOperator(task_id='check_next_file', python_callable=check_next_file)
    generate_args_task = PythonOperator(task_id='generate_args', python_callable=generate_args)

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        folder_id=YC_FOLDER_ID,
        cluster_name=cluster_name,
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
        datanode_disk_size=70,
        datanode_count=3,
        properties={
            "yarn:yarn.nodemanager.resource.memory-mb": "32000",
            "yarn:yarn.scheduler.capacity.root.queues.default.user-limit-factor": "1.0",
            "yarn:yarn.scheduler.capacity.maximum-am-resource-percent": "0.8",
            "spark:spark.driver.memory": "512m",
            "spark:spark.executor.memory": "2g",
            "spark:spark.executor.instances": "1",
            "spark:spark.dynamicAllocation.enabled": "true",
            "spark:spark.dynamicAllocation.minExecutors": "1",
        },
        computenode_count=0,
        services=["YARN", "SPARK", "HDFS", "MAPREDUCE"],
        connection_id="yc-sa",
    )

    pyspark_job = PythonOperator(task_id='pyspark_job', python_callable=run_pyspark_job)

    delete_cluster = DataprocDeleteClusterOperator(
    task_id="delete_cluster",
    cluster_id="{{ ti.xcom_pull(task_ids='create_cluster', key='return_value') }}",
    connection_id="yc-sa",
    trigger_rule=TriggerRule.ALL_DONE,
)


    increment = PythonOperator(
        task_id='increment_index',
        python_callable=increment_index,
        trigger_rule=TriggerRule.ALL_DONE,
    )


    setup >> init_files >> check_file
    check_file >> generate_args_task >> create_cluster >> pyspark_job >> delete_cluster
    [pyspark_job, delete_cluster] >> increment
