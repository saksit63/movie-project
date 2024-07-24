#gcloud compute scp ~/Airflow/dags/movies.py instance-20240719-155639:~/Airflow/ --project=ornate-chemist-425808-e2 --zone=us-central1-c
import time
from airflow import DAG
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.mysql_to_gcs import MySQLToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteTableOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator, 
    DataprocCreateClusterOperator, 
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)
import os


#gcs
GCP_BUCKET_NAME = 'project-etl-saksit'
PYSPARK_PATH = "pyspark/pyspark_script.py"

#cluster
PROJECT_ID = "ornate-chemist-425808-e2"
CLUSTER_NAME = "cluster-pyspark-airflow"
REGION = "us-central1"
ZONE_CLUSTER = "us-central1-a"
BUCKET_NAME_CLUSTER = "dataproc-cluster-saksit"

#warehours
PROJECT_ID = "ornate-chemist-425808-e2"
DATASET_NAME = "movies_project"

cluster_generator_config = ClusterGenerator(
    project_id=PROJECT_ID,
    auto_zone=True,
    cluster_name=CLUSTER_NAME,
    master_machine_type='n2-standard-2',
    master_disk_type='pd-standard',
    master_disk_size_gb=30,
    worker_disk_size_gb=0,
    num_masters=1,
    num_workers=0,
    num_gpus=0,  # กำหนดจำนวน GPU เป็น 0
    num_local_ssd=0,  # กำหนดจำนวน Local SSD เป็น 
    image_version='2.0-debian10',
    storage_bucket=BUCKET_NAME_CLUSTER
    #init_actions_uris = กำหนดไฟล์ที่ต้องการติดตั้งแพคเพิ่มเติม
).make()

job = {
        "reference" : {"project_id" : PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {"main_python_file_uri": f"gs://{BUCKET_NAME_CLUSTER}/{PYSPARK_PATH}"}
    }

default_args = {
    'owner': 'Saksit',
    'depends_on_past': False,
    'catchup': False,
    'start_date': days_ago(1),
    'email': ['saksit_ch@kkumail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'execution_timeout': timedelta(minutes=30),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='movies_pipeline',
    default_args=default_args, 
    schedule_interval=None, 
    tags=['movies', 'project']
) as dag:

    create_bucket = GCSCreateBucketOperator(
        task_id = 'create_bucket',
        bucket_name=GCP_BUCKET_NAME,
        location="US",
        gcp_conn_id="gcp"
    )

    upload_movie_review_to_gcs = LocalFilesystemToGCSOperator(
        task_id = 'upload_movie_review_to_gcs',
        src="/home/airflow/dataset/movie_review.csv", #พาธที่อ้างอิงไปยังไฟล์ที่ต้องการย้ายจาก Local filesystem
        dst='raw/movie_review.csv', #ชื่อของ Object ใน Google Cloud Storage ที่ต้องการเก็บไฟล์
        bucket= GCP_BUCKET_NAME, #คือชื่อ Bucket ที่ต้องการให้ไฟล์ถูกบันทึกใน GCS
        gcp_conn_id="gcp", #ชื่อของ Connection ID
        mime_type="text/csv" #ระบุประเภทไฟล์
    )

    user_purchase_mysql_to_gcs = MySQLToGCSOperator(
        task_id = "user_purchase_mysql_to_gcs",
        sql="SELECT * FROM movies.user_purchase LIMIT 200000;",
        bucket=GCP_BUCKET_NAME,
        filename='raw/user_purchase/user_purchase.csv',
        schema_filename="None",
        export_format="CSV",
        mysql_conn_id="mysql",
        gcp_conn_id="gcp",
    )  

    #task_group_dataproc
    task_group_dataproc = TaskGroup('run_spark_using_dataproc')
    
    create_dataproc_cluster = DataprocCreateClusterOperator(
        task_id = 'create_dataproc_cluster',
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        gcp_conn_id='gcp_cluster',
        delete_on_error=True, #ลบถ้า error
        use_if_exists=True, 
            #True มีคลัสเตอร์ที่มีชื่อเดียวกันอยู่แล้ว ระบบจะใช้คลัสเตอร์นั้นแทนที่จะสร้างใหม่
            #False ระบบจะพยายามสร้างคลัสเตอร์ใหม่เสมอ ถ้ามีคลัสเตอร์ที่มีชื่อเดียวกันอยู่แล้ว การสร้างคลัสเตอร์จะล้มเหลว
        cluster_config=cluster_generator_config,
        task_group = task_group_dataproc
    )

    pyspark_clean_to_gcs = DataprocSubmitJobOperator(
        task_id="pyspark_clean_to_gcs",
        project_id=PROJECT_ID,
        gcp_conn_id='gcp_cluster',
        region=REGION,
        job=job,
        task_group = task_group_dataproc
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id = 'delete_cluster',
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        task_group = task_group_dataproc
    )

    create_movies_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id = 'create_retail_dataset',
        dataset_id = DATASET_NAME,
        exists_ok=True,
        gcp_conn_id="gcp"
    )

    TABLE_NAME = "movie_review"
    movie_review_from_gcs_to_bigquery = GCSToBigQueryOperator(
        task_id = 'movie_review_from_gcs_to_bigquery',
        bucket=GCP_BUCKET_NAME,
        source_objects=['clean/movie_review.parquet'],
        destination_project_dataset_table=f'{DATASET_NAME}.{TABLE_NAME}',
        # schema_fields=[
        #     {"name":"customer_id", 'type': 'STRING'},
        #     {"name":"positive_review", 'type': 'BOOLEAN'},
        #     {"name":"insert_date", 'type': 'DATETIME'}
        # ],
        source_format="PARQUET",
        gcp_conn_id="gcp",
        skip_leading_rows=0,
        write_disposition="WRITE_TRUNCATE"
    )

    TABLE_NAME = "user_purchase"
    user_purchase_from_gcs_to_bigquery = GCSToBigQueryOperator(
        task_id = 'user_purchase_from_gcs_to_bigquery',
        bucket=GCP_BUCKET_NAME,
        source_objects=['raw/user_purchase/user_purchase.csv'],
        destination_project_dataset_table=f'{DATASET_NAME}.{TABLE_NAME}',
        # schema_fields=[
        #     {"name":"invoice_id", 'type': 'STRING'},
        #     {"name":"stock_id", 'type': 'STRING'},
        #     {"name":"description", 'type': 'STRING'},
        #     {"name":"quantity", 'type': 'INTEGER'},
        #     {"name":"invoice_date", 'type': 'DATETIME'},
        #     {"name":"unit_price", 'type': 'FLOAT'},
        #     {"name":"customer_id", 'type': 'INTEGER'},
        #     {"name":"country", 'type': 'STRING'}
        # ],
        source_format="CSV",
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id="gcp",
        skip_leading_rows=1, #ใช้กับ csv ที่มี header
    )

    q = f"""
        WITH user_purchase AS (
            SELECT * FROM {PROJECT_ID}.{DATASET_NAME}.user_purchase
        ),
        movie_review AS (
            SELECT * FROM {PROJECT_ID}.{DATASET_NAME}.movie_review
        )

        CREATE OR REPLACE VIEW {PROJECT_ID}.{DATASET_NAME}.customer_reviews AS
            SELECT
                up.CustomerID,
                sum(up.Quantity * up.UnitPrice) as amount_spent,
                sum(case
                    when mr.positive_review then 1 else 0
                    end) as num_positive_reviews,
                count(mr.cid) as num_reviews
            FROM user_purchase up
            JOIN movie_review mr ON up.CustomerID = mr.cid
            GROUP BY up.CustomerID
            ORDER BY amount_spent
    """

    run_job_bigquery = BigQueryInsertJobOperator(
        task_id='run_job_bigquery',
        configuration={
            "query": {
                "query": q,
                "useLegacySql": False,
                "writeDisposition":"WRITE_TRUNCATE"
            }
        },
        gcp_conn_id="gcp"
    )


    create_bucket >> upload_movie_review_to_gcs >> user_purchase_mysql_to_gcs  >>  create_dataproc_cluster >> pyspark_clean_to_gcs >> delete_cluster
    delete_cluster >> create_movies_dataset >> [movie_review_from_gcs_to_bigquery, user_purchase_from_gcs_to_bigquery] >> run_job_bigquery