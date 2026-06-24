"""
DAG: aw_silver_to_gold
Pipeline:
  1. silver_to_gold   → Genera cubos analíticos agregados
  2. registrar_hive   → Registra cubos Gold en Hive Metastore
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    "owner": "datalake",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}

JARS = (
    "/opt/airflow/jars/hadoop-aws-3.3.4.jar,"
    "/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar"
)

SPARK_ENV = {
    "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-amd64",
    "PATH": "/usr/lib/jvm/java-17-openjdk-amd64/bin:/home/airflow/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
    "PYSPARK_PYTHON": "/usr/local/bin/python",
    "SPARK_LOCAL_IP": "127.0.0.1",
}

SPARK_CMD = (
    "spark-submit "
    "--master local[2] "
    "--driver-memory 1g "
    "--conf spark.sql.shuffle.partitions=4 "
    "--conf spark.sql.legacy.timeParserPolicy=LEGACY "
    "--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
    "--conf spark.hadoop.fs.s3a.access.key=minioadmin "
    "--conf spark.hadoop.fs.s3a.secret.key=minioadmin123 "
    "--conf spark.hadoop.fs.s3a.path.style.access=true "
    "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
    "--conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider "
    "--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false "
    f"--jars {JARS} "
)

SPARK_HIVE = (
    "spark-submit "
    "--master local[1] "
    "--driver-memory 512m "
    "--conf spark.sql.catalogImplementation=hive "
    "--conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 "
    "--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
    "--conf spark.hadoop.fs.s3a.access.key=minioadmin "
    "--conf spark.hadoop.fs.s3a.secret.key=minioadmin123 "
    "--conf spark.hadoop.fs.s3a.path.style.access=true "
    "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
    "--conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider "
    "--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false "
    f"--jars {JARS} "
)

with DAG(
    dag_id="aw_silver_to_gold",
    default_args=default_args,
    description="AdventureWorks: Silver → Gold (Cubo Ventas) → Hive",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["adventureworks", "silver", "gold", "cubo"],
) as dag:

    generar_cubos = BashOperator(
        task_id="silver_to_gold",
        bash_command=SPARK_CMD + "/opt/airflow/scripts/silver_to_gold/adventureworks_gold.py",
        env=SPARK_ENV,
    )

    registrar_gold = BashOperator(
        task_id="registrar_hive_gold",
        bash_command=SPARK_HIVE + "/opt/airflow/scripts/hive_register_aw_gold.py",
        env=SPARK_ENV,
    )

    generar_cubos >> registrar_gold
