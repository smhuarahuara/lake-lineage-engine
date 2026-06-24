"""
DAG: dag_lineage_engine
Ejecuta el motor de linaje para actualizar los metadatos de trazabilidad.
Corre diariamente o puede ejecutarse manualmente.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "datalake",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

SPARK_SUBMIT = """
spark-submit \
  --master local[1] \
  --driver-memory 600m \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
  --conf spark.sql.warehouse.dir=/opt/airflow/spark-warehouse \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin123 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --jars /opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar \
  /opt/airflow/lineage_engine/lineage_engine.py
"""

with DAG(
    dag_id="dag_lineage_engine",
    description="Motor de linaje — escanea scripts, DAGs y Hive para construir el grafo de trazabilidad",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 2 * * *",  # Todos los días a las 2 AM
    catchup=False,
    tags=["lineage", "governance"],
) as dag:

    ejecutar_lineage_engine = BashOperator(
        task_id="ejecutar_lineage_engine",
        bash_command=SPARK_SUBMIT,
    )