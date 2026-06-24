"""
REGISTRO HIVE — AdventureWorks Gold (Cubo de Ventas)
Registra cubos analíticos en Hive vía Spark
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("hive_register_aw_gold") \
    .master("local[1]") \
    .config("spark.driver.memory", "512m") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.sql.warehouse.dir", "/opt/airflow/spark-warehouse") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("  REGISTRANDO CUBOS GOLD AdventureWorks EN HIVE")
print("=" * 60)

spark.sql("CREATE DATABASE IF NOT EXISTS gold COMMENT 'Capa Gold - Cubos analiticos'")

GOLD = "s3a://gold/adventureworks"

tablas = [
    "ventas_por_territorio",
    "ventas_por_categoria",
    "ventas_por_periodo",
    "top_productos",
]

for tabla in tablas:
    try:
        nombre_hive = f"aw_{tabla}"
        df = spark.read.parquet(f"{GOLD}/{tabla}/")
        spark.sql(f"DROP TABLE IF EXISTS gold.{nombre_hive}")
        cols = ", ".join([f"`{f.name}` {f.dataType.simpleString()}" for f in df.schema.fields])
        spark.sql(f"""
            CREATE EXTERNAL TABLE gold.{nombre_hive} ({cols})
            STORED AS PARQUET
            LOCATION '{GOLD}/{tabla}/'
        """)
        count = spark.sql(f"SELECT COUNT(*) as n FROM gold.{nombre_hive}").collect()[0]["n"]
        print(f"  ✔ gold.{nombre_hive:<35s} {count:>8,} registros")
    except Exception as e:
        print(f"  ✗ {tabla}: {str(e)[:120]}")

print("\n✔ Cubos Gold AdventureWorks registrados en Hive")
spark.stop()
