"""
REGISTRO HIVE — AdventureWorks Bronze
Registra tablas externas apuntando a los Parquet en s3a://bronze/adventureworks/
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("hive_register_aw_bronze") \
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
print("  REGISTRANDO TABLAS BRONZE AdventureWorks EN HIVE")
print("=" * 60)

spark.sql("CREATE DATABASE IF NOT EXISTS bronze COMMENT 'Capa Bronze - Datos crudos'")

BRONZE = "s3a://bronze/adventureworks"

tablas = [
    "sales_order_header",
    "sales_order_detail",
    "customer",
    "sales_territory",
    "product",
    "product_category",
    "product_subcategory",
    "person",
    "address",
]

for tabla in tablas:
    try:
        nombre_hive = f"aw_{tabla}"
        df = spark.read.parquet(f"{BRONZE}/{tabla}/")
        spark.sql(f"DROP TABLE IF EXISTS bronze.{nombre_hive}")
        cols = ", ".join([f"`{f.name}` {f.dataType.simpleString()}" for f in df.schema.fields])
        spark.sql(f"""
            CREATE EXTERNAL TABLE bronze.{nombre_hive} ({cols})
            STORED AS PARQUET
            LOCATION '{BRONZE}/{tabla}/'
        """)
        count = spark.sql(f"SELECT COUNT(*) as n FROM bronze.{nombre_hive}").collect()[0]["n"]
        print(f"  ✔ bronze.{nombre_hive:<35s} {count:>8,} registros")
    except Exception as e:
        print(f"  ✗ {tabla}: {str(e)[:120]}")

print("\n✔ Tablas Bronze AdventureWorks registradas en Hive")
spark.stop()
