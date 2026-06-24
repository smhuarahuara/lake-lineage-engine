"""
INGESTA: SQL Server (AdventureWorks2022) → Bronze (MinIO)
Lee tablas desde SQL Server vía JDBC y las escribe como Parquet en s3a://bronze/adventureworks/
"""
from pyspark.sql import SparkSession

# ── Configuración ────────────────────────────────────────────────
JDBC_URL = "jdbc:sqlserver://host.docker.internal:1433;databaseName=AdventureWorks2022;encrypt=false;trustServerCertificate=true"
JDBC_USER = "spark_user"
JDBC_PASS = "Spark123"
JDBC_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

BRONZE = "s3a://bronze/adventureworks"

# Tablas a ingestar: (schema.tabla, nombre_destino_bronze)
TABLAS = [
    ("Sales.SalesOrderHeader",              "sales_order_header"),
    ("Sales.SalesOrderDetail",              "sales_order_detail"),
    ("Sales.Customer",                      "customer"),
    ("Sales.SalesTerritory",                "sales_territory"),
    ("Production.Product",                  "product"),
    ("Production.ProductCategory",          "product_category"),
    ("Production.ProductSubcategory",       "product_subcategory"),
    ("Person.Person",                       "person"),
    ("Person.Address",                      "address"),
]

# ── Spark Session ────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("ingesta_adventureworks_bronze") \
    .master("local[2]") \
    .config("spark.driver.memory", "1g") \
    .config("spark.jars", "/opt/airflow/jars/mssql-jdbc-12.8.1.jre11.jar") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("  INGESTA: AdventureWorks2022 → Bronze (MinIO)")
print("=" * 60)

total_registros = 0

for tabla_origen, nombre_destino in TABLAS:
    try:
        print(f"\n  Leyendo {tabla_origen}...")
        df = spark.read \
            .format("jdbc") \
            .option("url", JDBC_URL) \
            .option("dbtable", tabla_origen) \
            .option("user", JDBC_USER) \
            .option("password", JDBC_PASS) \
            .option("driver", JDBC_DRIVER) \
            .load()

        count = df.count()
        ruta = f"{BRONZE}/{nombre_destino}/"

        df.write.mode("overwrite").parquet(ruta)

        print(f"  ✔ {tabla_origen:<45s} → {nombre_destino:<25s} {count:>8,} registros")
        total_registros += count

    except Exception as e:
        print(f"  ✗ {tabla_origen}: {str(e)[:150]}")

print(f"\n{'=' * 60}")
print(f"  Total: {total_registros:,} registros ingestados en Bronze")
print(f"{'=' * 60}")

spark.stop()
