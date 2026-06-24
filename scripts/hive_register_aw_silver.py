from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("hive_register_aw_silver") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

SILVER = "s3a://silver/adventureworks"

spark.sql("CREATE DATABASE IF NOT EXISTS silver COMMENT 'Capa Silver - Datos limpios'")

tablas = [
    "pedidos", "detalle_pedidos", "clientes",
    "territorios", "productos", "direcciones"
]

print("=" * 60)
print("  REGISTRANDO TABLAS SILVER AdventureWorks EN HIVE")
print("=" * 60)

for tabla in tablas:
    ruta = f"{SILVER}/{tabla}/"
    nombre = f"aw_{tabla}"
    try:
        df = spark.read.parquet(ruta)
        cols = ", ".join([f"`{c.name}` {c.dataType.simpleString()}" for c in df.schema])
        spark.sql(f"DROP TABLE IF EXISTS silver.{nombre}")
        spark.sql(f"""
            CREATE EXTERNAL TABLE silver.{nombre} ({cols})
            STORED AS PARQUET
            LOCATION '{ruta}'
        """)
        cnt = spark.sql(f"SELECT COUNT(*) as c FROM silver.{nombre}").collect()[0]["c"]
        print(f"  ✔ silver.{nombre}: {cnt:,} registros")
    except Exception as e:
        print(f"  ✘ silver.{nombre}: {e}")

print(f"\n{'=' * 60}")
print("  ✔ Registro Silver completado")
print(f"{'=' * 60}")

spark.stop()
