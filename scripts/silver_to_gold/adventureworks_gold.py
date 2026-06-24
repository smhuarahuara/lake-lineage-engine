"""
SILVER → GOLD: Cubo de Ventas AdventureWorks
Genera tablas analíticas agregadas a partir de Silver
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, countDistinct,
    round as spark_round, desc, current_timestamp,
    year, month, quarter, when
)

spark = SparkSession.builder \
    .appName("adventureworks_silver_to_gold") \
    .master("local[2]") \
    .config("spark.driver.memory", "1g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

SILVER = "s3a://silver/adventureworks"
GOLD = "s3a://gold/adventureworks"

print("=" * 60)
print("  AdventureWorks: Silver → Gold (Cubo de Ventas)")
print("=" * 60)

# ── Leer tablas Silver ──────────────────────────────────────────
pedidos     = spark.read.parquet(f"{SILVER}/pedidos/")
detalle     = spark.read.parquet(f"{SILVER}/detalle_pedidos/")
clientes    = spark.read.parquet(f"{SILVER}/clientes/")
territorios = spark.read.parquet(f"{SILVER}/territorios/")
productos   = spark.read.parquet(f"{SILVER}/productos/")

# ── 1. Ventas por Territorio ────────────────────────────────────
print("\n  [1/4] Calculando ventas_por_territorio...")
ventas_territorio = pedidos \
    .join(territorios, "territorio_id", "left") \
    .groupBy("territorio_id", "territorio", "pais_codigo", "grupo") \
    .agg(
        count("pedido_id").alias("total_pedidos"),
        countDistinct("cliente_id").alias("clientes_unicos"),
        spark_sum("total").alias("ingresos_totales"),
        avg("total").alias("ticket_promedio"),
        spark_sum("flete").alias("flete_total"),
    ) \
    .withColumn("ingresos_totales", spark_round(col("ingresos_totales"), 2)) \
    .withColumn("ticket_promedio", spark_round(col("ticket_promedio"), 2)) \
    .withColumn("flete_total", spark_round(col("flete_total"), 2)) \
    .withColumn("_gold_at", current_timestamp()) \
    .orderBy(desc("ingresos_totales"))

ventas_territorio.write.mode("overwrite").parquet(f"{GOLD}/ventas_por_territorio/")
print(f"  ✔ ventas_por_territorio: {ventas_territorio.count()} registros")

# ── 2. Ventas por Categoría de Producto ─────────────────────────
print("\n  [2/4] Calculando ventas_por_categoria...")
ventas_categoria = detalle \
    .join(productos, "producto_id", "left") \
    .groupBy("categoria", "subcategoria") \
    .agg(
        count("detalle_id").alias("lineas_vendidas"),
        spark_sum("cantidad").alias("unidades_vendidas"),
        spark_sum("total_linea").alias("ingresos_totales"),
        avg("precio_unitario").alias("precio_promedio"),
        avg("descuento").alias("descuento_promedio"),
    ) \
    .withColumn("ingresos_totales", spark_round(col("ingresos_totales"), 2)) \
    .withColumn("precio_promedio", spark_round(col("precio_promedio"), 2)) \
    .withColumn("descuento_promedio", spark_round(col("descuento_promedio"), 4)) \
    .withColumn("_gold_at", current_timestamp()) \
    .orderBy(desc("ingresos_totales"))

ventas_categoria.write.mode("overwrite").parquet(f"{GOLD}/ventas_por_categoria/")
print(f"  ✔ ventas_por_categoria: {ventas_categoria.count()} registros")

# ── 3. Ventas por Periodo (Año / Trimestre / Mes) ───────────────
print("\n  [3/4] Calculando ventas_por_periodo...")
ventas_periodo = pedidos \
    .withColumn("anio", year("fecha_pedido")) \
    .withColumn("trimestre", quarter("fecha_pedido")) \
    .withColumn("mes", month("fecha_pedido")) \
    .groupBy("anio", "trimestre", "mes") \
    .agg(
        count("pedido_id").alias("total_pedidos"),
        countDistinct("cliente_id").alias("clientes_unicos"),
        spark_sum("total").alias("ingresos_totales"),
        avg("total").alias("ticket_promedio"),
        spark_sum(when(col("es_online") == True, 1).otherwise(0)).alias("pedidos_online"),
        spark_sum(when(col("es_online") == False, 1).otherwise(0)).alias("pedidos_tienda"),
    ) \
    .withColumn("ingresos_totales", spark_round(col("ingresos_totales"), 2)) \
    .withColumn("ticket_promedio", spark_round(col("ticket_promedio"), 2)) \
    .withColumn("pct_online", spark_round(
        col("pedidos_online") / (col("pedidos_online") + col("pedidos_tienda")) * 100, 2
    )) \
    .withColumn("_gold_at", current_timestamp()) \
    .orderBy("anio", "trimestre", "mes")

ventas_periodo.write.mode("overwrite").parquet(f"{GOLD}/ventas_por_periodo/")
print(f"  ✔ ventas_por_periodo: {ventas_periodo.count()} registros")

# ── 4. Top Productos ────────────────────────────────────────────
print("\n  [4/4] Calculando top_productos...")
top_productos = detalle \
    .join(productos, "producto_id", "left") \
    .groupBy("producto_id", "producto", "categoria", "subcategoria") \
    .agg(
        count("detalle_id").alias("veces_vendido"),
        spark_sum("cantidad").alias("unidades_totales"),
        spark_sum("total_linea").alias("ingresos_totales"),
        avg("precio_unitario").alias("precio_promedio"),
    ) \
    .withColumn("ingresos_totales", spark_round(col("ingresos_totales"), 2)) \
    .withColumn("precio_promedio", spark_round(col("precio_promedio"), 2)) \
    .withColumn("margen_estimado",
        spark_round(col("ingresos_totales") - (col("unidades_totales") * col("precio_promedio") * 0.6), 2)
    ) \
    .withColumn("_gold_at", current_timestamp()) \
    .orderBy(desc("ingresos_totales"))

top_productos.write.mode("overwrite").parquet(f"{GOLD}/top_productos/")
print(f"  ✔ top_productos: {top_productos.count()} registros")

print(f"\n{'=' * 60}")
print("  ✔ Silver → Gold AdventureWorks completado")
print(f"  Cubos generados:")
print(f"    • ventas_por_territorio")
print(f"    • ventas_por_categoria")
print(f"    • ventas_por_periodo")
print(f"    • top_productos")
print(f"{'=' * 60}")

spark.stop()
