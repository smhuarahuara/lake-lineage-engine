"""
BRONZE → SILVER: AdventureWorks
Lee Parquet crudo de Bronze y genera tablas limpias en Silver
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, lower, concat, lit, coalesce,
    to_timestamp, current_timestamp, when, round as spark_round
)

spark = SparkSession.builder \
    .appName("adventureworks_bronze_to_silver") \
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

BRONZE = "s3a://bronze/adventureworks"
SILVER = "s3a://silver/adventureworks"

print("=" * 60)
print("  AdventureWorks: Bronze → Silver")
print("=" * 60)

# ── 1. Pedidos (SalesOrderHeader) ────────────────────────────────
print("\n  [1/6] Limpiando pedidos...")
pedidos = spark.read.parquet(f"{BRONZE}/sales_order_header/") \
    .select(
        col("SalesOrderID").alias("pedido_id"),
        col("OrderDate").alias("fecha_pedido"),
        col("DueDate").alias("fecha_entrega"),
        col("ShipDate").alias("fecha_envio"),
        col("Status").alias("estado"),
        col("OnlineOrderFlag").alias("es_online"),
        col("CustomerID").alias("cliente_id"),
        col("SalesPersonID").alias("vendedor_id"),
        col("TerritoryID").alias("territorio_id"),
        col("SubTotal").alias("subtotal"),
        col("TaxAmt").alias("impuesto"),
        col("Freight").alias("flete"),
        col("TotalDue").alias("total"),
    ) \
    .withColumn("subtotal", spark_round(col("subtotal"), 2)) \
    .withColumn("impuesto", spark_round(col("impuesto"), 2)) \
    .withColumn("flete", spark_round(col("flete"), 2)) \
    .withColumn("total", spark_round(col("total"), 2)) \
    .withColumn("_silver_at", current_timestamp())

pedidos.write.mode("overwrite").parquet(f"{SILVER}/pedidos/")
print(f"  ✔ pedidos: {pedidos.count():,} registros")

# ── 2. Detalle de Pedidos (SalesOrderDetail) ─────────────────────
print("\n  [2/6] Limpiando detalle_pedidos...")
detalle = spark.read.parquet(f"{BRONZE}/sales_order_detail/") \
    .select(
        col("SalesOrderDetailID").alias("detalle_id"),
        col("SalesOrderID").alias("pedido_id"),
        col("ProductID").alias("producto_id"),
        col("OrderQty").alias("cantidad"),
        col("UnitPrice").alias("precio_unitario"),
        col("UnitPriceDiscount").alias("descuento"),
        col("LineTotal").alias("total_linea"),
    ) \
    .withColumn("precio_unitario", spark_round(col("precio_unitario"), 2)) \
    .withColumn("total_linea", spark_round(col("total_linea"), 2)) \
    .withColumn("_silver_at", current_timestamp())

detalle.write.mode("overwrite").parquet(f"{SILVER}/detalle_pedidos/")
print(f"  ✔ detalle_pedidos: {detalle.count():,} registros")

# ── 3. Clientes + Persona ───────────────────────────────────────
print("\n  [3/6] Limpiando clientes...")
customer = spark.read.parquet(f"{BRONZE}/customer/")
person = spark.read.parquet(f"{BRONZE}/person/")

clientes = customer \
    .join(person, customer["PersonID"] == person["BusinessEntityID"], "left") \
    .select(
        customer["CustomerID"].alias("cliente_id"),
        customer["PersonID"].alias("persona_id"),
        customer["StoreID"].alias("tienda_id"),
        customer["TerritoryID"].alias("territorio_id"),
        concat(
            coalesce(person["FirstName"], lit("")),
            lit(" "),
            coalesce(person["LastName"], lit(""))
        ).alias("nombre_completo"),
        person["EmailPromotion"].alias("acepta_email_promo"),
    ) \
    .withColumn("nombre_completo", trim(col("nombre_completo"))) \
    .withColumn("tipo_cliente",
        when(col("tienda_id").isNotNull(), "Tienda").otherwise("Individual")
    ) \
    .withColumn("_silver_at", current_timestamp())

clientes.write.mode("overwrite").parquet(f"{SILVER}/clientes/")
print(f"  ✔ clientes: {clientes.count():,} registros")

# ── 4. Territorios ──────────────────────────────────────────────
print("\n  [4/6] Limpiando territorios...")
territorios = spark.read.parquet(f"{BRONZE}/sales_territory/") \
    .select(
        col("TerritoryID").alias("territorio_id"),
        col("Name").alias("territorio"),
        col("CountryRegionCode").alias("pais_codigo"),
        col("Group").alias("grupo"),
    ) \
    .withColumn("_silver_at", current_timestamp())

territorios.write.mode("overwrite").parquet(f"{SILVER}/territorios/")
print(f"  ✔ territorios: {territorios.count():,} registros")

# ── 5. Productos + Categorías ───────────────────────────────────
print("\n  [5/6] Limpiando productos...")
product = spark.read.parquet(f"{BRONZE}/product/")
subcat = spark.read.parquet(f"{BRONZE}/product_subcategory/")
cat = spark.read.parquet(f"{BRONZE}/product_category/")

productos = product \
    .join(subcat, product["ProductSubcategoryID"] == subcat["ProductSubcategoryID"], "left") \
    .join(cat, subcat["ProductCategoryID"] == cat["ProductCategoryID"], "left") \
    .select(
        product["ProductID"].alias("producto_id"),
        product["Name"].alias("producto"),
        product["ProductNumber"].alias("codigo_producto"),
        product["Color"].alias("color"),
        product["StandardCost"].alias("costo_estandar"),
        product["ListPrice"].alias("precio_lista"),
        product["Size"].alias("tamano"),
        product["Weight"].alias("peso"),
        subcat["Name"].alias("subcategoria"),
        cat["Name"].alias("categoria"),
    ) \
    .withColumn("costo_estandar", spark_round(col("costo_estandar"), 2)) \
    .withColumn("precio_lista", spark_round(col("precio_lista"), 2)) \
    .withColumn("_silver_at", current_timestamp())

productos.write.mode("overwrite").parquet(f"{SILVER}/productos/")
print(f"  ✔ productos: {productos.count():,} registros")

# ── 6. Direcciones ──────────────────────────────────────────────
print("\n  [6/6] Limpiando direcciones...")
direcciones = spark.read.parquet(f"{BRONZE}/address/") \
    .select(
        col("AddressID").alias("direccion_id"),
        col("AddressLine1").alias("direccion"),
        col("City").alias("ciudad"),
        col("StateProvinceID").alias("provincia_id"),
        col("PostalCode").alias("codigo_postal"),
    ) \
    .withColumn("_silver_at", current_timestamp())

direcciones.write.mode("overwrite").parquet(f"{SILVER}/direcciones/")
print(f"  ✔ direcciones: {direcciones.count():,} registros")

print(f"\n{'=' * 60}")
print("  ✔ Bronze → Silver AdventureWorks completado")
print(f"{'=' * 60}")

spark.stop()
