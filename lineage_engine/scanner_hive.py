"""
=============================================================================
SCANNER HIVE — Motor de Linaje
Consulta Hive Metastore via Spark SQL para extraer:
  - Bases de datos registradas
  - Tablas y sus schemas
  - Ubicaciones de los datos
=============================================================================
"""

import json
import os
from datetime import datetime
from typing import List, Dict, Any


def scan_hive(spark) -> Dict[str, Any]:
    """
    Consulta Hive Metastore y retorna metadata de todas las tablas.
    Requiere una SparkSession con Hive Support habilitado.
    """
    result = {
        "databases": [],
        "tables": [],
        "scanned_at": datetime.now().isoformat(),
        "error": None
    }

    try:
        # Obtener bases de datos
        databases = [row[0] for row in spark.sql("SHOW DATABASES").collect()]
        result["databases"] = databases
        print(f"  Bases de datos encontradas: {databases}")

        for db in databases:
            if db in ("default", "information_schema"):
                continue

            try:
                tables = spark.sql(f"SHOW TABLES IN {db}").collect()
                for table_row in tables:
                    table_name = table_row[1] if len(table_row) > 1 else table_row[0]

                    table_info = {
                        "database": db,
                        "table": table_name,
                        "full_name": f"{db}.{table_name}",
                        "columns": [],
                        "location": None,
                        "format": None,
                        "row_count": None,
                        "layer": db,
                    }

                    try:
                        # Obtener schema
                        schema = spark.sql(f"DESCRIBE {db}.{table_name}").collect()
                        table_info["columns"] = [
                            {"name": row[0], "type": row[1]}
                            for row in schema
                            if row[0] and not row[0].startswith("#")
                        ]

                        # Obtener detalles de la tabla
                        detail = spark.sql(f"DESCRIBE EXTENDED {db}.{table_name}").collect()
                        for row in detail:
                            if row[0] == "Location":
                                table_info["location"] = row[1]
                            elif row[0] == "InputFormat":
                                if "Parquet" in str(row[1]):
                                    table_info["format"] = "parquet"
                                elif "Text" in str(row[1]):
                                    table_info["format"] = "csv"

                        # Contar registros
                        count = spark.sql(f"SELECT COUNT(*) as n FROM {db}.{table_name}").collect()[0]["n"]
                        table_info["row_count"] = count

                    except Exception as e:
                        table_info["error"] = str(e)[:100]

                    result["tables"].append(table_info)
                    print(f"  ✔ {db}.{table_name}: {table_info.get('row_count', '?')} registros")

            except Exception as e:
                print(f"  ✗ Error en DB {db}: {e}")

    except Exception as e:
        result["error"] = str(e)

    return result


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .appName("hive_scanner") \
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
    print("  SCANNER HIVE — Extracción de metadata")
    print("=" * 60)

    result = scan_hive(spark)

    print(f"\nTotal tablas: {len(result['tables'])}")

    output_path = "/tmp/lineage_hive_results.json"
    with open(output_path, "w") as f:
        json.dump(result, f, indent=2, ensure_ascii=False, default=str)
    print(f"Resultados guardados en: {output_path}")

    spark.stop()