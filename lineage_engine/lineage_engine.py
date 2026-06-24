"""
LINEAGE ENGINE — Motor Principal de Linaje
Consolida los 3 scanners y guarda resultados como Parquet en MinIO,
registrando tablas externas en Hive para consulta via Trino.
"""

import json
import os
import sys
import re
from datetime import datetime
from typing import List, Dict, Any

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scanner_ast import scan_directory as scan_scripts
from scanner_dag import scan_dags_directory
from scanner_hive import scan_hive

LINEAGE_BUCKET = "s3a://lineage"


def run_lineage_engine(spark, scripts_dir: str, dags_dir: str) -> Dict[str, Any]:

    print("\n" + "=" * 60)
    print("  MOTOR DE LINAJE — Iniciando escaneo completo")
    print("=" * 60)
    print(f"  Timestamp: {datetime.now().isoformat()}")

    print("\n[1/4] Escaneando scripts PySpark...")
    ast_results = scan_scripts(scripts_dir)

    print("\n[2/4] Escaneando DAGs Airflow...")
    dag_results = scan_dags_directory(dags_dir)

    print("\n[3/4] Escaneando Hive Metastore...")
    hive_result = scan_hive(spark)

    print("\n[4/4] Consolidando y guardando linaje...")
    metadata, edges = _build_lineage_graph(ast_results, dag_results, hive_result)

    _save_as_parquet(spark, metadata, edges)

    summary = {
        "timestamp": datetime.now().isoformat(),
        "scripts_analyzed": len(ast_results),
        "dags_analyzed": len(dag_results),
        "hive_tables": len(hive_result.get("tables", [])),
        "lineage_nodes": len(metadata),
        "lineage_edges": len(edges),
    }

    print("\n" + "=" * 60)
    print("  RESUMEN DEL LINAJE")
    print("=" * 60)
    for k, v in summary.items():
        print(f"  {k:<25s}: {v}")

    return summary


def _build_lineage_graph(ast_results, dag_results, hive_result):
    metadata = {}
    edges = []

    # Registrar tablas Hive como nodos
    for table in hive_result.get("tables", []):
        node_id = f"hive://{table['full_name']}"
        cols = table.get("columns", [])
        metadata[node_id] = {
            "node_id": node_id,
            "node_type": "hive_table",
            "layer": table["layer"],
            "database": table["database"],
            "table_name": table["table"],
            "full_name": table["full_name"],
            "location": table.get("location") or "",
            "format": table.get("format") or "parquet",
            "row_count": int(table.get("row_count") or 0),
            "column_count": len(cols),
            "columns_json": json.dumps([{"name": c["name"], "type": c["type"]} for c in cols]),
            "scanned_at": datetime.now().isoformat(),
        }

    # Mapear scripts a DAGs
    script_to_dag = {}
    for dag in dag_results:
        for script in dag.get("scripts", []):
            script_name = os.path.basename(script)
            script_to_dag[script_name] = {
                "dag_id": dag.get("dag_id"),
                "layer": dag.get("layer"),
            }

    # Procesar scripts AST
    for script_result in ast_results:
        if script_result.get("error"):
            continue

        script_name = script_result["script"]
        dag_info = script_to_dag.get(script_name, {})
        reads  = script_result.get("reads", [])
        writes = script_result.get("writes", [])

        flows = script_result.get("flows", [])
        for flow in flows:
            target_id = flow.get("target", "").rstrip("/")
            if not target_id:
                continue
            for source_path in flow.get("sources", []):
                source_id = source_path.rstrip("/")
                if source_id and target_id and source_id != target_id:
                    edges.append({
                        "source_id": source_id,
                        "target_id": target_id,
                        "script": script_name,
                        "dag_id": dag_info.get("dag_id") or "",
                        "layer": script_result.get("layer") or "",
                        "transformations": ",".join(flow.get("transformations", [])),
                        "scanned_at": datetime.now().isoformat(),
                    })

        for write in writes:
            path = write.get("path", "")
            if not path:
                continue
            node_id = path.rstrip("/")
            if node_id not in metadata:
                metadata[node_id] = {
                    "node_id": node_id,
                    "node_type": "dataset",
                    "layer": _detect_layer_from_path(path),
                    "database": "",
                    "table_name": os.path.basename(path),
                    "full_name": node_id,
                    "location": path,
                    "format": write.get("format") or "",
                    "row_count": 0,
                    "column_count": 0,
                    "columns_json": "[]",
                    "scanned_at": datetime.now().isoformat(),
                }

        flows = script_result.get("flows", [])
        for flow in flows:
            target_id = flow.get("target", "").rstrip("/")
            if not target_id:
                continue
            for source_path in flow.get("sources", []):
                source_id = source_path.rstrip("/")
                if source_id and target_id and source_id != target_id:
                    edges.append({
                        "source_id": source_id,
                        "target_id": target_id,
                        "script": script_name,
                        "dag_id": dag_info.get("dag_id") or "",
                        "layer": script_result.get("layer") or "",
                        "transformations": ",".join(flow.get("transformations", [])),
                        "scanned_at": datetime.now().isoformat(),
                    })
    # Construir mapa: location S3A → node_id Hive
    hive_location_map = {}
    for nid, node in metadata.items():
        if node["node_type"] == "hive_table" and node.get("location"):
            loc = node["location"].strip().rstrip("/")
            hive_location_map[loc] = nid

    s3a_to_hive = {}
    datasets_to_remove = []
    for nid, node in metadata.items():
        if node["node_type"] == "dataset":
            clean_id = nid.strip().rstrip("/")
            if clean_id in hive_location_map:
                hive_nid = hive_location_map[clean_id]
                s3a_to_hive[nid] = hive_nid
                s3a_to_hive[clean_id] = hive_nid
                datasets_to_remove.append(nid)

    for edge in edges:
        src = edge["source_id"].strip().rstrip("/")
        tgt = edge["target_id"].strip().rstrip("/")
        if src in s3a_to_hive:
            edge["source_id"] = s3a_to_hive[src]
        elif src in hive_location_map:
            edge["source_id"] = hive_location_map[src]
        if tgt in s3a_to_hive:
            edge["target_id"] = s3a_to_hive[tgt]
        elif tgt in hive_location_map:
            edge["target_id"] = hive_location_map[tgt]

    for nid in datasets_to_remove:
        del metadata[nid]

    print(f"  [DEBUG] s3a_to_hive mappings: {len(s3a_to_hive)}")
    print(f"  [DEBUG] datasets fusionados: {len(datasets_to_remove)}")

    return list(metadata.values()), edges
    

def _detect_layer_from_path(path: str) -> str:
    if "bronze" in path.lower():
        return "bronze"
    elif "silver" in path.lower() and "gold" not in path.lower():
        return "silver"
    elif "gold" in path.lower():
        return "gold"
    return "unknown"


def _register_lineage_tables_in_hive(spark):
    """Registra tablas de linaje en Hive via Spark SQL nativo."""

    spark.sql("CREATE DATABASE IF NOT EXISTS lineage COMMENT 'Capa Lineage'")

    lineage_tables = {
        "lineage_metadata": {
            "cols": "node_id STRING, node_type STRING, layer STRING, `database` STRING, "
                    "table_name STRING, full_name STRING, location STRING, `format` STRING, "
                    "row_count BIGINT, column_count BIGINT, columns_json STRING, scanned_at STRING",
            "path": f"{LINEAGE_BUCKET}/lineage_metadata/",
        },
        "lineage_graph_edges": {
            "cols": "source_id STRING, target_id STRING, script STRING, dag_id STRING, "
                    "layer STRING, transformations STRING, scanned_at STRING",
            "path": f"{LINEAGE_BUCKET}/lineage_graph_edges/",
        },
    }

    for tabla, info in lineage_tables.items():
        try:
            spark.sql(f"DROP TABLE IF EXISTS lineage.{tabla}")
            spark.sql(f"""
                CREATE EXTERNAL TABLE lineage.{tabla} ({info['cols']})
                STORED AS PARQUET
                LOCATION '{info['path']}'
            """)
            print(f"  ✔ lineage.{tabla} registrada en Hive")
        except Exception as e:
            print(f"  ✗ Error registrando {tabla}: {e}")


def _save_as_parquet(spark, metadata: List[Dict], edges: List[Dict]):
    """Guarda resultados como Parquet en MinIO y registra tablas externas."""

    from pyspark.sql.types import StructType, StructField, StringType, LongType

    print("\n  Guardando lineage_metadata en MinIO...")
    try:
        meta_rows = [(
            str(m.get("node_id", "")),
            str(m.get("node_type", "")),
            str(m.get("layer", "")),
            str(m.get("database", "")),
            str(m.get("table_name", "")),
            str(m.get("full_name", "")),
            str(m.get("location", "")),
            str(m.get("format", "")),
            int(m.get("row_count") or 0),
            int(m.get("column_count") or 0),
            str(m.get("columns_json", "[]")),
            str(m.get("scanned_at", "")),
        ) for m in metadata]

        schema_meta = StructType([
            StructField("node_id",      StringType(), True),
            StructField("node_type",    StringType(), True),
            StructField("layer",        StringType(), True),
            StructField("database",     StringType(), True),
            StructField("table_name",   StringType(), True),
            StructField("full_name",    StringType(), True),
            StructField("location",     StringType(), True),
            StructField("format",       StringType(), True),
            StructField("row_count",    LongType(),   True),
            StructField("column_count", LongType(),   True),
            StructField("columns_json", StringType(), True),
            StructField("scanned_at",   StringType(), True),
        ])

        df_meta = spark.createDataFrame(meta_rows, schema_meta)
        df_meta.write.mode("overwrite").parquet(f"{LINEAGE_BUCKET}/lineage_metadata/")
        print(f"  lineage_metadata: {len(meta_rows)} nodos → {LINEAGE_BUCKET}/lineage_metadata/")

    except Exception as e:
        print(f"  Error guardando metadata: {e}")

    # ── Guardar lineage_graph_edges ───────────────────────────────
    print("\n  Guardando lineage_graph_edges en MinIO...")
    try:
        edge_rows = [(
            str(e.get("source_id", "")),
            str(e.get("target_id", "")),
            str(e.get("script", "")),
            str(e.get("dag_id", "")),
            str(e.get("layer", "")),
            str(e.get("transformations", "")),
            str(e.get("scanned_at", "")),
        ) for e in edges]

        schema_edges = StructType([
            StructField("source_id",       StringType(), True),
            StructField("target_id",       StringType(), True),
            StructField("script",          StringType(), True),
            StructField("dag_id",          StringType(), True),
            StructField("layer",           StringType(), True),
            StructField("transformations", StringType(), True),
            StructField("scanned_at",      StringType(), True),
        ])

        df_edges = spark.createDataFrame(edge_rows, schema_edges)
        df_edges.write.mode("overwrite").parquet(f"{LINEAGE_BUCKET}/lineage_graph_edges/")
        print(f"  lineage_graph_edges: {len(edge_rows)} edges → {LINEAGE_BUCKET}/lineage_graph_edges/")

    except Exception as e:
        print(f"  Error guardando edges: {e}")

    # ── Registrar tablas en Hive via PostgreSQL ──────────────────
    print("\n  Registrando tablas de linaje en Hive...")
    _register_lineage_tables_in_hive(spark)


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .appName("lineage_engine") \
        .master("local[1]") \
        .config("spark.driver.memory", "600m") \
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

    SCRIPTS_DIR = "/opt/airflow/scripts"
    DAGS_DIR    = "/opt/airflow/dags"

    summary = run_lineage_engine(spark, SCRIPTS_DIR, DAGS_DIR)
    spark.stop()