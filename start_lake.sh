#!/bin/bash
# ─────────────────────────────────────────────────────────────
#  start_lake.sh — Levanta el lago de datos federado en orden
# ─────────────────────────────────────────────────────────────

set -e

echo "=================================================="
echo "   LAGO DE DATOS FEDERADO — Iniciando entorno"
echo "=================================================="

# 1. Infraestructura base: PostgreSQL + MinIO unificado
echo ""
echo "[1/5] Levantando PostgreSQL + MinIO..."
docker compose up -d postgres minio

echo "  Esperando PostgreSQL (30s)..."
sleep 30

# 2. Crear buckets bronze / silver / gold
echo ""
echo "[2/5] Creando buckets bronze, silver y gold..."
docker compose up minio-init
# minio-init termina solo, no queda corriendo

# 3. Hive Metastore
echo ""
echo "[3/5] Levantando Hive Metastore..."
docker compose up -d hive-metastore
echo "  Esperando que Hive inicialice el esquema (40s)..."
sleep 40

# 4. Spark Master + Worker + JupyterLab
echo ""
echo "[4/5] Levantando Spark y JupyterLab..."
docker compose up -d spark-master spark-worker jupyterlab
sleep 15

# 5. Airflow + Trino
echo ""
echo "[5/5] Inicializando Airflow y levantando Trino..."
docker compose up airflow-init
sleep 10
docker compose up -d airflow-webserver airflow-scheduler trino

echo ""
echo "=================================================="
echo "   Entorno listo — Accesos:"
echo "=================================================="
echo ""
echo "  NODE-CONTROL"
echo "  Airflow UI       →  http://localhost:8080"
echo "                       usuario: admin | clave: admin123"
echo ""
echo "  NODE-PROCESSING"
echo "  Spark Master UI  →  http://localhost:8081"
echo "  JupyterLab       →  http://localhost:8888"
echo "                       token: jupyter123"
echo "  MinIO Console    →  http://localhost:9001"
echo "                       usuario: minioadmin | clave: minioadmin123"
echo ""
echo "  NODE-SERVING"
echo "  Spark Worker UI  →  http://localhost:8082"
echo "  Trino UI         →  http://localhost:8090"
echo "=================================================="
