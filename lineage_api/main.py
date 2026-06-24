"""
LINEAGE API v2.0 — FastAPI
Alineada con Lineage Explorer mockup.
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import trino
import os
import json

app = FastAPI(title="Lineage API", version="2.0.0",
    description="API REST para el Lineage Explorer del lago de datos federado")

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))

def get_conn():
    return trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT,
        user="admin", catalog="hive", schema="lineage")

def run_query(sql: str) -> List[dict]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    conn.close()
    return [dict(zip(cols, row)) for row in rows]

def run_one(sql: str) -> Optional[dict]:
    rows = run_query(sql)
    return rows[0] if rows else None

# ── Modelos ───────────────────────────────────────────────────────

class Column(BaseModel):
    name: str
    type: str
    description: Optional[str] = None

class NodeSummary(BaseModel):
    node_id: str
    node_type: str
    layer: str
    table_name: str
    full_name: Optional[str] = None
    format: Optional[str] = None
    row_count: Optional[int] = None
    column_count: Optional[int] = None
    scanned_at: Optional[str] = None

class NodeDetail(BaseModel):
    node_id: str
    node_type: str
    layer: str
    database: Optional[str] = None
    table_name: str
    full_name: Optional[str] = None
    location: Optional[str] = None
    format: Optional[str] = None
    row_count: Optional[int] = None
    column_count: Optional[int] = None
    columns: List[Column] = []
    upstream_count: int = 0
    downstream_count: int = 0
    scanned_at: Optional[str] = None

class Edge(BaseModel):
    source_id: str
    target_id: str
    script: Optional[str] = None
    dag_id: Optional[str] = None
    layer: Optional[str] = None
    transformations: Optional[str] = None

class SearchResult(BaseModel):
    node_id: str
    node_type: str
    layer: str
    table_name: str
    full_name: Optional[str] = None
    format: Optional[str] = None

# ── Health ────────────────────────────────────────────────────────

@app.get("/", tags=["Health"])
def root():
    return {"status": "ok", "service": "Lineage API", "version": "2.0.0"}

@app.get("/health", tags=["Health"])
def health():
    try:
        run_query("SELECT 1")
        return {"status": "ok", "trino": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))

# ── Stats ─────────────────────────────────────────────────────────

@app.get("/stats", tags=["Resumen"])
def get_stats():
    """Estadísticas generales del lago de datos."""
    try:
        return {
            "total_nodes": run_one("SELECT COUNT(*) as total FROM hive.lineage.lineage_metadata")["total"],
            "total_edges": run_one("SELECT COUNT(*) as total FROM hive.lineage.lineage_graph_edges")["total"],
            "nodes_by_layer": run_query("SELECT layer, COUNT(*) as total FROM hive.lineage.lineage_metadata GROUP BY layer ORDER BY layer"),
            "edges_by_layer": run_query("SELECT layer, COUNT(*) as total FROM hive.lineage.lineage_graph_edges GROUP BY layer ORDER BY layer"),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ── Búsqueda ──────────────────────────────────────────────────────

@app.get("/search", response_model=List[SearchResult], tags=["Búsqueda"])
def search(q: str = Query(..., min_length=1)):
    """Busca nodos por node_id o table_name (búsqueda parcial)."""
    term = q.lower().replace("'", "''")
    sql = f"""
        SELECT node_id, node_type, layer, table_name, full_name, format
        FROM hive.lineage.lineage_metadata
        WHERE LOWER(node_id) LIKE '%{term}%' OR LOWER(table_name) LIKE '%{term}%'
        ORDER BY layer, table_name LIMIT 20
    """
    try:
        return run_query(sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ── Nodos ─────────────────────────────────────────────────────────

@app.get("/nodes", response_model=List[NodeSummary], tags=["Nodos"])
def get_nodes(layer: Optional[str] = None):
    """Lista todos los nodos, opcionalmente filtrados por capa."""
    where = f"WHERE layer = '{layer}'" if layer else ""
    sql = f"""
        SELECT node_id, node_type, layer, table_name, full_name, format,
               CAST(row_count AS BIGINT) as row_count,
               CAST(column_count AS BIGINT) as column_count, scanned_at
        FROM hive.lineage.lineage_metadata {where}
        ORDER BY layer, table_name
    """
    try:
        return run_query(sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/nodes/{node_id:path}", response_model=NodeDetail, tags=["Nodos"])
def get_node(node_id: str):
    """Detalle completo de un nodo: metadata, schema, conteo upstream/downstream."""
    safe = node_id.replace("'", "''")
    sql = f"""
        SELECT node_id, node_type, layer, database, table_name, full_name,
               location, format,
               CAST(row_count AS BIGINT) as row_count,
               CAST(column_count AS BIGINT) as column_count,
               columns_json, scanned_at
        FROM hive.lineage.lineage_metadata WHERE node_id = '{safe}'
    """
    try:
        row = run_one(sql)
        if not row:
            raise HTTPException(status_code=404, detail=f"Nodo no encontrado: {node_id}")

        columns = []
        try:
            cols_raw = json.loads(row.get("columns_json") or "[]")
            columns = [Column(name=c["name"], type=c["type"]) for c in cols_raw]
        except Exception:
            pass

        up = run_one(f"SELECT COUNT(*) as total FROM hive.lineage.lineage_graph_edges WHERE target_id = '{safe}'")
        down = run_one(f"SELECT COUNT(*) as total FROM hive.lineage.lineage_graph_edges WHERE source_id = '{safe}'")

        return NodeDetail(
            node_id=row["node_id"], node_type=row["node_type"], layer=row["layer"],
            database=row.get("database"), table_name=row["table_name"],
            full_name=row.get("full_name"), location=row.get("location"),
            format=row.get("format"), row_count=row.get("row_count"),
            column_count=row.get("column_count"), columns=columns,
            upstream_count=up["total"] if up else 0,
            downstream_count=down["total"] if down else 0,
            scanned_at=row.get("scanned_at"),
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ── Edges ─────────────────────────────────────────────────────────

@app.get("/edges", response_model=List[Edge], tags=["Edges"])
def get_edges(layer: Optional[str] = None, dag_id: Optional[str] = None):
    """Lista todos los edges del grafo."""
    conditions = []
    if layer: conditions.append(f"layer = '{layer}'")
    if dag_id: conditions.append(f"dag_id = '{dag_id}'")
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    sql = f"SELECT source_id, target_id, script, dag_id, layer, transformations FROM hive.lineage.lineage_graph_edges {where} ORDER BY layer, dag_id"
    try:
        return run_query(sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ── Grafo completo ────────────────────────────────────────────────

@app.get("/graph", tags=["Linaje"])
def get_graph(layer: Optional[str] = None):
    """Grafo completo (nodos + edges) para renderizar en el frontend."""
    return {"nodes": get_nodes(layer), "edges": get_edges(layer)}

# ── Linaje ────────────────────────────────────────────────────────

def _fetch_node_summary(nid: str) -> Optional[dict]:
    safe = nid.replace("'", "''")
    return run_one(f"""
        SELECT node_id, node_type, layer, table_name, full_name, location,
               format, CAST(row_count AS BIGINT) as row_count,
               CAST(column_count AS BIGINT) as column_count, scanned_at
        FROM hive.lineage.lineage_metadata WHERE node_id = '{safe}'
    """)

@app.get("/lineage/upstream/{node_id:path}", tags=["Linaje"])
def get_upstream(node_id: str, depth: int = Query(default=3, ge=1, le=10)):
    """Linaje backward: ¿de dónde vienen los datos? (upstream)"""
    visited = {}
    edges_out = []

    def traverse(current: str, d: int):
        if d == 0 or current in visited:
            return
        node = _fetch_node_summary(current)
        if node:
            visited[current] = node
        safe = current.replace("'", "''")
        for edge in run_query(f"SELECT source_id, target_id, script, dag_id, layer, transformations FROM hive.lineage.lineage_graph_edges WHERE target_id = '{safe}'"):
            edges_out.append(edge)
            traverse(edge["source_id"], d - 1)

    traverse(node_id, depth)
    return {"node_id": node_id, "direction": "upstream", "depth": depth,
            "nodes": list(visited.values()), "edges": edges_out}

@app.get("/lineage/downstream/{node_id:path}", tags=["Linaje"])
def get_downstream(node_id: str, depth: int = Query(default=3, ge=1, le=10)):
    """Linaje forward: ¿a dónde van los datos? (downstream)"""
    visited = {}
    edges_out = []

    def traverse(current: str, d: int):
        if d == 0 or current in visited:
            return
        node = _fetch_node_summary(current)
        if node:
            visited[current] = node
        safe = current.replace("'", "''")
        for edge in run_query(f"SELECT source_id, target_id, script, dag_id, layer, transformations FROM hive.lineage.lineage_graph_edges WHERE source_id = '{safe}'"):
            edges_out.append(edge)
            traverse(edge["target_id"], d - 1)

    traverse(node_id, depth)
    return {"node_id": node_id, "direction": "downstream", "depth": depth,
            "nodes": list(visited.values()), "edges": edges_out}