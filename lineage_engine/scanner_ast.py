"""
=============================================================================
SCANNER AST v2 — Motor de Linaje
Analiza scripts PySpark usando AST + rastreo de flujo de datos.
Detecta: lecturas, escrituras, transformaciones y FLUJOS (qué read → write).
Soporta: parquet, csv, json, orc, jdbc, delta, iceberg, saveAsTable, insertInto,
          format genérico, f-strings, variables concatenadas.
=============================================================================
"""

import ast
import os
import re
import json
from datetime import datetime
from typing import List, Dict, Any, Optional, Set


class SymbolicEvaluator:
    """Evalúa expresiones Python para resolver rutas dinámicamente."""

    def __init__(self, variables: Dict[str, str]):
        self.variables = variables

    def evaluate(self, node) -> Optional[str]:
        try:
            if isinstance(node, ast.Constant):
                return str(node.value) if isinstance(node.value, str) else None
            elif isinstance(node, ast.Name):
                return self.variables.get(node.id)
            elif isinstance(node, ast.JoinedStr):
                return self._eval_fstring(node)
            elif isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
                left = self.evaluate(node.left)
                right = self.evaluate(node.right)
                if left is not None and right is not None:
                    return left + right
            elif isinstance(node, ast.Subscript):
                val = self.evaluate(node.value)
                if val:
                    return val
        except Exception:
            pass
        return None

    def _eval_fstring(self, node: ast.JoinedStr) -> Optional[str]:
        parts = []
        for value in node.values:
            if isinstance(value, ast.Constant):
                parts.append(str(value.value))
            elif isinstance(value, ast.FormattedValue):
                inner = self.evaluate(value.value)
                if inner is None:
                    try:
                        name = ast.unparse(value.value)
                        inner = self.variables.get(name)
                    except Exception:
                        pass
                if inner is None:
                    return None
                parts.append(inner)
            else:
                return None
        result = "".join(parts)
        return result if result else None


class VariableExtractor(ast.NodeVisitor):
    """Primera pasada: extrae asignaciones de variables con rutas."""

    def __init__(self):
        self.variables: Dict[str, str] = {}

    def visit_Assign(self, node: ast.Assign):
        if len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            var_name = node.targets[0].id
            evaluator = SymbolicEvaluator(self.variables)
            value = evaluator.evaluate(node.value)
            if value and isinstance(value, str):
                if any(p in value for p in ["s3a://", "hdfs://", "file://", "/", "jdbc:", "hive://"]):
                    self.variables[var_name] = value
                elif re.match(r'^[a-zA-Z_][a-zA-Z0-9_.]*$', value):
                    self.variables[var_name] = value
        self.generic_visit(node)


def _is_valid_path(path: str) -> bool:
    return any(p in path for p in [
        "s3a://", "hdfs://", "hive://", "file://",
        "gs://", "abfss://", "wasbs://", "dbfs://",
        "jdbc:", "delta://",
    ])


class DataFlowTracker(ast.NodeVisitor):
    """
    Segunda pasada: rastrea el flujo de DataFrames.
    Asocia cada variable con sus sources (paths leídos) y detecta
    a dónde se escriben, generando flujos precisos source→target.
    """

    TRANSFORM_OPS = {
        "groupBy", "filter", "where", "join", "withColumn", "withColumnRenamed",
        "select", "selectExpr", "agg", "orderBy", "sort", "sortWithinPartitions",
        "dropDuplicates", "distinct", "pivot", "unpivot",
        "union", "unionAll", "unionByName", "intersect", "intersectAll",
        "subtract", "exceptAll", "crossJoin",
        "explode", "explode_outer", "posexplode",
        "limit", "sample", "repartition", "coalesce",
        "fillna", "dropna", "na", "replace",
        "withWatermark", "alias", "toDF",
        "cache", "persist", "unpersist", "checkpoint",
        "hint", "broadcast",
    }

    def __init__(self, variables: Dict[str, str]):
        self.variables = variables
        self.evaluator = SymbolicEvaluator(variables)
        self.df_sources: Dict[str, Set[str]] = {}
        self.reads: List[Dict] = []
        self.writes: List[Dict] = []
        self.flows: List[Dict] = []
        self.transformations: List[str] = []
        self.seen_reads: Set[str] = set()
        self.seen_writes: Set[str] = set()

    def visit_Assign(self, node: ast.Assign):
        if len(node.targets) != 1 or not isinstance(node.targets[0], ast.Name):
            self.generic_visit(node)
            return

        var_name = node.targets[0].id

        try:
            call_str = ast.unparse(node.value)
        except Exception:
            self.generic_visit(node)
            return

        read_info = self._extract_read(node.value, call_str)
        if read_info:
            path = read_info["path"]
            if path not in self.seen_reads:
                self.seen_reads.add(path)
                self.reads.append(read_info)
            self.df_sources[var_name] = {path}
            self._extract_transforms(node.value)
            self.generic_visit(node)
            return

        sources = self._collect_df_sources(node.value)
        if sources:
            self.df_sources[var_name] = sources

        self._extract_transforms(node.value)
        self.generic_visit(node)

    def visit_Expr(self, node: ast.Expr):
        if isinstance(node.value, ast.Call):
            try:
                call_str = ast.unparse(node.value)
            except Exception:
                self.generic_visit(node)
                return

            write_info = self._extract_write(node.value, call_str)
            if write_info:
                path = write_info["path"]
                if path not in self.seen_writes:
                    self.seen_writes.add(path)
                    self.writes.append(write_info)

                sources = self._trace_write_sources(node.value)
                self.flows.append({
                    "sources": list(sources),
                    "target": path,
                    "script": "",
                    "transformations": list(self.transformations),
                    "line": node.lineno,
                })

            self._extract_transforms(node.value)

        self.generic_visit(node)

    def _extract_read(self, node, call_str: str) -> Optional[Dict]:
        if re.search(r'(?:spark\w*|sc)\.table\s*\(', call_str):
            return self._extract_table_read(node, call_str)

        if re.search(r'(?:spark\w*|sc)\.sql\s*\(', call_str):
            return self._extract_sql_read(node, call_str)

        if not re.search(r'(?:spark\w*|sc)\.read', call_str):
            return None

        path, fmt = self._find_read_path_and_format(node, call_str)
        if path:
            return {
                "path": path.rstrip("/"),
                "format": fmt,
                "line": getattr(node, 'lineno', 0),
                "operation": "read",
            }
        return None

    def _extract_table_read(self, node, call_str: str) -> Optional[Dict]:
        m = re.search(r'\.table\s*\(\s*["\']([^"\']+)["\']\s*\)', call_str)
        if m:
            return {
                "path": f"hive://{m.group(1)}",
                "format": "table",
                "line": getattr(node, 'lineno', 0),
                "operation": "read",
            }
        return None

    def _extract_sql_read(self, node, call_str: str) -> Optional[Dict]:
        m = re.search(r'\.sql\s*\(\s*["\'](.+?)["\']\s*\)', call_str, re.DOTALL)
        if m:
            sql = m.group(1)
            tables = re.findall(r'\bFROM\s+(\w+\.\w+)|\bJOIN\s+(\w+\.\w+)', sql, re.IGNORECASE)
            paths = []
            for groups in tables:
                for t in groups:
                    if t:
                        paths.append(f"hive://{t}")
            if paths:
                return {
                    "path": paths[0],
                    "format": "sql",
                    "line": getattr(node, 'lineno', 0),
                    "operation": "read",
                    "extra_tables": paths[1:] if len(paths) > 1 else [],
                }
        return None

    def _find_read_path_and_format(self, node, call_str: str) -> tuple:
        if "jdbc" in call_str.lower():
            return self._extract_jdbc_source(call_str)

        fmt_match = re.search(r'\.format\s*\(\s*["\'](\w+)["\']\s*\)', call_str)
        if fmt_match:
            fmt = fmt_match.group(1)
            path = self._extract_path_from_call(node, "load")
            if path:
                return (path, fmt)

        for fmt in ["parquet", "csv", "json", "orc", "text"]:
            if f".{fmt}(" in call_str:
                path = self._extract_path_from_call(node, fmt)
                if path:
                    return (path, fmt)

        path = self._extract_path_from_call(node, "load")
        if path:
            return (path, "unknown")

        m = re.search(r'\.table\s*\(\s*["\']([^"\']+)["\']\s*\)', call_str)
        if m:
            return (f"hive://{m.group(1)}", "table")

        return (None, "unknown")

    def _extract_jdbc_source(self, call_str: str) -> tuple:
        m = re.search(r'(?:dbtable|table)["\'],\s*["\']([^"\']+)["\']', call_str)
        if m:
            table = m.group(1)
            url_match = re.search(r'jdbc:(\w+)://([^/;]+)[/;].*?(?:databaseName=|/)(\w+)', call_str)
            if url_match:
                db_type = url_match.group(1)
                db_name = url_match.group(3)
                return (f"jdbc://{db_type}/{db_name}/{table}", "jdbc")
            return (f"jdbc://{table}", "jdbc")
        return (None, "jdbc")

    def _extract_write(self, node, call_str: str) -> Optional[Dict]:
        if ".write" not in call_str and ".writeStream" not in call_str:
            return None

        m = re.search(r'\.saveAsTable\s*\(\s*["\']([^"\']+)["\']\s*\)', call_str)
        if m:
            return {"path": f"hive://{m.group(1)}", "format": "saveAsTable",
                    "line": getattr(node, 'lineno', 0), "operation": "write"}

        m = re.search(r'\.insertInto\s*\(\s*["\']([^"\']+)["\']\s*\)', call_str)
        if m:
            return {"path": f"hive://{m.group(1)}", "format": "insertInto",
                    "line": getattr(node, 'lineno', 0), "operation": "write"}

        if "jdbc" in call_str.lower():
            jdbc_path = self._extract_jdbc_dest(call_str)
            if jdbc_path:
                return {"path": jdbc_path, "format": "jdbc",
                        "line": getattr(node, 'lineno', 0), "operation": "write"}

        fmt_match = re.search(r'\.format\s*\(\s*["\'](\w+)["\']\s*\)', call_str)
        if fmt_match:
            fmt = fmt_match.group(1)
            path = self._extract_path_from_call(node, "save")
            if path:
                return {"path": path.rstrip("/"), "format": fmt,
                        "line": getattr(node, 'lineno', 0), "operation": "write"}

        for fmt in ["parquet", "csv", "json", "orc", "text"]:
            if f".{fmt}(" in call_str:
                path = self._extract_path_from_call(node, fmt)
                if path:
                    return {"path": path.rstrip("/"), "format": fmt,
                            "line": getattr(node, 'lineno', 0), "operation": "write"}

        path = self._extract_path_from_call(node, "save")
        if path:
            return {"path": path.rstrip("/"), "format": "unknown",
                    "line": getattr(node, 'lineno', 0), "operation": "write"}

        return None

    def _extract_jdbc_dest(self, call_str: str) -> Optional[str]:
        m = re.search(r'(?:dbtable|table)["\'],\s*["\']([^"\']+)["\']', call_str)
        if m:
            table = m.group(1)
            url_match = re.search(r'jdbc:(\w+)://([^/;]+)[/;].*?(?:databaseName=|/)(\w+)', call_str)
            if url_match:
                return f"jdbc://{url_match.group(1)}/{url_match.group(3)}/{table}"
            return f"jdbc://{table}"
        return None

    def _extract_path_from_call(self, node, method: str) -> Optional[str]:
        for n in ast.walk(node):
            if not isinstance(n, ast.Call):
                continue
            try:
                if isinstance(n.func, ast.Attribute) and n.func.attr == method:
                    for arg in n.args:
                        val = self.evaluator.evaluate(arg)
                        if val and _is_valid_path(val):
                            return val
                        if method in ("saveAsTable", "insertInto") and val:
                            return f"hive://{val}"
            except Exception:
                pass
        return None

    def _collect_df_sources(self, node) -> Set[str]:
        sources = set()
        for n in ast.walk(node):
            if isinstance(n, ast.Name) and n.id in self.df_sources:
                sources.update(self.df_sources[n.id])
        return sources

    def _trace_write_sources(self, node) -> Set[str]:
        sources = set()
        root_name = self._find_chain_root(node)
        if root_name and root_name in self.df_sources:
            sources.update(self.df_sources[root_name])
        if not sources:
            for n in ast.walk(node):
                if isinstance(n, ast.Name) and n.id in self.df_sources:
                    sources.update(self.df_sources[n.id])
        return sources

    def _find_chain_root(self, node) -> Optional[str]:
        current = node
        while True:
            if isinstance(current, ast.Call):
                current = current.func
            elif isinstance(current, ast.Attribute):
                current = current.value
            elif isinstance(current, ast.Name):
                return current.id
            else:
                return None

    def _extract_transforms(self, node):
        for n in ast.walk(node):
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute):
                op = n.func.attr
                if op in self.TRANSFORM_OPS and op not in self.transformations:
                    self.transformations.append(op)


# ── Función principal ─────────────────────────────────────────────

def scan_script(script_path: str) -> Dict[str, Any]:
    script_name = os.path.basename(script_path)
    result = {
        "script": script_name,
        "script_path": script_path,
        "layer": _detect_layer(script_path),
        "reads": [],
        "writes": [],
        "flows": [],
        "transformations": [],
        "variables": {},
        "scanned_at": datetime.now().isoformat(),
        "error": None
    }

    try:
        source = open(script_path, "r", encoding="utf-8").read()
        tree = ast.parse(source)

        var_extractor = VariableExtractor()
        var_extractor.visit(tree)
        variables = var_extractor.variables
        result["variables"] = variables

        tracker = DataFlowTracker(variables)
        tracker.visit(tree)

        result["reads"] = tracker.reads
        result["writes"] = tracker.writes
        result["transformations"] = tracker.transformations

        for flow in tracker.flows:
            flow["script"] = script_name
        result["flows"] = tracker.flows

        if not result["flows"] and result["reads"] and result["writes"]:
            for write in result["writes"]:
                result["flows"].append({
                    "sources": [r["path"] for r in result["reads"]],
                    "target": write["path"],
                    "script": script_name,
                    "transformations": result["transformations"],
                    "line": write.get("line", 0),
                })

    except SyntaxError as e:
        result["error"] = f"SyntaxError: {e}"
    except Exception as e:
        result["error"] = str(e)

    return result


def _detect_layer(path: str) -> str:
    if "bronze_to_silver" in path:
        return "bronze_to_silver"
    elif "silver_to_gold" in path:
        return "silver_to_gold"
    elif "ingesta" in path:
        return "ingesta"
    elif "hive_register" in path:
        return "catalog"
    return "unknown"


def scan_directory(scripts_dir: str) -> List[Dict]:
    results = []
    for root, dirs, files in os.walk(scripts_dir):
        dirs[:] = [d for d in dirs if d not in ("__pycache__", "lineage_engine")]
        for file in sorted(files):
            if file.endswith(".py") and not file.startswith("__"):
                path = os.path.join(root, file)
                result = scan_script(path)
                results.append(result)
                status = "✔" if not result["error"] else "✗"
                n_flows = len(result.get("flows", []))
                print(f"  {status} {result['script']:<42s} "
                      f"reads={len(result['reads']):<3} "
                      f"writes={len(result['writes']):<3} "
                      f"flows={n_flows:<3} "
                      f"transforms={len(result['transformations'])}")
    return results


if __name__ == "__main__":
    import sys
    scripts_dir = sys.argv[1] if len(sys.argv) > 1 else "/opt/airflow/scripts"

    print("=" * 70)
    print("  SCANNER AST v2 — Análisis con rastreo de flujo de datos")
    print("=" * 70)

    results = scan_directory(scripts_dir)

    total_reads  = sum(len(r["reads"]) for r in results)
    total_writes = sum(len(r["writes"]) for r in results)
    total_flows  = sum(len(r.get("flows", [])) for r in results)
    errors       = sum(1 for r in results if r["error"])

    print(f"\n{'─' * 70}")
    print(f"  Scripts analizados : {len(results)}")
    print(f"  Total lecturas     : {total_reads}")
    print(f"  Total escrituras   : {total_writes}")
    print(f"  Total flujos       : {total_flows}")
    print(f"  Con errores        : {errors}")

    with open("/tmp/lineage_ast_results.json", "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"  Resultados en      : /tmp/lineage_ast_results.json")