"""
=============================================================================
SCANNER DAG — Motor de Linaje
Analiza DAGs de Airflow para extraer:
  - Tareas y sus dependencias
  - Scripts ejecutados por cada tarea
  - Orden de ejecución
=============================================================================
"""

import ast
import os
import re
import json
from datetime import datetime
from typing import List, Dict, Any


def scan_dag(dag_path: str) -> Dict[str, Any]:
    """Analiza un archivo DAG de Airflow y extrae su estructura."""

    result = {
        "dag_id": None,
        "dag_path": dag_path,
        "dag_file": os.path.basename(dag_path),
        "schedule": None,
        "tags": [],
        "tasks": [],
        "dependencies": [],
        "scripts": [],
        "layer": _detect_layer(dag_path),
        "scanned_at": datetime.now().isoformat(),
        "error": None
    }

    try:
        with open(dag_path, "r", encoding="utf-8") as f:
            source = f.read()

        tree = ast.parse(source)
        scanner = _DAGScanner(dag_path)
        scanner.visit(tree)

        result["dag_id"]      = scanner.dag_id
        result["schedule"]    = scanner.schedule
        result["tags"]        = scanner.tags
        result["tasks"]       = scanner.tasks
        result["dependencies"]= scanner.dependencies
        result["scripts"]     = _extract_scripts(source)

    except Exception as e:
        result["error"] = str(e)

    return result


class _DAGScanner(ast.NodeVisitor):
    """Visita el AST de un DAG y extrae su estructura."""

    def __init__(self, path: str):
        self.path = path
        self.dag_id = None
        self.schedule = None
        self.tags = []
        self.tasks = []
        self.dependencies = []

    def visit_Call(self, node):
        call_str = ""
        try:
            call_str = ast.unparse(node)
        except:
            pass

        # Detectar DAG(dag_id=...)
        if "DAG(" in call_str and "dag_id" in call_str:
            dag_id_match = re.search(r'dag_id=["\']([^"\']+)["\']', call_str)
            if dag_id_match:
                self.dag_id = dag_id_match.group(1)

            schedule_match = re.search(r'schedule_interval=["\']([^"\']+)["\']', call_str)
            if schedule_match:
                self.schedule = schedule_match.group(1)

            tags_match = re.search(r'tags=\[([^\]]+)\]', call_str)
            if tags_match:
                self.tags = re.findall(r'["\']([^"\']+)["\']', tags_match.group(1))

        # Detectar BashOperator
        elif "BashOperator(" in call_str:
            task_id = re.search(r'task_id=["\']([^"\']+)["\']', call_str)
            if task_id:
                self.tasks.append({
                    "task_id": task_id.group(1),
                    "operator": "BashOperator",
                    "bash_command": self._extract_bash_command(call_str)
                })

        # Detectar SparkSubmitOperator
        elif "SparkSubmitOperator(" in call_str:
            task_id = re.search(r'task_id=["\']([^"\']+)["\']', call_str)
            application = re.search(r'application=["\']([^"\']+)["\']', call_str)
            if task_id:
                self.tasks.append({
                    "task_id": task_id.group(1),
                    "operator": "SparkSubmitOperator",
                    "application": application.group(1) if application else None
                })

        self.generic_visit(node)

    def visit_BinOp(self, node):
        """Detecta dependencias: task1 >> task2."""
        try:
            if isinstance(node.op, ast.RShift):
                left = ast.unparse(node.left)
                right = ast.unparse(node.right)
                self.dependencies.append({
                    "from": left.strip(),
                    "to": right.strip()
                })
        except:
            pass
        self.generic_visit(node)

    def _extract_bash_command(self, call_str: str) -> str:
        match = re.search(r'bash_command=["\']([^"\']+)["\']', call_str)
        if match:
            return match.group(1)
        # bash_command como string multilínea
        match = re.search(r'bash_command=\(([^)]+)\)', call_str, re.DOTALL)
        if match:
            return match.group(1).strip()
        return ""


def _extract_scripts(source: str) -> List[str]:
    """Extrae rutas de scripts Python mencionados en el DAG."""
    scripts = []
    patterns = [
        r'/opt/airflow/scripts/[^\s"\']+\.py',
        r'scripts/[^\s"\']+\.py',
    ]
    for pattern in patterns:
        found = re.findall(pattern, source)
        scripts.extend(found)
    return list(set(scripts))


def _detect_layer(path: str) -> str:
    if "bronze_to_silver" in path:
        return "bronze_to_silver"
    elif "silver_to_gold" in path:
        return "silver_to_gold"
    return "unknown"


def scan_dags_directory(dags_dir: str) -> List[Dict]:
    """Escanea todos los DAGs en un directorio."""
    results = []
    for root, dirs, files in os.walk(dags_dir):
        for file in sorted(files):
            if file.endswith(".py") and file.startswith("dag_"):
                path = os.path.join(root, file)
                result = scan_dag(path)
                results.append(result)
                status = "✔" if not result["error"] else "✗"
                print(f"  {status} {result['dag_file']}: "
                      f"dag_id={result['dag_id']}, "
                      f"{len(result['tasks'])} tareas")
    return results


if __name__ == "__main__":
    import sys

    dags_dir = sys.argv[1] if len(sys.argv) > 1 else "/opt/airflow/dags"
    print("=" * 60)
    print("  SCANNER DAG — Análisis de DAGs Airflow")
    print("=" * 60)

    results = scan_dags_directory(dags_dir)

    print(f"\nTotal DAGs analizados: {len(results)}")
    print(f"Con errores: {sum(1 for r in results if r['error'])}")

    output_path = "/tmp/lineage_dag_results.json"
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\nResultados guardados en: {output_path}")