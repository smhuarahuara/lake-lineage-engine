"""
=============================================================================
EVALUADOR DE CALIDAD — Motor de Linaje
Compara el linaje generado por el motor vs el ground truth definido
manualmente a partir del análisis de los scripts.

Métricas calculadas:
  - Precisión:   edges correctos / edges generados
  - Completitud: edges correctos / edges esperados (ground truth)
  - F1-Score:    media armónica de precisión y completitud
  - Detección de transformaciones por script
  - Detección de nodos por capa

Uso: spark-submit eval_lineage_quality.py
=============================================================================
"""

import json
import os
from datetime import datetime


# ═════════════════════════════════════════════════════════════════════
#  GROUND TRUTH — Definido manualmente a partir del código fuente
# ═════════════════════════════════════════════════════════════════════

# Cada entrada: (source_table, target_table, script)
# Usa el formato hive://schema.tabla

GROUND_TRUTH_EDGES = [
    # ── INGESTA: SQL Server → Bronze ─────────────────────────────
    # (El script de ingesta usa JDBC, los edges son externos)

    # ── BRONZE → SILVER: adventureworks_silver.py ────────────────
    # pedidos: lee sales_order_header
    ("hive://bronze.aw_sales_order_header", "hive://silver.aw_pedidos", "adventureworks_silver.py"),

    # detalle_pedidos: lee sales_order_detail
    ("hive://bronze.aw_sales_order_detail", "hive://silver.aw_detalle_pedidos", "adventureworks_silver.py"),

    # clientes: join customer + person
    ("hive://bronze.aw_customer", "hive://silver.aw_clientes", "adventureworks_silver.py"),
    ("hive://bronze.aw_person", "hive://silver.aw_clientes", "adventureworks_silver.py"),

    # territorios: lee sales_territory
    ("hive://bronze.aw_sales_territory", "hive://silver.aw_territorios", "adventureworks_silver.py"),

    # productos: join product + product_subcategory + product_category
    ("hive://bronze.aw_product", "hive://silver.aw_productos", "adventureworks_silver.py"),
    ("hive://bronze.aw_product_subcategory", "hive://silver.aw_productos", "adventureworks_silver.py"),
    ("hive://bronze.aw_product_category", "hive://silver.aw_productos", "adventureworks_silver.py"),

    # direcciones: lee address
    ("hive://bronze.aw_address", "hive://silver.aw_direcciones", "adventureworks_silver.py"),

    # ── SILVER → GOLD: adventureworks_gold.py ────────────────────
    # ventas_por_territorio: join pedidos + territorios
    ("hive://silver.aw_pedidos", "hive://gold.aw_ventas_por_territorio", "adventureworks_gold.py"),
    ("hive://silver.aw_territorios", "hive://gold.aw_ventas_por_territorio", "adventureworks_gold.py"),

    # ventas_por_categoria: join detalle_pedidos + productos
    ("hive://silver.aw_detalle_pedidos", "hive://gold.aw_ventas_por_categoria", "adventureworks_gold.py"),
    ("hive://silver.aw_productos", "hive://gold.aw_ventas_por_categoria", "adventureworks_gold.py"),

    # ventas_por_periodo: lee pedidos
    ("hive://silver.aw_pedidos", "hive://gold.aw_ventas_por_periodo", "adventureworks_gold.py"),

    # top_productos: join detalle_pedidos + productos
    ("hive://silver.aw_detalle_pedidos", "hive://gold.aw_top_productos", "adventureworks_gold.py"),
    ("hive://silver.aw_productos", "hive://gold.aw_top_productos", "adventureworks_gold.py"),
]

# Transformaciones esperadas por script
GROUND_TRUTH_TRANSFORMS = {
    "adventureworks_silver.py": {"select", "withColumn", "join", "trim"},
    "adventureworks_gold.py": {"groupBy", "agg", "orderBy", "withColumn", "join", "filter"},
}

# Nodos esperados por capa
GROUND_TRUTH_NODES = {
    "bronze": {
        "aw_sales_order_header", "aw_sales_order_detail", "aw_customer",
        "aw_person", "aw_sales_territory", "aw_product",
        "aw_product_subcategory", "aw_product_category", "aw_address",
    },
    "silver": {
        "aw_pedidos", "aw_detalle_pedidos", "aw_clientes",
        "aw_territorios", "aw_productos", "aw_direcciones",
    },
    "gold": {
        "aw_ventas_por_territorio", "aw_ventas_por_categoria",
        "aw_ventas_por_periodo", "aw_top_productos",
    },
}


# ═════════════════════════════════════════════════════════════════════
#  FUNCIONES DE EVALUACIÓN
# ═════════════════════════════════════════════════════════════════════

def normalize_edge(source, target):
    """Normaliza un edge para comparación."""
    return (source.strip().rstrip("/").lower(), target.strip().rstrip("/").lower())


def evaluate_edges(generated_edges, ground_truth):
    """
    Calcula precisión, completitud y F1 de los edges generados.
    """
    # Normalizar ground truth
    gt_set = set()
    for src, tgt, script in ground_truth:
        gt_set.add(normalize_edge(src, tgt))

    # Normalizar edges generados
    gen_set = set()
    for edge in generated_edges:
        gen_set.add(normalize_edge(edge["source_id"], edge["target_id"]))

    # Calcular métricas
    true_positives = gt_set & gen_set        # Edges correctos
    false_positives = gen_set - gt_set        # Edges generados que no deberían existir
    false_negatives = gt_set - gen_set        # Edges que faltan

    precision = len(true_positives) / len(gen_set) if gen_set else 0
    recall = len(true_positives) / len(gt_set) if gt_set else 0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0

    return {
        "total_ground_truth": len(gt_set),
        "total_generated": len(gen_set),
        "true_positives": len(true_positives),
        "false_positives": len(false_positives),
        "false_negatives": len(false_negatives),
        "precision": round(precision * 100, 2),
        "recall": round(recall * 100, 2),
        "f1_score": round(f1 * 100, 2),
        "tp_edges": sorted([f"{s} → {t}" for s, t in true_positives]),
        "fp_edges": sorted([f"{s} → {t}" for s, t in false_positives]),
        "fn_edges": sorted([f"{s} → {t}" for s, t in false_negatives]),
    }


def evaluate_edges_by_script(generated_edges, ground_truth):
    """
    Calcula métricas desglosadas por script.
    """
    scripts = set(gt[2] for gt in ground_truth)
    results = {}

    for script in scripts:
        gt_script = set()
        for src, tgt, s in ground_truth:
            if s == script:
                gt_script.add(normalize_edge(src, tgt))

        gen_script = set()
        for edge in generated_edges:
            if edge.get("script") == script:
                gen_script.add(normalize_edge(edge["source_id"], edge["target_id"]))

        tp = gt_script & gen_script
        fp = gen_script - gt_script
        fn = gt_script - gen_script

        precision = len(tp) / len(gen_script) if gen_script else 0
        recall = len(tp) / len(gt_script) if gt_script else 0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0

        results[script] = {
            "ground_truth": len(gt_script),
            "generated": len(gen_script),
            "true_positives": len(tp),
            "false_positives": len(fp),
            "false_negatives": len(fn),
            "precision": round(precision * 100, 2),
            "recall": round(recall * 100, 2),
            "f1_score": round(f1 * 100, 2),
            "fp_detail": sorted([f"{s} → {t}" for s, t in fp]),
            "fn_detail": sorted([f"{s} → {t}" for s, t in fn]),
        }

    return results


def evaluate_transforms(generated_edges, ground_truth_transforms):
    """
    Evalúa la detección de transformaciones por script.
    """
    results = {}

    for script, expected_transforms in ground_truth_transforms.items():
        detected = set()
        for edge in generated_edges:
            if edge.get("script") == script and edge.get("transformations"):
                for t in edge["transformations"].split(","):
                    detected.add(t.strip())

        tp = expected_transforms & detected
        fp = detected - expected_transforms
        fn = expected_transforms - detected

        precision = len(tp) / len(detected) if detected else 0
        recall = len(tp) / len(expected_transforms) if expected_transforms else 0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0

        results[script] = {
            "expected": sorted(expected_transforms),
            "detected": sorted(detected),
            "true_positives": sorted(tp),
            "false_positives": sorted(fp),
            "false_negatives": sorted(fn),
            "precision": round(precision * 100, 2),
            "recall": round(recall * 100, 2),
            "f1_score": round(f1 * 100, 2),
        }

    return results


def evaluate_nodes(generated_nodes, ground_truth_nodes):
    """
    Evalúa la detección de nodos por capa.
    """
    results = {}

    for layer, expected_tables in ground_truth_nodes.items():
        detected = set()
        for node in generated_nodes:
            if node.get("layer") == layer:
                detected.add(node["table_name"])

        tp = expected_tables & detected
        fp = detected - expected_tables
        fn = expected_tables - detected

        precision = len(tp) / len(detected) if detected else 0
        recall = len(tp) / len(expected_tables) if expected_tables else 0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0

        results[layer] = {
            "expected": len(expected_tables),
            "detected": len(detected),
            "true_positives": len(tp),
            "false_positives": len(fp),
            "false_negatives": len(fn),
            "precision": round(precision * 100, 2),
            "recall": round(recall * 100, 2),
            "f1_score": round(f1 * 100, 2),
            "fp_tables": sorted(fp),
            "fn_tables": sorted(fn),
        }

    return results


# ═════════════════════════════════════════════════════════════════════
#  EJECUCIÓN PRINCIPAL
# ═════════════════════════════════════════════════════════════════════

def main():
    import requests

    API = os.getenv("LINEAGE_API_URL", "http://lineage-api:8085")

    print("=" * 70)
    print("  EVALUACIÓN DE CALIDAD — Motor de Linaje")
    print("=" * 70)
    print(f"  Timestamp: {datetime.now().isoformat()}")
    print(f"  API: {API}")
    print()

    # ── Obtener datos del motor ──────────────────────────────────
    try:
        nodes = requests.get(f"{API}/nodes", timeout=10).json()
        edges = requests.get(f"{API}/edges", timeout=10).json()
        print(f"  Datos obtenidos: {len(nodes)} nodos, {len(edges)} edges")
    except Exception as e:
        print(f"  ✗ Error conectando a la API: {e}")
        print(f"    Intentando leer desde archivo local...")
        try:
            with open("/tmp/lineage_ast_results.json") as f:
                data = json.load(f)
            print(f"  ✔ Datos cargados desde archivo")
            nodes = []
            edges = []
        except:
            print(f"  ✗ No se pudo cargar datos")
            return

    # ══════════════════════════════════════════════════════════════
    #  1. EVALUACIÓN DE EDGES (Precisión y Completitud del Linaje)
    # ══════════════════════════════════════════════════════════════
    print()
    print("─" * 70)
    print("  1. EVALUACIÓN DE EDGES — Precisión y Completitud del Linaje")
    print("─" * 70)

    edge_results = evaluate_edges(edges, GROUND_TRUTH_EDGES)

    print(f"""
  Ground truth (edges esperados):  {edge_results['total_ground_truth']}
  Edges generados por el motor:    {edge_results['total_generated']}
  True Positives (correctos):      {edge_results['true_positives']}
  False Positives (sobrantes):     {edge_results['false_positives']}
  False Negatives (faltantes):     {edge_results['false_negatives']}

  ┌─────────────────────────────────────────┐
  │  PRECISIÓN:    {edge_results['precision']:>6.2f}%                   │
  │  COMPLETITUD:  {edge_results['recall']:>6.2f}%                   │
  │  F1-SCORE:     {edge_results['f1_score']:>6.2f}%                   │
  └─────────────────────────────────────────┘""")

    if edge_results['fp_edges']:
        print(f"\n  ⚠ Edges FALSOS POSITIVOS (generados pero no esperados):")
        for e in edge_results['fp_edges']:
            print(f"    - {e}")

    if edge_results['fn_edges']:
        print(f"\n  ⚠ Edges FALSOS NEGATIVOS (esperados pero no generados):")
        for e in edge_results['fn_edges']:
            print(f"    - {e}")

    # ══════════════════════════════════════════════════════════════
    #  2. EVALUACIÓN POR SCRIPT
    # ══════════════════════════════════════════════════════════════
    print()
    print("─" * 70)
    print("  2. EVALUACIÓN POR SCRIPT")
    print("─" * 70)

    script_results = evaluate_edges_by_script(edges, GROUND_TRUTH_EDGES)

    for script, r in script_results.items():
        status = "✔" if r['f1_score'] == 100 else "△" if r['f1_score'] >= 80 else "✗"
        print(f"""
  {status} {script}
    GT={r['ground_truth']}  Gen={r['generated']}  TP={r['true_positives']}  FP={r['false_positives']}  FN={r['false_negatives']}
    Precisión={r['precision']:.1f}%  Completitud={r['recall']:.1f}%  F1={r['f1_score']:.1f}%""")
        if r['fp_detail']:
            for e in r['fp_detail']:
                print(f"    FP: {e}")
        if r['fn_detail']:
            for e in r['fn_detail']:
                print(f"    FN: {e}")

    # ══════════════════════════════════════════════════════════════
    #  3. EVALUACIÓN DE TRANSFORMACIONES
    # ══════════════════════════════════════════════════════════════
    print()
    print("─" * 70)
    print("  3. EVALUACIÓN DE TRANSFORMACIONES DETECTADAS")
    print("─" * 70)

    transform_results = evaluate_transforms(edges, GROUND_TRUTH_TRANSFORMS)

    for script, r in transform_results.items():
        status = "✔" if r['f1_score'] == 100 else "△" if r['f1_score'] >= 80 else "✗"
        print(f"""
  {status} {script}
    Esperadas:  {', '.join(r['expected'])}
    Detectadas: {', '.join(r['detected'])}
    TP: {', '.join(r['true_positives']) if r['true_positives'] else '—'}
    FP: {', '.join(r['false_positives']) if r['false_positives'] else '—'}
    FN: {', '.join(r['false_negatives']) if r['false_negatives'] else '—'}
    Precisión={r['precision']:.1f}%  Completitud={r['recall']:.1f}%  F1={r['f1_score']:.1f}%""")

    # ══════════════════════════════════════════════════════════════
    #  4. EVALUACIÓN DE NODOS POR CAPA
    # ══════════════════════════════════════════════════════════════
    print()
    print("─" * 70)
    print("  4. EVALUACIÓN DE NODOS POR CAPA")
    print("─" * 70)

    node_results = evaluate_nodes(nodes, GROUND_TRUTH_NODES)

    for layer, r in node_results.items():
        status = "✔" if r['f1_score'] == 100 else "△" if r['f1_score'] >= 80 else "✗"
        print(f"""
  {status} {layer.upper()}
    Esperados={r['expected']}  Detectados={r['detected']}  TP={r['true_positives']}  FP={r['false_positives']}  FN={r['false_negatives']}
    Precisión={r['precision']:.1f}%  Completitud={r['recall']:.1f}%  F1={r['f1_score']:.1f}%""")
        if r['fp_tables']:
            print(f"    FP: {', '.join(r['fp_tables'])}")
        if r['fn_tables']:
            print(f"    FN: {', '.join(r['fn_tables'])}")

    # ══════════════════════════════════════════════════════════════
    #  5. RESUMEN GENERAL
    # ══════════════════════════════════════════════════════════════
    print()
    print("═" * 70)
    print("  RESUMEN GENERAL DE CALIDAD")
    print("═" * 70)

    avg_edge_f1 = edge_results['f1_score']

    script_f1s = [r['f1_score'] for r in script_results.values()]
    avg_script_f1 = sum(script_f1s) / len(script_f1s) if script_f1s else 0

    transform_f1s = [r['f1_score'] for r in transform_results.values()]
    avg_transform_f1 = sum(transform_f1s) / len(transform_f1s) if transform_f1s else 0

    node_f1s = [r['f1_score'] for r in node_results.values()]
    avg_node_f1 = sum(node_f1s) / len(node_f1s) if node_f1s else 0

    overall = (avg_edge_f1 + avg_script_f1 + avg_transform_f1 + avg_node_f1) / 4

    print(f"""
  ┌───────────────────────────────────────────────────────┐
  │  Métrica                          │  F1-Score         │
  ├───────────────────────────────────┼───────────────────┤
  │  Edges (linaje)                   │  {avg_edge_f1:>6.2f}%           │
  │  Edges por script                 │  {avg_script_f1:>6.2f}%           │
  │  Transformaciones                 │  {avg_transform_f1:>6.2f}%           │
  │  Nodos por capa                   │  {avg_node_f1:>6.2f}%           │
  ├───────────────────────────────────┼───────────────────┤
  │  CALIDAD GENERAL                  │  {overall:>6.2f}%           │
  └───────────────────────────────────┴───────────────────┘
""")

    if overall >= 95:
        print("  ★★★ EXCELENTE — El motor de linaje cumple con alta fidelidad")
    elif overall >= 85:
        print("  ★★  MUY BUENO — El motor detecta la mayoría de las relaciones")
    elif overall >= 70:
        print("  ★   ACEPTABLE — Algunas relaciones no se detectan correctamente")
    else:
        print("  ⚠   NECESITA MEJORAS — El motor tiene deficiencias significativas")

    # ── Guardar resultados ────────────────────────────────────────
    report = {
        "timestamp": datetime.now().isoformat(),
        "edge_evaluation": edge_results,
        "script_evaluation": script_results,
        "transform_evaluation": transform_results,
        "node_evaluation": node_results,
        "summary": {
            "edge_f1": avg_edge_f1,
            "script_f1": avg_script_f1,
            "transform_f1": avg_transform_f1,
            "node_f1": avg_node_f1,
            "overall_quality": round(overall, 2),
        }
    }

    output_path = "/tmp/lineage_quality_report.json"
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print(f"\n  Reporte guardado en: {output_path}")


if __name__ == "__main__":
    main()
