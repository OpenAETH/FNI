"""
api/comparator.py
Motor de comparación intertrimestral.
Consulta Supabase, compara métricas Q vs Q-1 y genera discrepancias.
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
import re

from db import supabase_client as sb


# ── Output types ──────────────────────────────────────────────────────────────

@dataclass
class Discrepancy:
    company_name:    str
    metric_name:     str
    period_current:  str
    period_previous: str
    value_current:   float
    value_previous:  float
    deviation_pct:   float    # ((current - previous) / |previous|) * 100
    severity:        str      # "low" | "medium" | "high"
    direction:       str      # "up" | "down"


# ── Thresholds ────────────────────────────────────────────────────────────────

# (metric_pattern, low_threshold, high_threshold)
# Si la desviación absoluta supera high → "high", supera low → "medium", else "low"
SEVERITY_RULES: list[tuple[str, float, float]] = [
    (r"eps|earnings per share",    5.0,  15.0),
    (r"revenue|sales|turnover",    3.0,  10.0),
    (r"margin|margen",             2.0,   7.0),
    (r"guidance",                  5.0,  15.0),
    (r"ebitda|ebit|operating",     5.0,  12.0),
    (r"cash|liquidity",            8.0,  20.0),
    (r".*",                        5.0,  15.0),   # fallback
]


def _severity(metric_name: str, deviation_pct: float) -> str:
    abs_dev = abs(deviation_pct)
    for pattern, low_th, high_th in SEVERITY_RULES:
        if re.search(pattern, metric_name, re.IGNORECASE):
            if abs_dev >= high_th:
                return "high"
            elif abs_dev >= low_th:
                return "medium"
            else:
                return "low"
    return "low"


# ── Period sorting ────────────────────────────────────────────────────────────

_QUARTER_ORDER = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}

def _period_sort_key(period: str) -> tuple[int, int]:
    """Convierte 'Q3 2024' en (2024, 3) para ordenamiento correcto."""
    m = re.search(r"(Q[1-4])\s*(\d{4})", period, re.IGNORECASE)
    if m:
        return (int(m.group(2)), _QUARTER_ORDER.get(m.group(1).upper(), 0))
    year = re.search(r"(\d{4})", period)
    return (int(year.group(1)) if year else 0, 0)


# ── Main comparator ───────────────────────────────────────────────────────────

def compare_quarters(company_name: str) -> list[Discrepancy]:
    """
    Compara las dos últimas períodos disponibles para una empresa.
    Retorna lista de Discrepancy ordenada por severidad descendente.
    """
    all_metrics = sb.get_metrics(company_name)
    if not all_metrics:
        return []

    # Agrupar por nombre de métrica → { "Revenue": [{"period": ..., "value_numeric": ...}, ...] }
    by_metric: dict[str, list[dict]] = {}
    for row in all_metrics:
        if row.get("value_numeric") is None:
            continue
        name = row["name"]
        by_metric.setdefault(name, []).append(row)

    discrepancies: list[Discrepancy] = []

    for metric_name, rows in by_metric.items():
        if len(rows) < 2:
            continue

        # Ordenar por período
        sorted_rows = sorted(rows, key=lambda r: _period_sort_key(r["period"]))

        # Tomar los dos más recientes
        prev = sorted_rows[-2]
        curr = sorted_rows[-1]

        val_prev = float(prev["value_numeric"])
        val_curr = float(curr["value_numeric"])

        if val_prev == 0:
            continue

        deviation = ((val_curr - val_prev) / abs(val_prev)) * 100

        discrepancies.append(Discrepancy(
            company_name    = company_name,
            metric_name     = metric_name,
            period_current  = curr["period"],
            period_previous = prev["period"],
            value_current   = val_curr,
            value_previous  = val_prev,
            deviation_pct   = round(deviation, 2),
            severity        = _severity(metric_name, deviation),
            direction       = "up" if deviation >= 0 else "down",
        ))

    # Ordenar: high → medium → low, luego por |desviación| desc
    severity_order = {"high": 0, "medium": 1, "low": 2}
    discrepancies.sort(key=lambda d: (severity_order[d.severity], -abs(d.deviation_pct)))

    return discrepancies


def save_discrepancies(discrepancies: list[Discrepancy]) -> int:
    """Persiste las discrepancias en Supabase. Retorna cantidad guardada."""
    if not discrepancies:
        return 0
    db = sb.get_client()
    rows = [
        {
            "company_name":    d.company_name,
            "metric_name":     d.metric_name,
            "period_current":  d.period_current,
            "period_previous": d.period_previous,
            "value_current":   d.value_current,
            "value_previous":  d.value_previous,
            "deviation_pct":   d.deviation_pct,
            "severity":        d.severity,
        }
        for d in discrepancies
    ]
    db.table("discrepancies").insert(rows).execute()
    return len(rows)
