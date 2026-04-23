# api/intelligence.py
"""
Motor de inteligencia agregada del sistema FNI.

Responsabilidades:
  - Computar risk_score global por empresa (señales + discrepancias + tendencia)
  - Generar resumen ejecutivo narrativo (3–5 líneas) listo para decisor
  - Mantener histórico de scores en MongoDB
  - Exponer ranking de empresas ordenado por riesgo real

Endpoints que consume:
  GET  /intelligence/ranking           → lista de empresas ordenada por riesgo
  GET  /intelligence/{company_name}    → score + narrativa + histórico de una empresa
"""

from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import config
import db.supabase_client as sb
import db.mongo_client as mg


# ── Modelo de salida ───────────────────────────────────────────────────────────

@dataclass
class CompanyIntelligence:
    company_name:       str
    ticker:             Optional[str]
    sector:             Optional[str]
    risk_score:         float       # 0–1, score global de riesgo
    signal_score:       float       # componente señales narrativas
    discrepancy_score:  float       # componente discrepancias financieras
    trend:              str         # "worsening" | "stable" | "improving"
    trend_delta:        float       # cambio de risk_score vs período anterior
    pending_count:      int         # señales + discrepancias sin validar
    critical_count:     int         # señales marcadas críticas
    pending_sigs:       int         # señales pendientes
    high_discs:         int         # discrepancias de alta severidad sin validar
    executive_summary:  str         # narrativa 3–5 líneas
    last_updated:       str         # ISO timestamp


# ── Score computation ──────────────────────────────────────────────────────────

def compute_risk_score(signals: list[dict], discrepancies: list[dict]) -> dict:
    """
    Computa score de riesgo 0–1 para una empresa a partir de sus señales y discrepancias.

    Ponderación:
      60% → signal_score  (promedio de scores de señales sin validar + bonus por críticas)
      40% → discrepancy_score (acumulado por severidad de discrepancias sin validar)

    Returns dict con risk_score, signal_score, discrepancy_score.
    """
    pending_sigs   = [s for s in signals if not s.get("validated")]
    critical_sigs  = [s for s in signals if s.get("decision") == "critical"]

    signal_score = 0.0
    if pending_sigs:
        signal_score = sum(s["score"] for s in pending_sigs) / len(pending_sigs)
    if critical_sigs:
        signal_score = min(1.0, signal_score + len(critical_sigs) * 0.1)

    SEV_WEIGHTS = {"high": 0.30, "medium": 0.15, "low": 0.05}
    disc_score = 0.0
    for d in discrepancies:
        if not d.get("validated"):
            disc_score += SEV_WEIGHTS.get(d.get("severity", "low"), 0.05)
    disc_score = min(1.0, disc_score)

    risk_score = round(signal_score * 0.6 + disc_score * 0.4, 3)

    return {
        "risk_score":         risk_score,
        "signal_score":       round(signal_score, 3),
        "discrepancy_score":  round(disc_score, 3),
    }


# ── Narrative generation ───────────────────────────────────────────────────────

TYPE_LABELS = {
    "defensive_language": "lenguaje defensivo",
    "smoothing":          "suavización de resultados",
    "omission":           "omisiones notables",
    "guidance_change":    "cambio de guidance",
    "hedge_excess":       "exceso de hedging",
    "inconsistency":      "inconsistencias narrativas",
}


def generate_narrative(
    company:        str,
    signals:        list[dict],
    discrepancies:  list[dict],
    risk_score:     float,
) -> str:
    """
    Genera resumen ejecutivo de 3–5 líneas orientado a decisión.
    En producción usa Groq. En modo mock construye desde templates.
    """
    if config.USE_MOCK:
        return _mock_narrative(company, signals, discrepancies, risk_score)
    return _groq_narrative(company, signals, discrepancies, risk_score)


def _mock_narrative(company, signals, discrepancies, risk_score):
    pending_sigs = [s for s in signals if not s.get("validated")]
    critical_sigs = [s for s in signals if s.get("decision") == "critical"]
    high_discs = [d for d in discrepancies if d.get("severity") == "high" and not d.get("validated")]

    label = "elevado" if risk_score > 0.7 else "moderado" if risk_score > 0.4 else "bajo"
    parts = [f"{company} presenta un perfil de riesgo {label} (score {risk_score:.2f})."]

    if critical_sigs:
        n = len(critical_sigs)
        parts.append(
            f"⚠ {n} señal{'es' if n > 1 else ''} marcada{'s' if n > 1 else ''} como crítica{'s' if n > 1 else ''} "
            f"— requiere{'n' if n > 1 else ''} acción inmediata."
        )

    if pending_sigs:
        types = list(dict.fromkeys(
            TYPE_LABELS.get(s["signal_type"], s["signal_type"])
            for s in pending_sigs[:3]
        ))
        parts.append(
            f"{len(pending_sigs)} señal{'es' if len(pending_sigs) > 1 else ''} narrativa{'s' if len(pending_sigs) > 1 else ''} "
            f"pendiente{'s' if len(pending_sigs) > 1 else ''} de revisión: {', '.join(types)}."
        )

    if high_discs:
        d = high_discs[0]
        dev_str = f"{d.get('deviation_pct', 0):+.1f}%"
        parts.append(
            f"Discrepancia de alto riesgo en {d.get('metric_name','métricas clave')}: "
            f"{dev_str} respecto al período anterior."
        )

    if not pending_sigs and not high_discs and not critical_sigs:
        parts.append("Sin alertas activas. Situación bajo monitoreo de rutina.")
    elif risk_score > 0.7:
        parts.append("→ Revisar señales críticas antes de tomar decisiones sobre esta empresa.")
    elif risk_score > 0.4:
        parts.append("→ Validar señales pendientes en los próximos días.")
    else:
        parts.append("→ Monitoreo de rutina recomendado.")

    return " ".join(parts)


def _groq_narrative(company, signals, discrepancies, risk_score):
    """Genera narrativa usando Groq. Fallback a mock si falla."""
    try:
        import requests

        ctx = {
            "company":           company,
            "risk_score":        risk_score,
            "pending_signals":   len([s for s in signals if not s.get("validated")]),
            "critical_signals":  len([s for s in signals if s.get("decision") == "critical"]),
            "high_discrepancies":len([d for d in discrepancies if d.get("severity") == "high"]),
            "top_signal_types":  list(dict.fromkeys(s["signal_type"] for s in signals[:5])),
            "top_metric":        discrepancies[0].get("metric_name") if discrepancies else None,
        }

        prompt = (
            f"Sos analista financiero senior. Generá un resumen ejecutivo sobre {company}.\n\n"
            f"Datos del sistema: {ctx}\n\n"
            "Reglas:\n"
            "- Máximo 5 líneas de prosa directa, sin bullets\n"
            "- Primera línea: nivel de riesgo y score\n"
            "- Mencioná señales críticas si existen\n"
            "- Última línea: recomendación de acción concreta\n"
            "- Español rioplatense, tiempo presente, sin rodeos\n"
            "- Escribís para alguien que toma decisiones, no para un reporte técnico"
        )

        headers = {
            "Authorization": f"Bearer {config.GROQ_API_KEY}",
            "Content-Type":  "application/json",
        }
        body = {
            "model":       config.GROQ_MODEL,
            "messages":    [{"role": "user", "content": prompt}],
            "max_tokens":  350,
            "temperature": 0.25,
        }
        resp = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers=headers, json=body, timeout=10,
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"].strip()

    except Exception:
        return _mock_narrative(company, signals, discrepancies, risk_score)


# ── Main intelligence computation ─────────────────────────────────────────────

def get_company_intelligence(company_name: str, company_meta: Optional[dict] = None) -> CompanyIntelligence:
    """
    Computa el perfil de inteligencia completo de una empresa:
    score, narrativa, tendencia y guarda histórico.
    """
    db = sb.get_client()

    signals = (
        db.table("signals")
        .select("*")
        .eq("company_name", company_name)
        .order("score", desc=True)
        .limit(50)
        .execute()
        .data
    )
    discrepancies = (
        db.table("discrepancies")
        .select("*")
        .eq("company_name", company_name)
        .order("severity")
        .limit(50)
        .execute()
        .data
    )

    scores = compute_risk_score(signals, discrepancies)
    narrative = generate_narrative(company_name, signals, discrepancies, scores["risk_score"])

    pending_sigs = len([s for s in signals if not s.get("validated")])
    high_discs   = len([d for d in discrepancies if d.get("severity") == "high" and not d.get("validated")])
    critical     = len([s for s in signals if s.get("decision") == "critical"])
    pending      = pending_sigs + len([d for d in discrepancies if not d.get("validated")])

    # Compute trend from MongoDB history
    trend = "stable"
    trend_delta = 0.0

    if not config.USE_MOCK:
        try:
            mongo_db = mg.get_db()
            history = list(mongo_db["intelligence_history"].find(
                {"company_name": company_name},
                sort=[("timestamp", -1)],
                limit=2,
            ))
            if len(history) >= 2:
                prev_score  = history[1].get("risk_score", scores["risk_score"])
                trend_delta = round(scores["risk_score"] - prev_score, 3)
                if trend_delta > 0.05:
                    trend = "worsening"
                elif trend_delta < -0.05:
                    trend = "improving"

            # Persist current snapshot
            mongo_db["intelligence_history"].insert_one({
                "company_name":     company_name,
                "risk_score":       scores["risk_score"],
                "signal_score":     scores["signal_score"],
                "discrepancy_score":scores["discrepancy_score"],
                "pending_count":    pending,
                "critical_count":   critical,
                "timestamp":        datetime.utcnow(),
            })
        except Exception:
            pass

    meta = company_meta or {}
    return CompanyIntelligence(
        company_name      = company_name,
        ticker            = meta.get("ticker"),
        sector            = meta.get("sector"),
        risk_score        = scores["risk_score"],
        signal_score      = scores["signal_score"],
        discrepancy_score = scores["discrepancy_score"],
        trend             = trend,
        trend_delta       = trend_delta,
        pending_count     = pending,
        critical_count    = critical,
        pending_sigs      = pending_sigs,
        high_discs        = high_discs,
        executive_summary = narrative,
        last_updated      = datetime.utcnow().isoformat(),
    )


def get_ranking() -> list[dict]:
    """
    Devuelve todas las empresas ordenadas por risk_score descendente.
    Calcula inteligencia de cada empresa y persiste snapshot.
    """
    db = sb.get_client()
    companies = db.table("companies").select("*").execute().data

    ranking = []
    for company in companies:
        intel = get_company_intelligence(company["name"], company_meta=company)
        ranking.append({
            "company_name":       intel.company_name,
            "ticker":             intel.ticker,
            "sector":             intel.sector,
            "risk_score":         intel.risk_score,
            "signal_score":       intel.signal_score,
            "discrepancy_score":  intel.discrepancy_score,
            "trend":              intel.trend,
            "trend_delta":        intel.trend_delta,
            "pending_count":      intel.pending_count,
            "critical_count":     intel.critical_count,
            "pending_sigs":       intel.pending_sigs,
            "high_discs":         intel.high_discs,
            "executive_summary":  intel.executive_summary,
            "last_updated":       intel.last_updated,
        })

    return sorted(ranking, key=lambda x: x["risk_score"], reverse=True)