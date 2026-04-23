# db/supabase_client.py
import os
import uuid
from datetime import datetime, timedelta
from typing import Optional
import config

_client = None
_mock_data = None


# ── Mock data generation ───────────────────────────────────────────────────────

def _generate_mock_data():
    """
    Genera datos mock realistas y diferenciados por empresa.

    Cada empresa tiene:
      - perfil de riesgo propio (high / medium / low)
      - métricas con valores y tendencias distintas
      - señales narrativas con frases y justificaciones específicas
      - discrepancias con severidades calibradas al perfil
      - estado de validación variable (sin validar / aprobadas / críticas)
    """

    now = datetime.now()

    # ── 9 empresas con perfiles distintos ────────────────────────────────────
    COMPANY_PROFILES = [
        {
            "name": "Luminar Pharma Inc.",
            "ticker": "LMNR",
            "sector": "Healthcare",
            "risk_profile": "high",          # señales críticas, discrepancias severas
            "base_revenue": 4_800,            # M USD
            "revenue_growth": -0.08,          # caída real
            "base_eps": 0.42,
            "eps_growth": -0.31,
            "margins_trend": "compressing",
            "quarters": ["Q2 2024", "Q3 2024", "Q4 2024"],
            "doc_type": "transcript",
        },
        {
            "name": "Vertex Energy Corp.",
            "ticker": "VXEN",
            "sector": "Energy",
            "risk_profile": "high",
            "base_revenue": 12_300,
            "revenue_growth": -0.14,
            "base_eps": 1.87,
            "eps_growth": -0.44,
            "margins_trend": "compressing",
            "quarters": ["Q1 2024", "Q2 2024", "Q3 2024", "Q4 2024"],
            "doc_type": "transcript",
        },
        {
            "name": "Meridian Financial Group",
            "ticker": "MFGR",
            "sector": "Financials",
            "risk_profile": "medium_high",
            "base_revenue": 8_100,
            "revenue_growth": 0.03,
            "base_eps": 3.12,
            "eps_growth": -0.09,
            "margins_trend": "stable",
            "quarters": ["Q2 2024", "Q3 2024", "Q4 2024"],
            "doc_type": "10-K",
        },
        {
            "name": "Stratum Semiconductors",
            "ticker": "STSC",
            "sector": "Technology",
            "risk_profile": "medium",
            "base_revenue": 6_700,
            "revenue_growth": 0.11,
            "base_eps": 2.05,
            "eps_growth": 0.07,
            "margins_trend": "stable",
            "quarters": ["Q3 2024", "Q4 2024"],
            "doc_type": "transcript",
        },
        {
            "name": "ClearPath Logistics",
            "ticker": "CPLG",
            "sector": "Industrials",
            "risk_profile": "medium",
            "base_revenue": 3_200,
            "revenue_growth": 0.05,
            "base_eps": 1.44,
            "eps_growth": -0.12,
            "margins_trend": "compressing",
            "quarters": ["Q2 2024", "Q3 2024", "Q4 2024"],
            "doc_type": "transcript",
        },
        {
            "name": "Apple Inc.",
            "ticker": "AAPL",
            "sector": "Technology",
            "risk_profile": "low",
            "base_revenue": 89_500,
            "revenue_growth": 0.06,
            "base_eps": 1.52,
            "eps_growth": 0.09,
            "margins_trend": "expanding",
            "quarters": ["Q2 2024", "Q3 2024", "Q4 2024"],
            "doc_type": "transcript",
        },
        {
            "name": "Microsoft Corp.",
            "ticker": "MSFT",
            "sector": "Technology",
            "risk_profile": "low",
            "base_revenue": 62_000,
            "revenue_growth": 0.15,
            "base_eps": 2.94,
            "eps_growth": 0.18,
            "margins_trend": "expanding",
            "quarters": ["Q2 2024", "Q3 2024", "Q4 2024"],
            "doc_type": "transcript",
        },
        {
            "name": "Orbis Retail Holdings",
            "ticker": "ORBH",
            "sector": "Consumer",
            "risk_profile": "medium_low",
            "base_revenue": 18_900,
            "revenue_growth": 0.02,
            "base_eps": 0.88,
            "eps_growth": 0.04,
            "margins_trend": "stable",
            "quarters": ["Q3 2024", "Q4 2024"],
            "doc_type": "transcript",
        },
        {
            "name": "Tesla Inc.",
            "ticker": "TSLA",
            "sector": "Automotive",
            "risk_profile": "medium_high",
            "base_revenue": 25_000,
            "revenue_growth": -0.06,
            "base_eps": 0.71,
            "eps_growth": -0.29,
            "margins_trend": "compressing",
            "quarters": ["Q2 2024", "Q3 2024", "Q4 2024"],
            "doc_type": "transcript",
        },
    ]

    # ── Señales por perfil ─────────────────────────────────────────────────────
    # Cada empresa tiene pool propio de señales verosímiles para su sector/situación.
    SIGNAL_POOLS = {
        "Luminar Pharma Inc.": [
            {
                "type": "defensive_language",
                "score": 0.91,
                "text": "We don't provide specific guidance on trial outcomes, but we feel the data will ultimately support our thesis.",
                "justification": "Evade preguntas directas sobre resultados clínicos Phase III usando lenguaje defensivo; la métrica de guidance de aprobación FDA quedó sin responder.",
            },
            {
                "type": "smoothing",
                "score": 0.87,
                "text": "While R&D spend elevated this quarter, we view this as an investment in our long-term pipeline strength.",
                "justification": "Reencuadra el aumento de gastos operativos como inversión estratégica sin reconocer la presión sobre márgenes reportada en la nota al pie.",
            },
            {
                "type": "omission",
                "score": 0.83,
                "text": "[Breakout por indicación terapéutica ausente en prepared remarks por primera vez en 6 trimestres]",
                "justification": "El breakdown por área terapéutica se reportó regularmente hasta Q1 2024; su ausencia coincide con caída de 34% en segmento oncología.",
            },
            {
                "type": "guidance_change",
                "score": 0.78,
                "text": "We are recalibrating our full-year revenue outlook to a range that better reflects the current reimbursement environment.",
                "justification": "Reducción de guidance de $5.4B a $4.6B encuadrada como 'recalibración'; la palabra 'reducción' no aparece en ninguna parte del call.",
            },
            {
                "type": "inconsistency",
                "score": 0.74,
                "text": "Gross margins came in at 58%, which we believe reflects our premium positioning in the market.",
                "justification": "Margen bruto cayó 11pp vs Q3 2024; el CFO había proyectado expansión de márgenes en el call anterior.",
            },
            {
                "type": "hedge_excess",
                "score": 0.66,
                "text": "Subject to regulatory review timelines and market dynamics, we may potentially explore partnership opportunities in certain geographies.",
                "justification": "Uso acumulado de 14 calificadores modales en 3 oraciones consecutivas sobre expansión internacional.",
            },
        ],
        "Vertex Energy Corp.": [
            {
                "type": "guidance_change",
                "score": 0.93,
                "text": "Given the evolving commodity price environment, we are withdrawing our full-year EBITDA guidance at this time.",
                "justification": "Retiro completo de guidance de EBITDA anual sin reemplazo; ocurre 6 semanas después de reconfirmación en investor day.",
            },
            {
                "type": "defensive_language",
                "score": 0.88,
                "text": "The refinery margin compression is largely a macro phenomenon that is not unique to Vertex.",
                "justification": "Atribuye la caída de 44% en márgenes de refinación exclusivamente a factores macro, sin reconocer la pérdida de cuota frente a competidores regionales.",
            },
            {
                "type": "omission",
                "score": 0.85,
                "text": "[Capacidad de refinación utilizada no mencionada; dato clave en los últimos 8 trimestres]",
                "justification": "Utilización de capacidad cayó a 61% vs 84% en Q4 2023; la omisión coincide con inicio de negociaciones de refinanciación de deuda.",
            },
            {
                "type": "smoothing",
                "score": 0.79,
                "text": "Our strategic pivot toward renewable feedstocks positions us well for the energy transition, with several exciting opportunities in the pipeline.",
                "justification": "La narrativa de transición energética ocupa 40% del prepared remarks; los resultados del segmento de renovables representan menos del 2% de revenue.",
            },
            {
                "type": "inconsistency",
                "score": 0.72,
                "text": "We remain committed to our $500M buyback program and see our balance sheet as a key competitive advantage.",
                "justification": "Ratio deuda/EBITDA subió de 3.2x a 5.7x en 12 meses; el programa de recompra fue pausado en los hechos sin anuncio formal.",
            },
        ],
        "Meridian Financial Group": [
            {
                "type": "smoothing",
                "score": 0.76,
                "text": "Credit quality metrics show some normalization from historically low loss rates, which we anticipated and communicated previously.",
                "justification": "NPL ratio subió de 0.8% a 1.9%; 'normalización anticipada' es inconsistente con guidance de estabilidad crediticia del Q2 2024.",
            },
            {
                "type": "defensive_language",
                "score": 0.71,
                "text": "We don't comment on specific reserve build methodology, but our provisioning reflects our conservative risk culture.",
                "justification": "Provisiones para pérdidas crediticias aumentaron 87% QoQ; la negativa a explicar la metodología dificulta la comparación sectorial.",
            },
            {
                "type": "omission",
                "score": 0.67,
                "text": "[Exposición a préstamos CRE por sector no incluida en el suplemento de resultados]",
                "justification": "El detalle de Commercial Real Estate por sector (office/retail/industrial) fue reportado en todos los quarters de 2023; su ausencia es notable en contexto de estrés en el segmento office.",
            },
            {
                "type": "guidance_change",
                "score": 0.61,
                "text": "We are updating our NIM guidance to reflect the current rate environment and competitive deposit dynamics.",
                "justification": "NIM guidance reducida de 3.2% a 2.7% para FY2024; el costo de depósitos subió más rápido de lo proyectado.",
            },
        ],
        "Stratum Semiconductors": [
            {
                "type": "hedge_excess",
                "score": 0.69,
                "text": "While we are optimistic about AI-driven demand, visibility into second-half bookings remains somewhat limited given customer inventory digestion.",
                "justification": "Lenguaje de cobertura excesivo sobre demanda AI contrasta con el tono del investor day de hace 6 semanas; señal de posible revisión de guidance.",
            },
            {
                "type": "smoothing",
                "score": 0.58,
                "text": "The inventory correction in the consumer segment is largely behind us, and we are seeing green shoots in the data center vertical.",
                "justification": "Utiliza 'green shoots' sin datos de bookings concretos; inventarios de clientes todavía 1.8x por encima del promedio histórico según canal checks.",
            },
            {
                "type": "defensive_language",
                "score": 0.51,
                "text": "We prefer not to disclose design-win specifics for competitive reasons, but our pipeline remains robust.",
                "justification": "Rechazo de preguntas sobre design wins con cliente top-3 genera incertidumbre sobre renovación de contrato en Q2 2025.",
            },
        ],
        "ClearPath Logistics": [
            {
                "type": "guidance_change",
                "score": 0.77,
                "text": "We are tightening our full-year EBIT margin guidance to reflect the current freight rate environment and fuel cost dynamics.",
                "justification": "'Tightening' encubre una reducción del rango de 8.5-9.5% a 7.0-8.0%; el término técnico oculta una compresión de 150bps en el punto medio.",
            },
            {
                "type": "smoothing",
                "score": 0.65,
                "text": "Despite near-term volume softness, our network optimization initiatives are creating structural cost advantages that will benefit future periods.",
                "justification": "Volúmenes cayeron 11% YoY; la referencia a 'beneficios futuros' no viene acompañada de timeline ni métricas de seguimiento.",
            },
            {
                "type": "omission",
                "score": 0.59,
                "text": "[Métricas de utilización de flota y days-in-transit ausentes por primera vez en 5 quarters]",
                "justification": "Indicadores operativos clave históricamente reportados en el suplemento de resultados no aparecen; coincide con deterioro de calidad de servicio según datos de tracking.",
            },
        ],
        "Apple Inc.": [
            {
                "type": "hedge_excess",
                "score": 0.48,
                "text": "Services growth may face some variability in coming quarters depending on regulatory developments in certain markets.",
                "justification": "Hedging moderado sobre regulación de App Store en UE; impacto estimado acotado pero lenguaje más cauteloso que quarters anteriores.",
            },
            {
                "type": "smoothing",
                "score": 0.41,
                "text": "Hardware revenue reflected some typical seasonality and product cycle timing, with strong underlying demand fundamentals.",
                "justification": "Caída de 3% en iPhone revenue encuadrada como estacionalidad; benchmarks de demanda de canal muestran estabilidad — señal de baja materialidad.",
            },
        ],
        "Microsoft Corp.": [
            {
                "type": "defensive_language",
                "score": 0.44,
                "text": "We don't break out Copilot revenue specifically, but we're very pleased with the adoption trajectory across enterprise customers.",
                "justification": "Analistas presionaron por métricas de monetización de AI en 4 preguntas consecutivas; la evasión es sistemática pero el contexto competitivo la justifica parcialmente.",
            },
            {
                "type": "hedge_excess",
                "score": 0.38,
                "text": "Azure growth guidance reflects a range of potential outcomes given the dynamic demand environment for AI infrastructure.",
                "justification": "Rango de guidance de Azure más amplio que períodos previos (16-17% vs rango habitual de 1pp); señal de menor visibilidad, no de deterioro.",
            },
        ],
        "Orbis Retail Holdings": [
            {
                "type": "smoothing",
                "score": 0.63,
                "text": "Comp store sales declined modestly, reflecting a deliberate strategy to protect margins rather than chase top-line growth.",
                "justification": "SSS de -4.2% encuadrada como decisión estratégica; no hay evidencia en el call de que la política de precios haya cambiado respecto al año anterior.",
            },
            {
                "type": "guidance_change",
                "score": 0.57,
                "text": "We are narrowing our full-year revenue guidance to reflect updated consumer traffic assumptions.",
                "justification": "'Narrowing' implica reducción del límite superior de $19.8B a $19.2B; el management usó el mismo eufemismo en Q2 2023 antes de un profit warning.",
            },
            {
                "type": "omission",
                "score": 0.52,
                "text": "[E-commerce penetration y conversion rate no mencionados en los prepared remarks]",
                "justification": "KPIs de canal digital reportados en todos los quarters desde 2021; su ausencia podría indicar deterioro en métricas de engagement online.",
            },
        ],
        "Tesla Inc.": [
            {
                "type": "defensive_language",
                "score": 0.84,
                "text": "We don't comment on ASP dynamics quarter to quarter, but our pricing strategy is designed to maximize long-term volume and margin.",
                "justification": "ASP cayó $6,800 en 3 trimestres consecutivos; la negativa a cuantificar el impacto evita reconocer la erosión del poder de fijación de precios.",
            },
            {
                "type": "smoothing",
                "score": 0.79,
                "text": "The margin compression reflects our deliberate investment in future capacity and the acceleration of our energy business transition.",
                "justification": "Margen bruto automotriz cayó a 14.6%; las dos razones estructurales citadas coinciden con las justificaciones de los últimos 3 quarters sin reversión visible.",
            },
            {
                "type": "inconsistency",
                "score": 0.75,
                "text": "FSD adoption continues to grow and we expect meaningful revenue recognition in coming quarters.",
                "justification": "Declaración idéntica a la del Q3 2023 y Q1 2024; la revenue de FSD reconocida en los estados financieros fue $0 en ambos períodos.",
            },
            {
                "type": "guidance_change",
                "score": 0.68,
                "text": "We are recalibrating volume growth expectations for 2024 to reflect production optimization priorities.",
                "justification": "Guidance de volumen reducida de 1.8M a '1.5M+ units'; la palabra 'recalibración' reemplaza a 'reducción' en todas las comunicaciones formales.",
            },
            {
                "type": "omission",
                "score": 0.62,
                "text": "[Regulatory credit revenue desglosado por región ausente en el suplemento financiero]",
                "justification": "Sin regulatory credits, Q4 GAAP net income habría sido negativo; la omisión del detalle geográfico dificulta proyectar sostenibilidad de este ítem.",
            },
        ],
    }

    # ── Discrepancias por perfil ───────────────────────────────────────────────
    DISC_PROFILES = {
        "high": [
            {"metric": "Revenue",       "dev": -8.2,  "sev": "high"},
            {"metric": "Gross Margin",  "dev": -11.4, "sev": "high"},
            {"metric": "EPS",           "dev": -31.0, "sev": "high"},
            {"metric": "Operating CF",  "dev": -22.8, "sev": "high"},
            {"metric": "R&D Expense",   "dev": +18.5, "sev": "medium"},
        ],
        "medium_high": [
            {"metric": "Revenue",       "dev": +3.1,  "sev": "medium"},
            {"metric": "Net Margin",    "dev": -9.4,  "sev": "high"},
            {"metric": "EPS",           "dev": -9.2,  "sev": "medium"},
            {"metric": "NIM",           "dev": -14.8, "sev": "high"},
            {"metric": "Provisions",    "dev": +87.0, "sev": "high"},
        ],
        "medium": [
            {"metric": "Revenue",       "dev": +5.5,  "sev": "low"},
            {"metric": "EBIT Margin",   "dev": -12.3, "sev": "medium"},
            {"metric": "EPS",           "dev": -11.8, "sev": "medium"},
            {"metric": "CapEx",         "dev": +24.0, "sev": "medium"},
        ],
        "medium_low": [
            {"metric": "Revenue",       "dev": +2.1,  "sev": "low"},
            {"metric": "Comp Sales",    "dev": -4.2,  "sev": "medium"},
            {"metric": "EPS",           "dev": +4.1,  "sev": "low"},
        ],
        "low": [
            {"metric": "Revenue",       "dev": +6.1,  "sev": "low"},
            {"metric": "EPS",           "dev": +8.7,  "sev": "low"},
            {"metric": "Gross Margin",  "dev": +1.2,  "sev": "low"},
        ],
    }

    # ── Validation states por perfil ───────────────────────────────────────────
    # (threshold: señales con idx >= N se marcan validadas)
    VALIDATION_CUTOFFS = {
        "high":        99,   # casi ninguna validada — urgencia real
        "medium_high":  3,   # algunas validadas
        "medium":       2,
        "medium_low":   1,
        "low":          0,   # todas validadas — situación sana
    }

    CRITICAL_INDICES = {
        "high":        [0, 1],    # primeras dos marcadas críticas
        "medium_high": [0],
        "medium":      [],
        "medium_low":  [],
        "low":         [],
    }

    # ── Build companies table ──────────────────────────────────────────────────
    companies = [
        {
            "id":     str(uuid.uuid4()),
            "name":   p["name"],
            "ticker": p["ticker"],
            "sector": p["sector"],
        }
        for p in COMPANY_PROFILES
    ]

    reports       = []
    metrics       = []
    discrepancies = []
    signals       = []

    for profile in COMPANY_PROFILES:
        name    = profile["name"]
        ticker  = profile["ticker"]
        rp      = profile["risk_profile"]
        base_rev = profile["base_revenue"]
        rev_g    = profile["revenue_growth"]
        base_eps = profile["base_eps"]
        eps_g    = profile["eps_growth"]
        quarters = profile["quarters"]
        doc_type = profile["doc_type"]

        val_cutoff = VALIDATION_CUTOFFS.get(rp, 99)
        crit_idxs  = CRITICAL_INDICES.get(rp, [])

        # ── Reports + metrics ──────────────────────────────────────────────────
        company_reports = []
        for i, quarter in enumerate(quarters):
            report_id = str(uuid.uuid4())
            days_ago  = (len(quarters) - i) * 90
            company_reports.append({
                "id":            report_id,
                "company_name":  name,
                "period":        quarter,
                "document_type": doc_type,
                "file_hash":     f"mock_{ticker}_{quarter.replace(' ','')}",
                "page_count":    12 + (i * 2),
                "char_count":    38_000 + (i * 4_200),
                "processed_at":  (now - timedelta(days=days_ago)).isoformat(),
                "parse_warnings": [] if i < len(quarters) - 1 else (
                    [{"type": "low_text_density", "page": 8}] if rp in ("high", "medium_high") else []
                ),
            })
            reports.append(company_reports[-1])

            # Revenue: aplica crecimiento compuesto desde base hacia el futuro
            factor      = (1 + rev_g) ** i
            rev_val     = round(base_rev * factor, 1)
            eps_val     = round(base_eps * ((1 + eps_g) ** i), 2)
            margin_base = 0.38 if profile["margins_trend"] == "expanding" else \
                          0.28 if profile["margins_trend"] == "compressing" else 0.33
            margin_val  = round(margin_base + (0.01 if profile["margins_trend"] == "expanding" else
                                               -0.012 if profile["margins_trend"] == "compressing" else 0) * i, 3)

            metrics.extend([
                {
                    "id":            str(uuid.uuid4()),
                    "report_id":     report_id,
                    "company_name":  name,
                    "period":        quarter,
                    "name":          "Revenue",
                    "value":         f"${rev_val}M",
                    "value_numeric": rev_val * 1_000_000,
                    "unit":          "USD",
                },
                {
                    "id":            str(uuid.uuid4()),
                    "report_id":     report_id,
                    "company_name":  name,
                    "period":        quarter,
                    "name":          "EPS",
                    "value":         f"${eps_val:.2f}",
                    "value_numeric": eps_val,
                    "unit":          "USD",
                },
                {
                    "id":            str(uuid.uuid4()),
                    "report_id":     report_id,
                    "company_name":  name,
                    "period":        quarter,
                    "name":          "Gross Margin",
                    "value":         f"{margin_val*100:.1f}%",
                    "value_numeric": margin_val,
                    "unit":          "%",
                },
            ])

        # ── Discrepancias ──────────────────────────────────────────────────────
        disc_template = DISC_PROFILES.get(rp, DISC_PROFILES["medium"])
        last_q  = quarters[-1]
        prev_q  = quarters[-2] if len(quarters) > 1 else quarters[0]

        for j, disc in enumerate(disc_template):
            dev  = disc["dev"]
            # Valor previo sintético, valor actual = prev * (1 + dev/100)
            prev_val = (base_rev * 1_000_000) if disc["metric"] == "Revenue" else \
                       (base_eps)              if disc["metric"] == "EPS" else \
                       (base_rev * 0.33 * 1_000_000)
            curr_val = round(prev_val * (1 + dev / 100), 2)
            is_validated = j < val_cutoff
            decision     = "critical" if j in crit_idxs else ("approved" if is_validated else None)

            discrepancies.append({
                "id":              str(uuid.uuid4()),
                "company_name":    name,
                "metric_name":     disc["metric"],
                "period_current":  last_q,
                "period_previous": prev_q,
                "value_current":   curr_val,
                "value_previous":  prev_val,
                "deviation_pct":   dev,
                "severity":        disc["sev"],
                "direction":       "up" if dev >= 0 else "down",
                "validated":       is_validated,
                "validated_by":    "analyst" if is_validated else None,
                "validated_at":    (now - timedelta(hours=j * 8)).isoformat() if is_validated else None,
                "analyst_note":    _disc_note(disc["metric"], dev) if is_validated else None,
                "decision":        decision,
                "created_at":      (now - timedelta(days=3)).isoformat(),
            })

        # ── Señales narrativas ─────────────────────────────────────────────────
        pool = SIGNAL_POOLS.get(name, [])
        last_report = company_reports[-1] if company_reports else None
        if not last_report:
            continue

        for k, sig in enumerate(pool):
            is_validated = k < val_cutoff
            decision     = "critical" if k in crit_idxs else ("approved" if is_validated else None)
            report_idx   = k % len(company_reports)

            signals.append({
                "id":            str(uuid.uuid4()),
                "report_id":     company_reports[report_idx]["id"],
                "company_name":  name,
                "period":        company_reports[report_idx]["period"],
                "signal_type":   sig["type"],
                "score":         sig["score"],
                "source_text":   sig["text"],
                "justification": sig["justification"],
                "validated":     is_validated,
                "decision":      decision,
                "analyst":       "analyst" if is_validated else None,
                "analyst_note":  _sig_note(sig["type"], decision) if is_validated else None,
                "validated_at":  (now - timedelta(hours=k * 6)).isoformat() if is_validated else None,
                "created_at":    (now - timedelta(days=2 + k)).isoformat(),
            })

    return {
        "companies":     companies,
        "reports":       reports,
        "metrics":       metrics,
        "discrepancies": discrepancies,
        "signals":       signals,
    }


def _disc_note(metric: str, dev: float) -> str:
    """Genera una nota de analista realista para una discrepancia validada."""
    notes = {
        "Revenue":      f"Variación de {dev:+.1f}% confirmada contra reporte auditado. {'Investigar canal de ventas.' if dev < -5 else 'Consistente con guidance.'}",
        "EPS":          f"EPS delta {dev:+.1f}% reconciliado con estados financieros. {'Revisar estructura de costos.' if dev < -15 else 'Dentro del rango esperado.'}",
        "Gross Margin": f"Compresión de margen de {abs(dev):.1f}pp validada. {'Presión de costos de insumos.' if dev < 0 else 'Mejora operativa confirmada.'}",
        "NIM":          f"NIM en {dev:+.1f}% vs período previo, en línea con presión de tasas del sector.",
        "Provisions":   f"Build de provisiones {dev:+.1f}% respecto a Q anterior. Solicité detalle de cartera al CFO.",
        "CapEx":        f"CapEx {dev:+.1f}% YoY. Confirmar si responde a plan de expansión aprobado por el board.",
    }
    return notes.get(metric, f"Discrepancia de {dev:+.1f}% revisada y documentada.")


def _sig_note(signal_type: str, decision: Optional[str]) -> Optional[str]:
    if not decision:
        return None
    if decision == "critical":
        notes = {
            "guidance_change": "⚠ CRÍTICO: Cambio de guidance no anunciado formalmente. Escalar a portfolio manager.",
            "omission": "⚠ CRÍTICO: Omisión sistemática de KPI clave. Solicitar aclaración al IR.",
            "defensive_language": "⚠ CRÍTICO: Patrón de evasión recurrente en 3 calls consecutivos.",
            "inconsistency": "⚠ CRÍTICO: Inconsistencia entre comunicación oral y estados financieros.",
        }
        return notes.get(signal_type, "⚠ CRÍTICO: Señal marcada para seguimiento inmediato.")
    if decision == "approved":
        notes = {
            "smoothing": "Confirmado. Framing positivo documentado, impacto cuantificado como bajo.",
            "hedge_excess": "Lenguaje hedging detectado, dentro de rango normal para el sector.",
            "defensive_language": "Revisado. Evasión registrada, sin indicios de ocultamiento material.",
        }
        return notes.get(signal_type, "Señal revisada y catalogada.")
    return None


# ── Mock infrastructure ────────────────────────────────────────────────────────

class MockResponse:
    def __init__(self, data):
        self.data  = data
        self.count = len(data) if isinstance(data, list) else None


class MockQuery:
    def __init__(self, table_name, data_list):
        self.table_name   = table_name
        self._result_data = list(data_list)

    def select(self, *args):
        return self

    def eq(self, field, value):
        self._result_data = [d for d in self._result_data if d.get(field) == value]
        return self

    def neq(self, field, value):
        self._result_data = [d for d in self._result_data if d.get(field) != value]
        return self

    def gte(self, field, value):
        self._result_data = [d for d in self._result_data if (d.get(field) or 0) >= value]
        return self

    def lte(self, field, value):
        self._result_data = [d for d in self._result_data if (d.get(field) or 0) <= value]
        return self

    def order(self, field, desc=False):
        SEV = {"high": 0, "medium": 1, "low": 2}
        if field == "severity":
            self._result_data = sorted(
                self._result_data,
                key=lambda x: SEV.get(x.get(field, "low"), 2),
                reverse=desc,
            )
        else:
            self._result_data = sorted(
                self._result_data,
                key=lambda x: (x.get(field) is None, x.get(field, "")),
                reverse=desc,
            )
        return self

    def limit(self, n):
        self._result_data = self._result_data[:n]
        return self

    def execute(self):
        return MockResponse(self._result_data)

    # ── Write ops (in-memory) ─────────────────────────────────────────────────

    def insert(self, rows):
        if isinstance(rows, dict):
            rows = [rows]
        self._pending_insert = rows
        return self

    def update(self, patch):
        self._pending_patch = patch
        return self

    def _apply_update(self, global_list, patch):
        updated = []
        for item in global_list:
            if item in self._result_data:
                updated.append({**item, **patch})
            else:
                updated.append(item)
        return updated

    def upsert(self, rows):
        return self.insert(rows)


class MockClient:
    def __init__(self, mock_data: dict):
        self.mock_data = mock_data

    def table(self, name: str) -> "MockTableProxy":
        return MockTableProxy(name, self.mock_data)


class MockTableProxy:
    """
    Proxy que separa la construcción de la query de la ejecución,
    y soporta update() con escritura real al diccionario compartido.
    """

    def __init__(self, table_name: str, mock_data: dict):
        self._name       = table_name
        self._mock_data  = mock_data
        self._filters    = []       # list of (field, op, value)
        self._order      = []       # list of (field, desc)
        self._limit_val  = None
        self._patch      = None
        self._insert_rows= None

    # ── Builder methods ────────────────────────────────────────────────────────

    def select(self, *args):
        return self

    def eq(self, field, value):
        self._filters.append((field, "eq", value))
        return self

    def neq(self, field, value):
        self._filters.append((field, "neq", value))
        return self

    def gte(self, field, value):
        self._filters.append((field, "gte", value))
        return self

    def lte(self, field, value):
        self._filters.append((field, "lte", value))
        return self

    def order(self, field, desc=False):
        self._order.append((field, desc))
        return self

    def limit(self, n):
        self._limit_val = n
        return self

    def insert(self, rows):
        self._insert_rows = [rows] if isinstance(rows, dict) else rows
        return self

    def update(self, patch: dict):
        self._patch = patch
        return self

    def upsert(self, rows):
        return self.insert(rows)

    # ── Execute ────────────────────────────────────────────────────────────────

    def execute(self) -> MockResponse:
        data = self._mock_data.get(self._name, [])

        # INSERT
        if self._insert_rows is not None:
            for row in self._insert_rows:
                if "id" not in row:
                    row["id"] = str(uuid.uuid4())
                data.append(row)
            self._mock_data[self._name] = data
            return MockResponse(self._insert_rows)

        # UPDATE — apply patch to filtered rows, write back
        if self._patch is not None:
            result = self._apply_filters(data)
            updated = []
            for item in data:
                if item in result:
                    updated.append({**item, **self._patch})
                else:
                    updated.append(item)
            self._mock_data[self._name] = updated
            # Return the updated rows
            return MockResponse([{**item, **self._patch} for item in result])

        # SELECT
        result = self._apply_filters(data)
        result = self._apply_order(result)
        if self._limit_val is not None:
            result = result[: self._limit_val]
        return MockResponse(result)

    def _apply_filters(self, data):
        result = data
        for field, op, value in self._filters:
            if op == "eq":
                result = [d for d in result if d.get(field) == value]
            elif op == "neq":
                result = [d for d in result if d.get(field) != value]
            elif op == "gte":
                result = [d for d in result if (d.get(field) or 0) >= value]
            elif op == "lte":
                result = [d for d in result if (d.get(field) or 0) <= value]
        return result

    def _apply_order(self, data):
        SEV = {"high": 0, "medium": 1, "low": 2}
        for field, desc in reversed(self._order):
            if field == "severity":
                data = sorted(data, key=lambda x: SEV.get(x.get(field, "low"), 2), reverse=desc)
            else:
                data = sorted(data, key=lambda x: (x.get(field) is None, x.get(field, "")), reverse=desc)
        return data


# ── Public API ─────────────────────────────────────────────────────────────────

def get_client():
    global _client, _mock_data

    if config.USE_MOCK:
        if _mock_data is None:
            _mock_data = _generate_mock_data()
        return MockClient(_mock_data)

    if _client is None:
        from supabase import create_client
        url = config.SUPABASE_URL
        key = config.SUPABASE_SERVICE_KEY
        if not url or not key or key == "mock-key":
            raise ValueError("SUPABASE_URL y SUPABASE_SERVICE_KEY requeridas en modo real")
        _client = create_client(url, key)
    return _client


def get_metrics(company_name: str):
    client = get_client()
    if config.USE_MOCK:
        return client.table("metrics").eq("company_name", company_name).execute().data
    res = client.table("metrics").select("*").eq("company_name", company_name).execute()
    return res.data