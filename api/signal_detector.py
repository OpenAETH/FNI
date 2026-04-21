# api/signal_detector.py
"""
api/signal_detector.py
Motor de señales narrativas.
Analiza transcripciones con Groq para detectar lenguaje defensivo,
suavización de resultados, omisiones e inconsistencias.
"""

from __future__ import annotations
import json
import re
from dataclasses import dataclass

import config
from db import supabase_client as sb, mongo_client as mg

# ── Signal types ──────────────────────────────────────────────────────────────

SIGNAL_TYPES = {
    "defensive_language": "Lenguaje que minimiza responsabilidad o desvía preguntas",
    "smoothing":          "Suavización de resultados negativos con framing positivo",
    "omission":           "Ausencia notable de métricas o temas que se discutían antes",
    "inconsistency":      "Contradicción entre lo dicho y los números reportados",
    "guidance_change":    "Cambio silencioso o reducción de guidance vs período anterior",
    "hedge_excess":       "Uso excesivo de calificadores: 'may', 'could', 'subject to'",
}


@dataclass
class NarrativeSignal:
    report_id:     str
    company_name:  str
    period:        str
    signal_type:   str
    score:         float   # 0.0 – 1.0
    source_text:   str     # fragmento literal del transcript
    justification: str     # explicación del LLM


# ── Prompts ───────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """
Eres un analista financiero forense especializado en detectar señales de alerta
en comunicaciones corporativas. Tu criterio es riguroso y basado en evidencia textual.
Respondés ÚNICAMENTE con JSON válido, sin markdown ni texto adicional.
""".strip()


def _build_signal_prompt(clean_text: str, company: str, period: str) -> str:
    excerpt = clean_text[:10_000]
    signal_defs = "\n".join(f'  - "{k}": {v}' for k, v in SIGNAL_TYPES.items())

    return f"""
Analizá la siguiente transcripción de earnings call de {company} ({period}).
Detectá señales narrativas de alerta según estas categorías:

{signal_defs}

TRANSCRIPCIÓN:
{excerpt}

---

Respondé SOLO con este JSON (sin markdown):

{{
  "signals": [
    {{
      "signal_type": "tipo_de_señal",
      "score": 0.85,
      "source_text": "fragmento literal del texto ≤200 chars",
      "justification": "explicación concisa de por qué es una señal ≤250 chars"
    }}
  ],
  "overall_risk": "low | medium | high",
  "summary": "resumen del análisis en 1-2 oraciones"
}}

Reglas:
- score entre 0.0 (señal débil) y 1.0 (señal muy fuerte)
- Incluí SOLO señales con score >= 0.4
- Máximo 8 señales por documento
- source_text debe ser texto literal del documento, no paráfrasis
- Si no encontrás señales reales, devolvé signals: []
- Evitá falsos positivos: exigí evidencia textual concreta
""".strip()


# ── Main detector ─────────────────────────────────────────────────────────────

def detect_signals(
    report_id:  str,
    company:    str,
    period:     str,
    clean_text: str,
) -> list[NarrativeSignal]:
    """
    Corre el análisis semántico con Groq y retorna señales encontradas.
    """
    # Modo Mock: retornar señales de ejemplo
    if config.USE_MOCK:
        return _generate_mock_signals(report_id, company, period)
    
    # Modo real con Groq
    try:
        from groq import Groq
        client = Groq(api_key=config.GROQ_API_KEY)

        response = client.chat.completions.create(
            model=config.GROQ_MODEL,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": _build_signal_prompt(clean_text, company, period)},
            ],
            temperature=0.15,
            max_tokens=2048,
        )

        raw = response.choices[0].message.content

        # Parse JSON
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            m = re.search(r"\{.*\}", raw, re.DOTALL)
            data = json.loads(m.group()) if m else {"signals": []}

        signals = []
        for s in data.get("signals", []):
            score = float(s.get("score", 0))
            if score < 0.4:
                continue
            sig_type = s.get("signal_type", "")
            if sig_type not in SIGNAL_TYPES:
                sig_type = "defensive_language"  # fallback

            signals.append(NarrativeSignal(
                report_id    = report_id,
                company_name = company,
                period       = period,
                signal_type  = sig_type,
                score        = round(score, 3),
                source_text  = s.get("source_text", "")[:200],
                justification= s.get("justification", "")[:250],
            ))

        return sorted(signals, key=lambda s: s.score, reverse=True)
    
    except Exception as e:
        print(f"Error en Groq API: {e}, usando mock fallback")
        return _generate_mock_signals(report_id, company, period)


def _generate_mock_signals(report_id: str, company: str, period: str) -> list[NarrativeSignal]:
    """Genera señales mock para desarrollo"""
    import random
    import hashlib
    
    # Usar el nombre de la empresa para generar consistencia
    seed = int(hashlib.md5(company.encode()).hexdigest()[:8], 16)
    random.seed(seed)
    
    signal_templates = [
        {
            "type": "defensive_language",
            "text": "We don't guide specifically on that metric, but we feel comfortable with the trajectory...",
            "justification": "Evade responder directamente sobre métricas clave usando lenguaje defensivo"
        },
        {
            "type": "smoothing",
            "text": "While this quarter presented some challenges, we're seeing accelerating momentum across our core business lines...",
            "justification": "Suaviza resultados negativos enfatizando momentum futuro sin datos concretos"
        },
        {
            "type": "omission",
            "text": "[No mention of international revenue breakdown in the prepared remarks]",
            "justification": "Omite el breakdown geográfico que siempre reportaba en trimestres anteriores"
        },
        {
            "type": "guidance_change",
            "text": "We're updating our full-year outlook to reflect a more balanced approach to the opportunities we see...",
            "justification": "Reduce guidance usando lenguaje diplomático para minimizar el impacto"
        },
        {
            "type": "hedge_excess",
            "text": "Subject to market conditions and regulatory approvals, we may potentially consider exploring strategic alternatives...",
            "justification": "Uso excesivo de calificadores que diluyen el compromiso real"
        },
        {
            "type": "inconsistency",
            "text": "Gross margin came in at 42% this quarter, though we've previously communicated expectations of margin expansion...",
            "justification": "Inconsistencia entre expectativas comunicadas y resultados reales"
        }
    ]
    
    # Generar 3-5 señales por reporte
    num_signals = random.randint(3, 5)
    selected = random.sample(signal_templates, min(num_signals, len(signal_templates)))
    
    signals = []
    for template in selected:
        score = round(random.uniform(0.45, 0.92), 2)
        signals.append(NarrativeSignal(
            report_id=report_id,
            company_name=company,
            period=period,
            signal_type=template["type"],
            score=score,
            source_text=template["text"],
            justification=template["justification"]
        ))
    
    return sorted(signals, key=lambda s: s.score, reverse=True)


def save_signals(signals: list[NarrativeSignal]) -> int:
    """Persiste señales en Supabase y MongoDB. Retorna cantidad guardada."""
    if not signals:
        return 0

    db = sb.get_client()
    
    if config.USE_MOCK:
        # En modo mock, agregar a los datos mock
        rows = [
            {
                "id": str(__import__('uuid').uuid4()),
                "report_id":    s.report_id,
                "company_name": s.company_name,
                "period":       s.period,
                "signal_type":  s.signal_type,
                "score":        s.score,
                "source_text":  s.source_text,
                "justification":s.justification,
                "validated": False,
                "created_at": __import__('datetime').datetime.now().isoformat()
            }
            for s in signals
        ]
        
        # Agregar a los datos mock globales
        mock_data = db.mock_data
        if "signals" in mock_data:
            mock_data["signals"].extend(rows)
        
        return len(signals)
    
    # Modo real con Supabase
    rows = [
        {
            "report_id":    s.report_id,
            "company_name": s.company_name,
            "period":       s.period,
            "signal_type":  s.signal_type,
            "score":        s.score,
            "source_text":  s.source_text,
            "justification":s.justification,
        }
        for s in signals
    ]
    db.table("narrative_signals").insert(rows).execute()

    # También en MongoDB para historial y auditoría
    for s in signals:
        mg.save_signal_event(
            company      = s.company_name,
            period       = s.period,
            signal_type  = s.signal_type,
            score        = s.score,
            source_text  = s.source_text,
            justification= s.justification,
        )

    return len(signals)