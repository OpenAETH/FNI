"""
api/main.py
FastAPI — endpoints del sistema FNI.
Expone datos al frontend y maneja validaciones de analistas.
"""

from __future__ import annotations
from typing import Optional
from datetime import datetime

from fastapi import FastAPI, HTTPException, Header, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import config
from db import supabase_client as sb, mongo_client as mg
from api.comparator import compare_quarters, save_discrepancies
from api.signal_detector import detect_signals, save_signals

from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import os

# ── App setup ─────────────────────────────────────────────────────────────────

app = FastAPI(
    title="FNI — Financial Narrative Intelligence",
    version="0.1.0",
    docs_url="/docs",
)

# Reemplazar la configuración CORS actual
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS if ALLOWED_ORIGINS != ["*"] else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)



# ── Auth ──────────────────────────────────────────────────────────────────────

API_KEY = config.__dict__.get("API_KEY", "dev-key-change-in-prod")

def verify_key(x_api_key: str = Header(default="dev-key-change-in-prod")):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="API key inválida")
    return x_api_key


# ── Pydantic models ───────────────────────────────────────────────────────────

class ValidateDiscrepancyRequest(BaseModel):
    validated_by:  str
    analyst_note:  Optional[str] = None


class ValidateSignalRequest(BaseModel):
    decision:      str            # "approved" | "rejected"
    analyst_note:  Optional[str] = None
    analyst:       str


class AnalyzeRequest(BaseModel):
    company_name: str
    report_id:    Optional[str] = None  # si ya existe, lo usa para señales


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {
        "status": "ok",
        "timestamp": datetime.utcnow().isoformat(),
        "mongo": mg.ping(),
    }


# ── Companies ─────────────────────────────────────────────────────────────────

@app.get("/companies")
def list_companies():
    """Lista todas las empresas con reportes ingestados."""
    db = sb.get_client()
    result = db.table("companies").select("*").order("name").execute()
    return result.data


@app.get("/companies/{name}/summary")
def company_summary(name: str):
    """Resumen de una empresa: reportes, última métrica, señales pendientes."""
    db = sb.get_client()

    reports = db.table("reports") \
        .select("id, period, document_type, processed_at") \
        .eq("company_name", name) \
        .order("processed_at", desc=True) \
        .execute().data

    pending_signals = db.table("signals") \
        .select("id", count="exact") \
        .eq("company_name", name) \
        .eq("validated", False) \
        .execute()

    pending_discrepancies = db.table("discrepancies") \
        .select("id", count="exact") \
        .eq("company_name", name) \
        .eq("validated", False) \
        .execute()

    return {
        "company": name,
        "report_count":             len(reports),
        "latest_report":            reports[0] if reports else None,
        "pending_signals":          pending_signals.count or 0,
        "pending_discrepancies":    pending_discrepancies.count or 0,
        "periods":                  [r["period"] for r in reports],
    }


# ── Reports ───────────────────────────────────────────────────────────────────

@app.get("/reports")
def list_reports(
    company: Optional[str] = Query(None),
    limit:   int           = Query(20, le=100),
):
    db = sb.get_client()
    query = db.table("reports").select("*").order("processed_at", desc=True).limit(limit)
    if company:
        query = query.eq("company_name", company)
    return query.execute().data


@app.get("/reports/{report_id}")
def get_report(report_id: str):
    db = sb.get_client()
    result = db.table("reports").select("*").eq("id", report_id).execute()
    if not result.data:
        raise HTTPException(404, "Reporte no encontrado")

    report = result.data[0]

    # Enriquecer con métricas y statements
    metrics = db.table("metrics").select("*").eq("report_id", report_id).execute().data
    statements = db.table("executive_statements").select("*").eq("report_id", report_id).execute().data
    guidance = db.table("forward_guidance").select("*").eq("report_id", report_id).execute().data

    return {**report, "metrics": metrics, "statements": statements, "guidance": guidance}


# ── Discrepancies ─────────────────────────────────────────────────────────────

@app.get("/discrepancies")
def list_discrepancies(
    company:  Optional[str] = Query(None),
    severity: Optional[str] = Query(None, pattern="^(low|medium|high)$"),
    validated: Optional[bool] = Query(None),
    limit: int = Query(50, le=200),
):
    db = sb.get_client()
    query = (
        db.table("discrepancies")
        .select("*")
        .order("severity")           # high primero (alphabetically: high < low < medium — ajustar con case)
        .order("deviation_pct", desc=True)
        .limit(limit)
    )
    if company:
        query = query.eq("company_name", company)
    if severity:
        query = query.eq("severity", severity)
    if validated is not None:
        query = query.eq("validated", validated)

    return query.execute().data


@app.post("/discrepancies/analyze/{company_name}", dependencies=[Depends(verify_key)])
def analyze_discrepancies(company_name: str):
    """Corre el motor de comparación para una empresa y persiste resultados."""
    discrepancies = compare_quarters(company_name)
    saved = save_discrepancies(discrepancies)
    return {
        "company":  company_name,
        "analyzed": len(discrepancies),
        "saved":    saved,
        "results":  [d.__dict__ for d in discrepancies],
    }


@app.post("/discrepancies/{disc_id}/validate", dependencies=[Depends(verify_key)])
def validate_discrepancy(disc_id: str, body: ValidateDiscrepancyRequest):
    db = sb.get_client()
    result = db.table("discrepancies").update({
        "validated":    True,
        "validated_by": body.validated_by,
        "validated_at": datetime.utcnow().isoformat(),
        "analyst_note": body.analyst_note,
    }).eq("id", disc_id).execute()

    if not result.data:
        raise HTTPException(404, "Discrepancia no encontrada")
    return result.data[0]


# ── Signals ───────────────────────────────────────────────────────────────────

# En api/main.py, verifica que este endpoint esté así:

@app.get("/signals")
def list_signals(
    company:   Optional[str]  = Query(None),
    validated: Optional[bool] = Query(None),
    min_score: float          = Query(0.0, ge=0.0, le=1.0),
    limit:     int            = Query(50, le=200),
):
    db = sb.get_client()
    query = (
        db.table("signals")
        .select("*")
        .order("score", desc=True)
        .limit(limit)
    )
    if company:
        query = query.eq("company_name", company)
    if validated is not None:
        query = query.eq("validated", validated)
    if min_score > 0:
        query = query.gte("score", min_score)

    result = query.execute()
    return result.data


@app.post("/signals/analyze/{report_id}", dependencies=[Depends(verify_key)])
def analyze_signals(report_id: str):
    """
    Corre el detector de señales narrativas para un reporte específico.
    Requiere que el texto crudo esté en MongoDB.
    """
    db = sb.get_client()
    report = db.table("reports").select("*").eq("id", report_id).execute().data
    if not report:
        raise HTTPException(404, "Reporte no encontrado")

    r = report[0]

    # Obtener texto limpio de MongoDB
    mongo_db = mg.get_db()
    raw_doc = mongo_db["raw_documents"].find_one({"file_hash": r["file_hash"]})
    if not raw_doc:
        raise HTTPException(404, "Texto crudo no encontrado en MongoDB")

    clean_text = raw_doc.get("clean_text", "")
    signals = detect_signals(
        report_id  = report_id,
        company    = r["company_name"],
        period     = r["period"],
        clean_text = clean_text,
    )
    saved = save_signals(signals)

    return {
        "report_id": report_id,
        "company":   r["company_name"],
        "period":    r["period"],
        "signals_found": len(signals),
        "signals_saved": saved,
        "results": [s.__dict__ for s in signals],
    }


@app.post("/signals/{signal_id}/validate", dependencies=[Depends(verify_key)])
def validate_signal(signal_id: str, body: ValidateSignalRequest):
    if body.decision not in ("approved", "rejected"):
        raise HTTPException(400, "decision debe ser 'approved' o 'rejected'")

    db = sb.get_client()
    result = db.table("signals").update({
        "validated":  True,
        "decision":   body.decision,
        "analyst_note": body.analyst_note,
    }).eq("id", signal_id).execute()

    if not result.data:
        raise HTTPException(404, "Señal no encontrada")

    # Log en MongoDB
    mg.log_analyst_feedback(
        signal_id   = signal_id,
        decision    = body.decision,
        analyst_note= body.analyst_note,
        analyst     = body.analyst,
    )

    return result.data[0]


# ── Validate (endpoint genérico para el dashboard) ────────────────────────────

@app.post("/validate", dependencies=[Depends(verify_key)])
def validate_item(body: dict):
    """
    Endpoint genérico de validación para el frontend.
    Recibe: { type: "signal"|"discrepancy", id: "...", decision: "...", analyst: "...", note: "..." }
    """
    item_type = body.get("type")
    item_id   = body.get("id")
    decision  = body.get("decision")
    analyst   = body.get("analyst", "anonymous")
    note      = body.get("note")

    if not all([item_type, item_id, decision]):
        raise HTTPException(400, "Faltan campos requeridos: type, id, decision")

    if item_type == "signal":
        return validate_signal(item_id, ValidateSignalRequest(
            decision=decision, analyst_note=note, analyst=analyst
        ))
    elif item_type == "discrepancy":
        return validate_discrepancy(item_id, ValidateDiscrepancyRequest(
            validated_by=analyst, analyst_note=note
        ))
    else:
        raise HTTPException(400, f"Tipo inválido: {item_type}")


# ── Servir archivos estáticos (Frontend) ────────────────────────────────────

# Determinar si estamos en producción (Render) o desarrollo local
FRONTEND_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "frontend")

# Si la carpeta frontend existe, servir archivos estáticos
if os.path.exists(FRONTEND_DIR):
    app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")
    
    @app.get("/")
    async def serve_dashboard():
        """Sirve el dashboard HTML principal"""
        dashboard_path = os.path.join(FRONTEND_DIR, "dashboard.html")
        if os.path.exists(dashboard_path):
            return FileResponse(dashboard_path)
        return {"message": "FNI API is running. Dashboard not found."}
    
    @app.get("/dashboard")
    async def dashboard_redirect():
        """Redirección al dashboard"""
        return FileResponse(os.path.join(FRONTEND_DIR, "dashboard.html"))
    
    @app.get("/health-check")
    async def health_check():
        """Endpoint simple para health checks"""
        return {"status": "healthy", "service": "FNI API"}