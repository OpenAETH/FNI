# config.py
import os
from dotenv import load_dotenv

# Cargar variables de entorno desde .env en desarrollo
if os.path.exists(".env"):
    load_dotenv()

# ── Environment ──────────────────────────────────────────────────
ENVIRONMENT = os.getenv("ENVIRONMENT", "development")
IS_PRODUCTION = ENVIRONMENT == "production"

# ── API & Security ────────────────────────────────────────────────
API_KEY = os.getenv("API_KEY", "dev-key-change-in-prod")
if IS_PRODUCTION and API_KEY == "dev-key-change-in-prod":
    raise ValueError("❌ API_KEY debe ser configurada en producción")

# ── Mock Mode (Desarrollo Local) ─────────────────────────────────
USE_MOCK = os.getenv("USE_MOCK", "true").lower() == "true"
if IS_PRODUCTION:
    USE_MOCK = False  # Forzar modo real en producción

# ── Groq (LLM) ────────────────────────────────────────────────────
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "mock-key")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-70b-versatile")

if IS_PRODUCTION and GROQ_API_KEY == "mock-key":
    raise ValueError("❌ GROQ_API_KEY debe ser configurada en producción")

# ── Supabase (PostgreSQL) ─────────────────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL", "http://localhost:54321")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "mock-key")

if IS_PRODUCTION and SUPABASE_SERVICE_KEY == "mock-key":
    raise ValueError("❌ SUPABASE_SERVICE_KEY debe ser configurada en producción")

# ── MongoDB (Documentos Crudos) ───────────────────────────────────
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "fni_raw_data")

# ── Logging ───────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")