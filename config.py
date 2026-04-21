# config.py
import os

# ── API & Security ────────────────────────────────────────────────
API_KEY = os.getenv("API_KEY", "dev-key-change-in-prod")

# ── Mock Mode (Desarrollo Local) ─────────────────────────────────
USE_MOCK = os.getenv("USE_MOCK", "true").lower() == "true"

# ── Groq (LLM) ────────────────────────────────────────────────────
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "mock-key")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-70b-versatile")

# ── Supabase (PostgreSQL) ─────────────────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL", "http://localhost:54321")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "mock-key")

# ── MongoDB (Documentos Crudos) ───────────────────────────────────
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "fni_raw_data")

# ── Logging ───────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")