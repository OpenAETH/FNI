-- ============================================================
-- FNI — Supabase schema (PostgreSQL)
-- Ejecutar en el SQL editor de Supabase
-- ============================================================

-- Habilitar UUID
create extension if not exists "uuid-ossp";


-- ── companies ────────────────────────────────────────────────
create table if not exists companies (
  id          uuid primary key default uuid_generate_v4(),
  name        text not null unique,
  ticker      text,
  sector      text,
  created_at  timestamptz default now()
);


-- ── reports ──────────────────────────────────────────────────
-- Un report = un documento ingestado (earnings call o reporte)
create table if not exists reports (
  id              uuid primary key default uuid_generate_v4(),
  company_id      uuid references companies(id) on delete cascade,
  company_name    text not null,      -- desnormalizado para queries rápidas
  period          text not null,      -- "Q3 2024"
  document_type   text not null,      -- "transcript" | "financial_report" | ...
  file_hash       text unique,        -- SHA-256 parcial para deduplicar
  source_path     text,
  page_count      int,
  char_count      int,
  mongo_doc_id    text,               -- referencia al doc crudo en MongoDB
  processed_at    timestamptz default now(),
  parse_warnings  text[]
);

create index if not exists idx_reports_company   on reports(company_name);
create index if not exists idx_reports_period    on reports(period);
create index if not exists idx_reports_hash      on reports(file_hash);


-- ── metrics ──────────────────────────────────────────────────
create table if not exists metrics (
  id              uuid primary key default uuid_generate_v4(),
  report_id       uuid references reports(id) on delete cascade,
  company_name    text not null,
  period          text not null,
  name            text not null,      -- "Revenue", "EPS", "Gross Margin"
  value           text,               -- raw string
  value_numeric   numeric,            -- parseado
  unit            text,
  vs_guidance     text,               -- "beat" | "miss" | "in-line" | "not_disclosed"
  source_quote    text
);

create index if not exists idx_metrics_report    on metrics(report_id);
create index if not exists idx_metrics_company   on metrics(company_name, name);


-- ── executive_statements ─────────────────────────────────────
create table if not exists executive_statements (
  id          uuid primary key default uuid_generate_v4(),
  report_id   uuid references reports(id) on delete cascade,
  speaker     text,
  role        text,
  statement   text,
  sentiment   text,   -- "positive" | "neutral" | "cautious" | "negative"
  topic       text
);

create index if not exists idx_stmt_report on executive_statements(report_id);
create index if not exists idx_stmt_sentiment on executive_statements(sentiment);


-- ── forward_guidance ─────────────────────────────────────────
create table if not exists forward_guidance (
  id          uuid primary key default uuid_generate_v4(),
  report_id   uuid references reports(id) on delete cascade,
  company_name text not null,
  metric      text,
  value       text,
  period      text,
  qualifier   text    -- "expects" | "targets" | "may" | "subject_to" | "reaffirms"
);

create index if not exists idx_guidance_report on forward_guidance(report_id);


-- ── discrepancies ─────────────────────────────────────────────
-- Generado por el motor de comparación (Fase 2), creamos la tabla ya
create table if not exists discrepancies (
  id              uuid primary key default uuid_generate_v4(),
  company_name    text not null,
  metric_name     text not null,
  period_current  text not null,
  period_previous text not null,
  value_current   numeric,
  value_previous  numeric,
  deviation_pct   numeric,            -- ((current - previous) / previous) * 100
  severity        text,               -- "low" | "medium" | "high"
  validated       bool default false,
  validated_by    text,
  validated_at    timestamptz,
  analyst_note    text,
  created_at      timestamptz default now()
);

create index if not exists idx_disc_company  on discrepancies(company_name);
create index if not exists idx_disc_severity on discrepancies(severity);


-- ── narrative_signals ────────────────────────────────────────
-- Generado por el motor de señales (Fase 2)
create table if not exists narrative_signals (
  id            uuid primary key default uuid_generate_v4(),
  report_id     uuid references reports(id) on delete cascade,
  company_name  text not null,
  period        text not null,
  signal_type   text,    -- "defensive_language" | "smoothing" | "omission" | "inconsistency"
  score         numeric, -- 0.0 a 1.0
  source_text   text,
  justification text,
  validated     bool default false,
  validated_by  text,
  decision      text,    -- "approved" | "rejected"
  analyst_note  text,
  created_at    timestamptz default now()
);

create index if not exists idx_signals_report   on narrative_signals(report_id);
create index if not exists idx_signals_score    on narrative_signals(score desc);
create index if not exists idx_signals_decision on narrative_signals(decision);
