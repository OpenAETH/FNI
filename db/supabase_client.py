# api/db/supabase_client.py
import os
import uuid
from datetime import datetime, timedelta
from typing import Optional
import config

_client = None
_mock_data = None

def _generate_mock_data():
    """Genera datos mock para desarrollo"""
    companies = [
        {"id": str(uuid.uuid4()), "name": "Apple Inc.", "ticker": "AAPL", "sector": "Technology"},
        {"id": str(uuid.uuid4()), "name": "Microsoft Corp.", "ticker": "MSFT", "sector": "Technology"},
        {"id": str(uuid.uuid4()), "name": "Tesla Inc.", "ticker": "TSLA", "sector": "Automotive"},
    ]
    
    reports = []
    metrics = []
    
    for company in companies:
        for i in range(3):
            report_id = str(uuid.uuid4())
            period = f"Q{(i%4)+1} 2024"
            
            reports.append({
                "id": report_id,
                "company_name": company["name"],
                "period": period,
                "document_type": "transcript",
                "file_hash": f"mock_hash_{company['ticker']}_{period}",
                "page_count": 15 + i,
                "char_count": 45000 + (i * 5000),
                "processed_at": (datetime.now() - timedelta(days=90-i*90)).isoformat(),
                "parse_warnings": []
            })
            
            # Métricas mock
            base_revenue = 89500 if company["name"] == "Apple Inc." else 62000 if company["name"] == "Microsoft Corp." else 25000
            revenue = base_revenue + (i * 5000)
            
            metrics.extend([
                {
                    "id": str(uuid.uuid4()),
                    "report_id": report_id,
                    "company_name": company["name"],
                    "period": period,
                    "name": "Revenue",
                    "value": f"${revenue}M",
                    "value_numeric": revenue * 1_000_000,
                    "unit": "USD"
                },
                {
                    "id": str(uuid.uuid4()),
                    "report_id": report_id,
                    "company_name": company["name"],
                    "period": period,
                    "name": "EPS",
                    "value": f"${1.20 + (i*0.15):.2f}",
                    "value_numeric": 1.20 + (i*0.15),
                    "unit": "USD"
                }
            ])
    
    # Discrepancias mock
    discrepancies = []
    for company in companies:
        for j in range(5):
            severity = ["high", "medium", "low"][j % 3]
            discrepancies.append({
                "id": str(uuid.uuid4()),
                "company_name": company["name"],
                "metric_name": "Revenue" if j % 2 == 0 else "EPS",
                "period_current": "Q4 2024",
                "period_previous": "Q3 2024",
                "value_current": 95000 if j % 2 == 0 else 1.45,
                "value_previous": 89500 if j % 2 == 0 else 1.20,
                "deviation_pct": 6.15 if j % 2 == 0 else 20.83,
                "severity": severity,
                "validated": j > 2,
                "validated_by": "analyst" if j > 2 else None,
                "validated_at": datetime.now().isoformat() if j > 2 else None,
                "analyst_note": "Revisado y confirmado" if j > 2 else None,
                "created_at": datetime.now().isoformat()
            })
    
    # AHORA SÍ: Señales mock (después de tener todos los reports)
    signals = []
    for company in companies:
        # Obtener solo los reportes de esta empresa
        company_reports = [r for r in reports if r["company_name"] == company["name"]]
        
        for k in range(5):
            report = company_reports[k % len(company_reports)]
            score = 0.45 + (k * 0.12)
            
            signal_types = ["defensive_language", "smoothing", "omission", "guidance_change", "hedge_excess"]
            source_texts = [
                "We remain cautiously optimistic about the upcoming quarter...",
                "While this quarter presented challenges, we see strong momentum...",
                "[No discussion of international operations in the call]",
                "We're updating our guidance to reflect current market conditions...",
                "Subject to various factors, we may potentially consider adjustments..."
            ]
            justifications = [
                "Lenguaje defensivo detectado en respuesta a preguntas sobre márgenes",
                "Suavización de resultados negativos con framing positivo",
                "Omisión notable de métricas que antes se reportaban regularmente",
                "Cambio de guidance sin explicación clara de las razones subyacentes",
                "Exceso de hedging que indica falta de certeza en proyecciones"
            ]
            
            signals.append({
                "id": str(uuid.uuid4()),
                "report_id": report["id"],
                "company_name": company["name"],
                "period": report["period"],
                "signal_type": signal_types[k % len(signal_types)],
                "score": min(score, 0.94),
                "source_text": source_texts[k % len(source_texts)],
                "justification": justifications[k % len(justifications)],
                "validated": k >= 3,
                "decision": "approved" if k >= 3 else None,
                "analyst_note": "Señal confirmada por el analista" if k >= 3 else None,
                "created_at": datetime.now().isoformat()
            })
    
    return {
        "companies": companies,
        "reports": reports,
        "metrics": metrics,
        "discrepancies": discrepancies,
        "signals": signals
    }

class MockResponse:
    def __init__(self, data):
        self.data = data
        self.count = len(data) if isinstance(data, list) else None

class MockQuery:
    def __init__(self, table_name, data_list):
        self.table_name = table_name
        self.data_list = data_list
        self.filters = []
        self.order_by_clause = None
        self.limit_value = None
        self._result_data = data_list
    
    def select(self, *args):
        return self
    
    def eq(self, field, value):
        self._result_data = [d for d in self._result_data if d.get(field) == value]
        return self
    
    def gte(self, field, value):
        self._result_data = [d for d in self._result_data if d.get(field, 0) >= value]
        return self
    
    def order(self, field, desc=False):
        reverse = desc
        if field == "severity":
            # Ordenar por severidad (high, medium, low)
            severity_order = {"high": 0, "medium": 1, "low": 2}
            self._result_data = sorted(self._result_data, key=lambda x: severity_order.get(x.get(field, "low"), 2), reverse=reverse)
        else:
            self._result_data = sorted(self._result_data, key=lambda x: x.get(field, ""), reverse=reverse)
        return self
    
    def limit(self, n):
        self._result_data = self._result_data[:n]
        return self
    
    def execute(self):
        return MockResponse(self._result_data)

class MockClient:
    def __init__(self, mock_data):
        self.mock_data = mock_data
    
    def table(self, name):
        if name in self.mock_data:
            return MockQuery(name, self.mock_data[name])
        return MockQuery(name, [])

def get_client():
    global _client, _mock_data
    
    if config.USE_MOCK:
        if _mock_data is None:
            _mock_data = _generate_mock_data()
        return MockClient(_mock_data)
    
    # Modo real con Supabase
    if _client is None:
        from supabase import create_client
        url = config.SUPABASE_URL
        key = config.SUPABASE_SERVICE_KEY
        if not url or not key or key == "mock-key":
            raise ValueError("SUPABASE_URL y SUPABASE_SERVICE_KEY son requeridas en modo real")
        _client = create_client(url, key)
    return _client

def get_metrics(company_name: str):
    client = get_client()
    if config.USE_MOCK:
        return client.table("metrics").eq("company_name", company_name).execute().data
    else:
        res = client.table("metrics").select("*").eq("company_name", company_name).execute()
        return res.data