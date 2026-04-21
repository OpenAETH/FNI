# api/db/mongo_client.py
import config
from datetime import datetime

_client = None

class MockMongoDB:
    def __init__(self):
        self.collections = {}
    
    def __getitem__(self, name):
        if name not in self.collections:
            self.collections[name] = MockCollection(name)
        return self.collections[name]
    
    def command(self, cmd):
        return {"ok": 1}

class MockCollection:
    def __init__(self, name):
        self.name = name
        self.documents = []
    
    def find_one(self, query):
        for doc in self.documents:
            match = True
            for k, v in query.items():
                if doc.get(k) != v:
                    match = False
                    break
            if match:
                return doc
        # Retornar un documento mock si no existe
        return {
            "file_hash": query.get("file_hash", "mock"),
            "clean_text": "This is a mock transcript. The company performed well this quarter with revenue growth of 15%. We remain cautiously optimistic about future performance. Some headwinds exist but we are confident in our strategy."
        }
    
    def insert_one(self, doc):
        self.documents.append(doc)
        return MockInsertResult()

class MockInsertResult:
    @property
    def inserted_id(self):
        return "mock_id"

def get_db():
    if config.USE_MOCK:
        return MockMongoDB()
    
    global _client
    if _client is None:
        from pymongo import MongoClient
        uri = config.MONGO_URI
        if not uri:
            raise ValueError("MONGO_URI es requerida")
        _client = MongoClient(uri)
    return _client[config.MONGO_DB_NAME]

def ping():
    try:
        get_db().command('ping')
        return True
    except:
        return False

def log_analyst_feedback(**kwargs):
    if config.USE_MOCK:
        print(f"[MOCK] Feedback logged: {kwargs}")
        return
    
    db = get_db()
    db["analyst_feedback"].insert_one({
        **kwargs,
        "timestamp": datetime.utcnow()
    })

def save_signal_event(**kwargs):
    if config.USE_MOCK:
        print(f"[MOCK] Signal event saved: {kwargs}")
        return
    
    db = get_db()
    db["signal_events"].insert_one({
        **kwargs,
        "timestamp": datetime.utcnow()
    })