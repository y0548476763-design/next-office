import os
from fastapi.testclient import TestClient
from main import app

def test_twilio_voice_returns_twiml(monkeypatch):
    monkeypatch.setenv("PUBLIC_BASE_URL", "https://example.com")
    client = TestClient(app)
    r = client.post("/twilio/voice")
    assert r.status_code == 200
    body = r.text
    assert "<Response>" in body and "<Connect><Stream url=\"wss://example.com/twilio/media\"/></Connect>" in body
