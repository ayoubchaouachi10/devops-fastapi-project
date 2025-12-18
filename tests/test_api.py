from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

def test_health():
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"

def test_create_and_list_tasks():
    r = client.post("/tasks", json={"title": "write report"})
    assert r.status_code == 201
    task = r.json()
    assert task["title"] == "write report"
    r2 = client.get("/tasks")
    assert r2.status_code == 200
    assert any(t["id"] == task["id"] for t in r2.json())

def test_mark_done_404():
    r = client.patch("/tasks/999999/done")
    assert r.status_code == 404
