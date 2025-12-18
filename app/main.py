from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field
import sqlite3, os, time, logging
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

def setup_tracing(app):
    # Enable/disable via env (useful for local/dev)
    if os.getenv("OTEL_TRACING_ENABLED", "true").lower() != "true":
        return

    service_name = os.getenv("OTEL_SERVICE_NAME", "devops-fastapi-project")
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318/v1/traces")

    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(provider)

    exporter = OTLPSpanExporter(endpoint=endpoint)
    provider.add_span_processor(BatchSpanProcessor(exporter))

    FastAPIInstrumentor.instrument_app(app)
APP_NAME = "devops-fastapi"
DB_PATH = os.getenv("DB_PATH", "data/app.db")

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(APP_NAME)

REQ_COUNT = Counter("http_requests_total", "Total HTTP requests", ["method", "path", "status"])
REQ_LAT = Histogram("http_request_duration_seconds", "HTTP request duration (s)", ["method", "path"])

app = FastAPI(title=APP_NAME)

class TaskCreate(BaseModel):
    title: str = Field(min_length=1, max_length=200)

def connect():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con

def init_db():
    con = connect()
    con.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          title TEXT NOT NULL,
          done INTEGER NOT NULL DEFAULT 0,
          created_at TEXT NOT NULL
        )
    """)
    con.commit()
    con.close()

@app.on_event("startup")
def startup():
    init_db()

@app.middleware("http")
async def observability_mw(request: Request, call_next):
    start = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        duration = time.perf_counter() - start
        REQ_COUNT.labels(request.method, request.url.path, "500").inc()
        REQ_LAT.labels(request.method, request.url.path).observe(duration)
        log.info(f'{{"event":"request","method":"{request.method}","path":"{request.url.path}","status":500,"duration_ms":{int(duration*1000)}}}')
        raise
    duration = time.perf_counter() - start
    REQ_COUNT.labels(request.method, request.url.path, str(response.status_code)).inc()
    REQ_LAT.labels(request.method, request.url.path).observe(duration)
    log.info(f'{{"event":"request","method":"{request.method}","path":"{request.url.path}","status":{response.status_code},"duration_ms":{int(duration*1000)}}}')
    return response

@app.get("/health")
def health():
    con = connect()
    con.execute("SELECT 1")
    con.close()
    return {"status": "ok", "db": "ok"}

@app.get("/tasks")
def list_tasks():
    con = connect()
    rows = con.execute("SELECT id,title,done,created_at FROM tasks ORDER BY id DESC").fetchall()
    con.close()
    return [dict(r) for r in rows]

@app.post("/tasks", status_code=201)
def create_task(payload: TaskCreate):
    con = connect()
    cur = con.execute(
        "INSERT INTO tasks(title, done, created_at) VALUES(?,?,datetime('now'))",
        (payload.title, 0),
    )
    con.commit()
    row = con.execute("SELECT id,title,done,created_at FROM tasks WHERE id=?", (cur.lastrowid,)).fetchone()
    con.close()
    return dict(row)

@app.patch("/tasks/{task_id}/done")
def mark_done(task_id: int):
    con = connect()
    cur = con.execute("UPDATE tasks SET done=1 WHERE id=?", (task_id,))
    con.commit()
    if cur.rowcount == 0:
        con.close()
        raise HTTPException(status_code=404, detail="Task not found")
    row = con.execute("SELECT id,title,done,created_at FROM tasks WHERE id=?", (task_id,)).fetchone()
    con.close()
    return dict(row)

@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
app = FastAPI()
setup_tracing(app)
