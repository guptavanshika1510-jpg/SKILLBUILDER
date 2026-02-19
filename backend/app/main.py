from pathlib import Path
import logging

from fastapi import Depends, FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session

from .database import Base, database_url_for_log, engine, get_db
from .models import AgentRun
from .schemas import AgentQueryRequest, AgentQueryResponse, AgentRunOut, DatasetSummary
from .services.agent_service import run_agent_query
from .services.dataset_service import get_dataset_summary, ingest_dataset

Base.metadata.create_all(bind=engine)

app = FastAPI(title="SkillMap Agentic API", version="1.0.0")
logger = logging.getLogger("uvicorn.error")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def log_database_url() -> None:
    logger.info("Database connection URL: %s", database_url_for_log())


@app.get("/api/health")
def health_check():
    return {"status": "ok"}


@app.post("/api/upload", response_model=DatasetSummary)
async def upload_dataset(file: UploadFile = File(...), db: Session = Depends(get_db)):
    data = await file.read()
    try:
        summary = ingest_dataset(file.filename or "dataset.csv", data, db)
        return summary
    except ValueError as ex:
        raise HTTPException(status_code=400, detail=str(ex)) from ex
    except Exception as ex:
        raise HTTPException(status_code=500, detail=f"Upload failed: {ex}") from ex


@app.get("/api/dataset/summary", response_model=DatasetSummary)
def dataset_summary(db: Session = Depends(get_db)):
    summary = get_dataset_summary(db)
    if not summary:
        raise HTTPException(status_code=404, detail="No dataset uploaded yet.")
    return summary


@app.post("/api/agent/query", response_model=AgentQueryResponse)
def agent_query(request: AgentQueryRequest, db: Session = Depends(get_db)):
    return run_agent_query(request.query, db)


@app.get("/api/agent/runs", response_model=list[AgentRunOut])
def list_agent_runs(db: Session = Depends(get_db)):
    return db.query(AgentRun).order_by(AgentRun.id.desc()).limit(25).all()


BASE_DIR = Path(__file__).resolve().parents[2]
FRONTEND_DIR = BASE_DIR / "frontend"
if FRONTEND_DIR.exists():
    app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")


@app.get("/")
def root_page():
    index = FRONTEND_DIR / "index.html"
    if index.exists():
        return FileResponse(index)
    return {"message": "Frontend not found. Serve frontend/index.html manually."}
