from fastapi import FastAPI, UploadFile, File, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
import pandas as pd
import io
from pydantic import BaseModel
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

from db.database import engine
from db.models import Base, JobPosting
from db.deps import get_db

from services.upload_service import process_upload
from services.analytics_service import get_top_skills, get_rising_skills, get_skill_trend

app = FastAPI(title="SkillMap - AI Skill Demand Radar API", version="1.0.0")


def read_csv_with_fallbacks(content: bytes) -> pd.DataFrame:
    encodings = ["utf-8", "utf-8-sig", "cp1252", "latin-1"]
    last_error = None

    for enc in encodings:
        try:
            return pd.read_csv(io.BytesIO(content), encoding=enc)
        except UnicodeDecodeError as exc:
            last_error = exc

    raise HTTPException(
        status_code=400,
        detail=f"Unable to decode CSV with supported encodings ({', '.join(encodings)}): {last_error}",
    )


def read_parquet_bytes(content: bytes) -> pd.DataFrame:
    try:
        return pd.read_parquet(io.BytesIO(content))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Unable to decode Parquet: {exc}")


def detect_format_from_url(url: str, content_type: str = "") -> str:
    path = urlparse(url).path.lower()
    ctype = (content_type or "").lower()

    if path.endswith(".csv") or "text/csv" in ctype:
        return "csv"
    if path.endswith(".parquet") or "parquet" in ctype or "application/octet-stream" in ctype:
        return "parquet"
    return ""


def read_dataset_bytes(content: bytes, data_format: str) -> pd.DataFrame:
    if data_format == "csv":
        return read_csv_with_fallbacks(content)
    if data_format == "parquet":
        return read_parquet_bytes(content)
    raise HTTPException(
        status_code=400,
        detail="Unsupported dataset format. Use direct .csv or .parquet file URL.",
    )


class UrlUploadRequest(BaseModel):
    url: str


def fetch_remote_file(url: str, max_bytes: int = 150 * 1024 * 1024) -> tuple[bytes, str, str]:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise HTTPException(status_code=400, detail="URL must start with http:// or https://")
    if parsed.netloc == "huggingface.co" and "/datasets/" in parsed.path and "/resolve/" not in parsed.path:
        raise HTTPException(
            status_code=400,
            detail=(
                "Hugging Face page URL detected. Paste a direct file URL with /resolve/main/... "
                "ending in .csv or .parquet."
            ),
        )

    request = Request(
        url,
        headers={"User-Agent": "SkillMapRadar/1.0 (dataset-fetcher)"},
    )

    try:
        with urlopen(request, timeout=20) as response:
            content_type = response.headers.get("Content-Type", "")
            final_url = response.geturl()
            content = response.read(max_bytes + 1)
    except HTTPError as exc:
        raise HTTPException(status_code=400, detail=f"Failed to fetch URL: HTTP {exc.code}")
    except URLError as exc:
        raise HTTPException(status_code=400, detail=f"Failed to fetch URL: {exc.reason}")

    if len(content) > max_bytes:
        raise HTTPException(status_code=413, detail="Dataset file is too large (max 150 MB).")

    return content, content_type, final_url


Base.metadata.create_all(bind=engine)

# CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    return {"status": "ok", "service": "skillmap-ai-skill-demand-radar-api"}


# ============================================================
# 1) Upload Dataset (REPLACE MODE)
# ============================================================
@app.post("/jobs/upload")
async def upload_jobs(file: UploadFile = File(...), db: Session = Depends(get_db)):
    name = (file.filename or "").lower()
    if not (name.endswith(".csv") or name.endswith(".parquet")):
        return {"error": "Only CSV or Parquet files are supported in SkillMap - AI Skill Demand Radar PoC"}

    content = await file.read()
    data_format = "parquet" if name.endswith(".parquet") else "csv"
    df = read_dataset_bytes(content, data_format)

    deleted_old_rows = db.query(JobPosting).delete()
    db.commit()

    result = process_upload(df, db)

    final_total = db.query(JobPosting).count()

    result["upload_mode"] = "REPLACE"
    result["deleted_old_rows"] = deleted_old_rows
    result["final_total_jobs"] = final_total

    return result


@app.post("/jobs/upload-from-url")
def upload_jobs_from_url(payload: UrlUploadRequest, db: Session = Depends(get_db)):
    content, content_type, final_url = fetch_remote_file(payload.url)
    data_format = detect_format_from_url(final_url, content_type)
    if not data_format:
        raise HTTPException(
            status_code=400,
            detail=(
                "Could not infer dataset format from URL/headers. Use a direct .csv or .parquet URL."
            ),
        )
    df = read_dataset_bytes(content, data_format)

    deleted_old_rows = db.query(JobPosting).delete()
    db.commit()

    result = process_upload(df, db)
    final_total = db.query(JobPosting).count()

    result["upload_mode"] = "REPLACE"
    result["upload_source"] = "URL"
    result["detected_format"] = data_format
    result["source_url"] = payload.url
    result["deleted_old_rows"] = deleted_old_rows
    result["final_total_jobs"] = final_total

    return result


# ============================================================
# 2) Dataset Debug Info
# ============================================================
@app.get("/dataset/info")
def dataset_info(db: Session = Depends(get_db)):
    total_jobs = db.query(JobPosting).count()

    roles = (
        db.query(JobPosting.dataset_role)
        .filter(JobPosting.dataset_role != None)
        .filter(JobPosting.dataset_role != "")
        .distinct()
        .all()
    )

    countries = (
        db.query(JobPosting.country)
        .filter(JobPosting.country != None)
        .filter(JobPosting.country != "")
        .distinct()
        .all()
    )

    roles_list = sorted([r[0] for r in roles])
    countries_list = sorted([c[0] for c in countries])

    return {
        "total_jobs": total_jobs,
        "available_roles": roles_list,
        "available_countries": countries_list,
    }


# ============================================================
# 3) Dropdowns (Dynamic)
# ============================================================
@app.get("/dropdown/roles")
def dropdown_roles(db: Session = Depends(get_db)):
    roles = (
        db.query(JobPosting.dataset_role)
        .filter(JobPosting.dataset_role != None)
        .filter(JobPosting.dataset_role != "")
        .distinct()
        .all()
    )
    roles_list = sorted([r[0] for r in roles])
    return {"roles": roles_list}


@app.get("/dropdown/countries")
def dropdown_countries(db: Session = Depends(get_db)):
    countries = (
        db.query(JobPosting.country)
        .filter(JobPosting.country != None)
        .filter(JobPosting.country != "")
        .distinct()
        .all()
    )
    countries_list = sorted([c[0] for c in countries])
    return {"countries": countries_list}


@app.get("/dropdown/skills")
def dropdown_skills(role: str, country: str, db: Session = Depends(get_db)):
    jobs = (
        db.query(JobPosting.extracted_skills)
        .filter(JobPosting.dataset_role == role)
        .filter(JobPosting.country == country)
        .all()
    )

    skills_set = set()

    for row in jobs:
        if not row[0]:
            continue
        parts = [x.strip() for x in row[0].split(",") if x.strip()]
        for p in parts:
            skills_set.add(p)

    skills_list = sorted(list(skills_set))
    return {"skills": skills_list}


@app.get("/skills/top")
def skills_top(role: str, country: str, k: int = 10, db: Session = Depends(get_db)):
    return {"top_skills": get_top_skills(db, role, country, k)}


@app.get("/skills/rising")
def skills_rising(role: str, country: str, k: int = 10, db: Session = Depends(get_db)):
    return {"rising_skills": get_rising_skills(db, role, country, k)}


@app.get("/trends/skill")
def trends_skill(skill: str, role: str, country: str, db: Session = Depends(get_db)):
    return {"trend": get_skill_trend(db, skill, role, country)}
