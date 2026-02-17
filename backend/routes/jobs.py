from fastapi import APIRouter, UploadFile, File, Depends, HTTPException
from sqlalchemy.orm import Session
import pandas as pd
import io

from db.models import JobPosting
from db.deps import get_db
from services.upload_service import process_upload

router = APIRouter(prefix="/jobs", tags=["Jobs"])


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


@router.post("/upload")
async def upload_jobs(file: UploadFile = File(...), db: Session = Depends(get_db)):
    """
    Upload CSV, auto-detect columns, extract skills, store in SQLite.
    This endpoint REPLACES the old dataset every time.
    """

    if not file.filename.lower().endswith(".csv"):
        return {"error": "Only CSV files are supported."}

    content = await file.read()

    # Read CSV
    df = read_csv_with_fallbacks(content)

    # âœ… Replace mode: delete old dataset
    db.query(JobPosting).delete()
    db.commit()

    # Insert new dataset
    result = process_upload(df, db)

    return result
