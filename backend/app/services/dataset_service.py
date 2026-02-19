import json
from typing import Any

import pandas as pd
from sqlalchemy import delete
from sqlalchemy.orm import Session

from ..models import Dataset, JobRecord
from ..utils.text_utils import best_match, extract_skills_from_description, normalize_text, smart_split_skills


COLUMN_ALIASES = {
    "role": ["role", "job title", "job_title", "title", "position", "occupation"],
    "country": ["country", "location", "job_country", "region", "market"],
    "skills": ["skills", "skill", "key_skills", "core_skills"],
    "description": ["description", "job_description", "summary", "responsibilities"],
    "date": ["date", "posted_date", "posting_date", "created_at", "publish_date"],
}
INSERT_CHUNK_SIZE = 500


def _clean_columns(columns: list[str]) -> list[str]:
    return [str(c).strip() for c in columns]


def _detect_column(columns: list[str], aliases: list[str]) -> tuple[str | None, float]:
    lowered = [c.lower().strip() for c in columns]

    for alias in aliases:
        if alias in lowered:
            idx = lowered.index(alias)
            return columns[idx], 1.0

    best_col, score = best_match(" ".join(aliases), columns, threshold=0.0)
    return best_col, score


def _parse_date(value: Any):
    if value is None or str(value).strip() == "":
        return None
    try:
        parsed = pd.to_datetime(value, errors="coerce")
        if pd.isna(parsed):
            return None
        return parsed.to_pydatetime().replace(tzinfo=None)
    except Exception:
        return None


def _summary_from_df(df: pd.DataFrame, dataset: Dataset) -> dict[str, Any]:
    role_counts = (
        df["role"].fillna("Unknown").value_counts().head(8).to_dict()
        if "role" in df.columns else {}
    )
    country_counts = (
        df["country"].fillna("Unknown").value_counts().head(8).to_dict()
        if "country" in df.columns else {}
    )

    min_date = None
    max_date = None
    if "posted_date" in df.columns and not df.empty and df["posted_date"].notna().any():
        min_date = str(pd.to_datetime(df["posted_date"]).min().date())
        max_date = str(pd.to_datetime(df["posted_date"]).max().date())

    skills_source = "skills_column" if dataset.has_skills_column else "description_extraction"

    return {
        "dataset_id": dataset.id,
        "filename": dataset.filename,
        "total_jobs": int(len(df)),
        "top_roles": [{"role": k, "count": int(v)} for k, v in role_counts.items()],
        "top_countries": [{"country": k, "count": int(v)} for k, v in country_counts.items()],
        "skills_source": skills_source,
        "date_range": {"start": min_date, "end": max_date},
        "mapping_confidence": round(float(dataset.mapping_confidence), 3),
        "suggested_questions": _suggest_questions(role_counts, country_counts),
    }


def _suggest_questions(role_counts: dict[str, int], country_counts: dict[str, int]) -> list[str]:
    top_role = next(iter(role_counts.keys()), "Data Analyst")
    top_country = next(iter(country_counts.keys()), "USA")
    return [
        f"What are the top skills for {top_role} in {top_country}?",
        f"Show rising skills for {top_role} in {top_country} for last 6 months",
        f"Give me skill trends for {top_role} in {top_country}",
    ]


def ingest_dataset(file_name: str, content: bytes, db: Session) -> dict[str, Any]:
    if file_name.lower().endswith(".csv"):
        df = pd.read_csv(pd.io.common.BytesIO(content))
    elif file_name.lower().endswith((".xlsx", ".xls")):
        df = pd.read_excel(pd.io.common.BytesIO(content))
    else:
        raise ValueError("Unsupported file type. Upload CSV or Excel.")

    if df.empty:
        raise ValueError("Uploaded dataset is empty.")

    columns = _clean_columns(list(df.columns))
    df.columns = columns

    role_col, role_score = _detect_column(columns, COLUMN_ALIASES["role"])
    country_col, country_score = _detect_column(columns, COLUMN_ALIASES["country"])
    skills_col, skills_score = _detect_column(columns, COLUMN_ALIASES["skills"])
    description_col, description_score = _detect_column(columns, COLUMN_ALIASES["description"])
    date_col, date_score = _detect_column(columns, COLUMN_ALIASES["date"])

    if not role_col:
        raise ValueError("Could not identify role column from dataset.")
    if not country_col:
        raise ValueError("Could not identify country/location column from dataset.")

    has_skills = skills_col is not None and skills_score >= 0.45
    has_description = description_col is not None and description_score >= 0.35
    has_date = date_col is not None and date_score >= 0.35

    if not has_skills and not has_description:
        raise ValueError("Neither skills nor description column found. Need one to extract skills.")

    mapping_confidence = (role_score + country_score + max(skills_score, description_score) + date_score) / 4

    db.execute(delete(JobRecord))
    db.execute(delete(Dataset))
    db.commit()

    dataset = Dataset(
        filename=file_name,
        total_jobs=int(len(df)),
        role_column=role_col,
        country_column=country_col,
        skills_column=skills_col if has_skills else None,
        description_column=description_col if has_description else None,
        date_column=date_col if has_date else None,
        has_skills_column=has_skills,
        used_description_extraction=not has_skills,
        has_date_column=has_date,
        mapping_confidence=float(mapping_confidence),
    )
    db.add(dataset)
    db.flush()

    rows = []
    for _, row in df.iterrows():
        role = normalize_text(row.get(role_col))
        country = normalize_text(row.get(country_col))
        description = normalize_text(row.get(description_col)) if has_description else ""

        if has_skills:
            skills = smart_split_skills(row.get(skills_col))
        else:
            skills = extract_skills_from_description(description)

        posted_date = _parse_date(row.get(date_col)) if has_date else None

        rows.append(
            {
                "dataset_id": dataset.id,
                "role": role,
                "country": country,
                "skills_text": ", ".join(skills),
                "description": description,
                "posted_date": posted_date,
                "raw_json": json.dumps({k: str(v) for k, v in row.to_dict().items()}),
            }
        )

    for idx in range(0, len(rows), INSERT_CHUNK_SIZE):
        chunk = rows[idx: idx + INSERT_CHUNK_SIZE]
        db.bulk_insert_mappings(JobRecord, chunk)
        db.commit()

    materialized = pd.DataFrame([
        {
            "role": r["role"],
            "country": r["country"],
            "skills_text": r["skills_text"],
            "posted_date": r["posted_date"],
        }
        for r in rows
    ])
    return _summary_from_df(materialized, dataset)


def latest_dataset(db: Session) -> Dataset | None:
    return db.query(Dataset).order_by(Dataset.id.desc()).first()


def get_dataset_summary(db: Session) -> dict[str, Any] | None:
    dataset = latest_dataset(db)
    if not dataset:
        return None

    jobs = db.query(JobRecord).filter(JobRecord.dataset_id == dataset.id).all()
    df = pd.DataFrame([
        {
            "role": j.role,
            "country": j.country,
            "skills_text": j.skills_text,
            "posted_date": j.posted_date,
        }
        for j in jobs
    ])
    return _summary_from_df(df, dataset)
