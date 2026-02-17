import pandas as pd
from typing import Dict, Any

from sqlalchemy.orm import Session

from db.models import JobPosting
from services.column_mapper import detect_columns
from services.skills_parser import parse_skills_cell, extract_skills_from_text
from services.role_cleaner import clean_role


def safe_str(x, default=""):
    if x is None:
        return default
    s = str(x).strip()
    if s.lower() in ["nan", "none"]:
        return default
    return s


def safe_float(x):
    try:
        if x is None:
            return None
        s = str(x).strip()
        if s.lower() in ["nan", "none", ""]:
            return None
        return float(s)
    except:
        return None


def safe_date(x):
    """
    Supports many formats automatically.
    """
    if x is None:
        return None

    s = str(x).strip()
    if s.lower() in ["nan", "none", ""]:
        return None

    try:
        dt = pd.to_datetime(s, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.date()
    except:
        return None


def process_upload(df: pd.DataFrame, db: Session) -> Dict[str, Any]:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    # Detect columns
    mapping = detect_columns(list(df.columns))

    inserted = 0
    skipped = 0

    # Optional: Clear old data so every upload is fresh
    # Comment this if you want to keep appending
    db.query(JobPosting).delete()
    db.commit()

    for _, row in df.iterrows():
        job_title = safe_str(row.get(mapping["job_title"])) if mapping["job_title"] else ""
        description = safe_str(row.get(mapping["description"])) if mapping["description"] else ""

        country_raw = safe_str(row.get(mapping["country"]), "Unknown") if mapping["country"] else "Unknown"
        country = country_raw if country_raw else "Unknown"

        # FIX: dataset_role mapping key is "dataset_role"
        role_raw = safe_str(row.get(mapping["dataset_role"]), "Unknown") if mapping["dataset_role"] else "Unknown"

        # FIX: Clean role to prevent pollution
        role = clean_role(role_raw)

        company = safe_str(row.get(mapping["company"])) if mapping["company"] else ""
        location = safe_str(row.get(mapping["location"])) if mapping["location"] else ""

        posted_date = safe_date(row.get(mapping["posted_date"])) if mapping["posted_date"] else None

        salary_min = safe_float(row.get(mapping["salary_min"])) if mapping["salary_min"] else None
        salary_max = safe_float(row.get(mapping["salary_max"])) if mapping["salary_max"] else None

        # Skills
        if mapping["skills"]:
            skills_list = parse_skills_cell(row.get(mapping["skills"]))
        else:
            skills_list = extract_skills_from_text(description)

        extracted_skills = ", ".join(sorted(set([s.strip() for s in skills_list if s.strip()])))

        job = JobPosting(
            job_title=job_title,
            company=company,
            location=location,
            country=country,
            dataset_role=role,
            role_category="",
            description=description,
            posted_date=posted_date,
            salary_min=salary_min,
            salary_max=salary_max,
            extracted_skills=extracted_skills,
        )

        db.add(job)
        inserted += 1

    db.commit()

    return {
        "message": "Upload processed successfully",
        "rows_received": int(len(df)),
        "rows_inserted": inserted,
        "rows_skipped": skipped,
        "detected_columns": mapping,
    }