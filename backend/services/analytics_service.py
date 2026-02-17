from collections import Counter, defaultdict
from datetime import date
from typing import Dict, List, Tuple
import pandas as pd

from sqlalchemy.orm import Session
from db.models import JobPosting


def _parse_skills(skills_text: str) -> List[str]:
    if not skills_text:
        return []
    parts = [x.strip() for x in skills_text.split(",") if x.strip()]
    return parts


def get_top_skills(db: Session, role: str, country: str, k: int = 10) -> List[Dict]:
    jobs = (
        db.query(JobPosting.extracted_skills)
        .filter(JobPosting.dataset_role == role)
        .filter(JobPosting.country == country)
        .all()
    )

    counter = Counter()

    for row in jobs:
        for skill in _parse_skills(row[0]):
            counter[skill] += 1

    top = counter.most_common(k)
    return [{"skill": s, "count": c} for s, c in top]


def get_rising_skills(db: Session, role: str, country: str, k: int = 10) -> List[Dict]:
    """
    Rising = count in last 3 months - count in previous 3 months
    """

    jobs = (
        db.query(JobPosting.posted_date, JobPosting.extracted_skills)
        .filter(JobPosting.dataset_role == role)
        .filter(JobPosting.country == country)
        .filter(JobPosting.posted_date != None)
        .all()
    )

    if not jobs:
        return []

    # Convert to dataframe for easy time filtering
    df = pd.DataFrame(jobs, columns=["posted_date", "skills"])

    df["posted_date"] = pd.to_datetime(df["posted_date"], errors="coerce")
    df = df.dropna(subset=["posted_date"])

    if df.empty:
        return []

    max_date = df["posted_date"].max()

    last_3_start = max_date - pd.DateOffset(months=3)
    prev_3_start = max_date - pd.DateOffset(months=6)

    last_df = df[df["posted_date"] >= last_3_start]
    prev_df = df[(df["posted_date"] >= prev_3_start) & (df["posted_date"] < last_3_start)]

    last_counter = Counter()
    prev_counter = Counter()

    for _, r in last_df.iterrows():
        for s in _parse_skills(r["skills"]):
            last_counter[s] += 1

    for _, r in prev_df.iterrows():
        for s in _parse_skills(r["skills"]):
            prev_counter[s] += 1

    # rising score
    all_skills = set(last_counter.keys()) | set(prev_counter.keys())
    rising = []

    for s in all_skills:
        diff = last_counter[s] - prev_counter[s]
        if diff > 0:
            rising.append({
                "skill": s,
                "last_3_months": last_counter[s],
                "prev_3_months": prev_counter[s],
                "growth": diff
            })

    rising.sort(key=lambda x: x["growth"], reverse=True)
    return rising[:k]


def get_skill_trend(db: Session, skill: str, role: str, country: str) -> List[Dict]:
    """
    Returns month-wise counts for a skill.
    """

    jobs = (
        db.query(JobPosting.posted_date, JobPosting.extracted_skills)
        .filter(JobPosting.dataset_role == role)
        .filter(JobPosting.country == country)
        .filter(JobPosting.posted_date != None)
        .all()
    )

    if not jobs:
        return []

    df = pd.DataFrame(jobs, columns=["posted_date", "skills"])
    df["posted_date"] = pd.to_datetime(df["posted_date"], errors="coerce")
    df = df.dropna(subset=["posted_date"])

    if df.empty:
        return []

    skill_lower = skill.strip().lower()

    def has_skill(skills_text):
        skills = [x.strip().lower() for x in _parse_skills(skills_text)]
        return skill_lower in skills

    df = df[df["skills"].apply(has_skill)]

    if df.empty:
        return []

    df["month"] = df["posted_date"].dt.to_period("M").astype(str)
    trend = df.groupby("month").size().reset_index(name="count")

    # sort month
    trend = trend.sort_values("month")

    return trend.to_dict(orient="records")