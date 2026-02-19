import json
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
from sqlalchemy.orm import Session

from ..models import AgentRun, JobRecord
from ..utils.text_utils import best_match, smart_split_skills
from .dataset_service import latest_dataset
from .query_parser import parse_query, time_delta_from_value


def _build_plan(intent: str, has_skills: bool, has_date: bool) -> list[str]:
    plan = [
        "Parse natural language query into intent and filters",
        "Auto-match role and country against dataset values with fuzzy correction",
    ]
    if has_skills:
        plan.append("Use skills column for skill aggregation")
    else:
        plan.append("Extract skills from description-derived values")

    if intent in {"rising_skills", "skill_trends"}:
        if has_date:
            plan.append("Use date windowing for trend calculations")
        else:
            plan.append("Date missing: fallback to top skills with warning")

    plan.append("Compute result, confidence score, warnings, and persist run logs")
    return plan


def _load_df(db: Session, dataset_id: int) -> pd.DataFrame:
    jobs = db.query(JobRecord).filter(JobRecord.dataset_id == dataset_id).all()
    return pd.DataFrame([
        {
            "role": j.role,
            "country": j.country,
            "skills_text": j.skills_text,
            "posted_date": j.posted_date,
            "description": j.description,
        }
        for j in jobs
    ])


def _parse_skills_column(df: pd.DataFrame) -> pd.DataFrame:
    skill_rows = []
    for _, row in df.iterrows():
        skills = smart_split_skills(row.get("skills_text"))
        if not skills:
            continue
        for s in skills:
            skill_rows.append(
                {
                    "role": row.get("role"),
                    "country": row.get("country"),
                    "posted_date": row.get("posted_date"),
                    "skill": s,
                }
            )
    return pd.DataFrame(skill_rows)


def _top_skills(skill_df: pd.DataFrame) -> list[dict[str, Any]]:
    if skill_df.empty:
        return []
    counts = skill_df["skill"].value_counts().head(15)
    return [{"skill": k, "count": int(v)} for k, v in counts.items()]


def _rising_skills(skill_df: pd.DataFrame, window_delta: timedelta) -> list[dict[str, Any]]:
    if skill_df.empty or skill_df["posted_date"].isna().all():
        return []

    skill_df = skill_df.copy()
    skill_df["posted_date"] = pd.to_datetime(skill_df["posted_date"], errors="coerce")
    skill_df = skill_df.dropna(subset=["posted_date"])
    if skill_df.empty:
        return []

    end = skill_df["posted_date"].max()
    current_start = end - window_delta
    previous_start = current_start - window_delta

    current_df = skill_df[(skill_df["posted_date"] > current_start) & (skill_df["posted_date"] <= end)]
    previous_df = skill_df[(skill_df["posted_date"] > previous_start) & (skill_df["posted_date"] <= current_start)]

    current_counts = current_df["skill"].value_counts()
    previous_counts = previous_df["skill"].value_counts()

    results = []
    all_skills = set(current_counts.index).union(set(previous_counts.index))
    for skill in all_skills:
        cur = int(current_counts.get(skill, 0))
        prev = int(previous_counts.get(skill, 0))
        growth = cur - prev
        pct = None if prev == 0 else round((growth / prev) * 100, 2)
        results.append(
            {
                "skill": skill,
                "current_count": cur,
                "previous_count": prev,
                "growth": growth,
                "growth_percent": pct,
            }
        )

    results.sort(key=lambda x: (x["growth"], x["current_count"]), reverse=True)
    return results[:15]


def _skill_trends(skill_df: pd.DataFrame) -> dict[str, Any]:
    if skill_df.empty or skill_df["posted_date"].isna().all():
        return {"series": []}

    temp = skill_df.copy()
    temp["posted_date"] = pd.to_datetime(temp["posted_date"], errors="coerce")
    temp = temp.dropna(subset=["posted_date"])
    if temp.empty:
        return {"series": []}

    temp["month"] = temp["posted_date"].dt.to_period("M").astype(str)
    top_skill_names = temp["skill"].value_counts().head(8).index.tolist()
    temp = temp[temp["skill"].isin(top_skill_names)]

    grouped = temp.groupby(["month", "skill"]).size().reset_index(name="count")
    series = []
    for skill in top_skill_names:
        points = grouped[grouped["skill"] == skill][["month", "count"]]
        series.append(
            {
                "skill": skill,
                "points": [{"month": r["month"], "count": int(r["count"])} for _, r in points.iterrows()],
            }
        )
    return {"series": series}


def _confidence(intent: str | None, role_score: float, country_score: float, warnings: list[str]) -> float:
    score = 0.5
    if intent:
        score += 0.2
    if role_score > 0:
        score += min(role_score, 1.0) * 0.15
    if country_score > 0:
        score += min(country_score, 1.0) * 0.15
    if warnings:
        score -= min(0.2, 0.05 * len(warnings))
    return round(max(0.1, min(0.99, score)), 3)


def run_agent_query(query: str, db: Session) -> dict[str, Any]:
    dataset = latest_dataset(db)
    if not dataset:
        return {
            "status": "error",
            "message": "No dataset uploaded yet.",
            "execution_plan": [],
            "parsed_intent": None,
            "parsed_filters": {},
            "result": None,
            "confidence": 0.0,
            "warnings": ["Upload a dataset first."],
            "clarification_questions": [],
        }

    started_at = datetime.utcnow()
    parsed = parse_query(query)
    df = _load_df(db, dataset.id)
    if df.empty:
        return {
            "status": "error",
            "message": "Dataset is available but contains no records.",
            "execution_plan": [],
            "parsed_intent": None,
            "parsed_filters": {},
            "result": None,
            "confidence": 0.0,
            "warnings": ["Upload a non-empty dataset."],
            "clarification_questions": [],
        }

    roles = sorted([r for r in df["role"].dropna().unique().tolist() if str(r).strip()])
    countries = sorted([c for c in df["country"].dropna().unique().tolist() if str(c).strip()])

    role_match, role_score = best_match(parsed.role_hint, roles, threshold=0.4)
    country_match, country_score = best_match(parsed.country_hint, countries, threshold=0.4)

    clarification_questions = []
    if not parsed.intent:
        clarification_questions.append("Do you want top skills, rising skills, or trends?")
    if not role_match:
        clarification_questions.append("Which job role should I analyze?")
    if not country_match and len(countries) > 1:
        clarification_questions.append("Which country should I filter by?")

    plan = _build_plan(parsed.intent or "unknown", dataset.has_skills_column, dataset.has_date_column)

    if clarification_questions:
        confidence = _confidence(parsed.intent, role_score, country_score, ["Incomplete query"])
        _log_run(
            db=db,
            dataset_id=dataset.id,
            query=query,
            parsed_intent=parsed.intent,
            parsed_filters={"role": role_match, "country": country_match},
            plan=plan,
            output={"clarification_questions": clarification_questions},
            status="clarification_needed",
            confidence=confidence,
            warnings=["Incomplete query. Clarification requested."],
            started_at=started_at,
        )
        return {
            "status": "clarification_needed",
            "message": "Need a bit more detail before execution.",
            "execution_plan": plan,
            "parsed_intent": parsed.intent,
            "parsed_filters": {
                "role": role_match,
                "country": country_match,
                "time": {"value": parsed.time_value, "unit": parsed.time_unit},
            },
            "result": None,
            "confidence": confidence,
            "warnings": ["Incomplete query. Clarification requested."],
            "clarification_questions": clarification_questions,
        }

    warnings = []
    skill_df = _parse_skills_column(df)
    filtered = skill_df[(skill_df["role"] == role_match) & (skill_df["country"] == country_match)]

    if filtered.empty:
        filtered = skill_df[skill_df["role"] == role_match]
        warnings.append("No exact country match after filtering; expanded to role-level results.")

    intent = parsed.intent or "top_skills"
    result: dict[str, Any]

    if intent == "top_skills":
        result = {
            "intent": "top_skills",
            "items": _top_skills(filtered),
        }
    elif intent == "rising_skills":
        if dataset.has_date_column:
            delta = time_delta_from_value(parsed.time_value or 6, parsed.time_unit or "months")
            result = {
                "intent": "rising_skills",
                "items": _rising_skills(filtered, delta),
            }
        else:
            warnings.append("Date column missing; fallback to top skills.")
            result = {
                "intent": "top_skills_fallback",
                "items": _top_skills(filtered),
            }
    else:
        if dataset.has_date_column:
            result = {
                "intent": "skill_trends",
                "data": _skill_trends(filtered),
            }
        else:
            warnings.append("Date column missing; fallback to top skills.")
            result = {
                "intent": "top_skills_fallback",
                "items": _top_skills(filtered),
            }

    confidence = _confidence(intent, role_score, country_score, warnings)
    parsed_filters = {
        "role_requested": parsed.role_hint,
        "role_matched": role_match,
        "role_match_score": round(role_score, 3),
        "country_requested": parsed.country_hint,
        "country_matched": country_match,
        "country_match_score": round(country_score, 3),
        "time": {"value": parsed.time_value, "unit": parsed.time_unit},
    }

    _log_run(
        db=db,
        dataset_id=dataset.id,
        query=query,
        parsed_intent=intent,
        parsed_filters=parsed_filters,
        plan=plan,
        output=result,
        status="completed",
        confidence=confidence,
        warnings=warnings,
        started_at=started_at,
    )

    return {
        "status": "completed",
        "message": "Agent execution completed.",
        "execution_plan": plan,
        "parsed_intent": intent,
        "parsed_filters": parsed_filters,
        "result": result,
        "confidence": confidence,
        "warnings": warnings,
        "clarification_questions": [],
    }


def _log_run(
    db: Session,
    dataset_id: int | None,
    query: str,
    parsed_intent: str | None,
    parsed_filters: dict[str, Any],
    plan: list[str],
    output: dict[str, Any],
    status: str,
    confidence: float,
    warnings: list[str],
    started_at: datetime,
) -> None:
    run = AgentRun(
        dataset_id=dataset_id,
        query=query,
        parsed_intent=json.dumps(parsed_intent),
        parsed_filters=json.dumps(parsed_filters),
        execution_plan=json.dumps(plan),
        output_summary=json.dumps(output),
        status=status,
        confidence=confidence,
        warnings=json.dumps(warnings),
        started_at=started_at,
        finished_at=datetime.utcnow(),
    )
    db.add(run)
    db.commit()
