import re
from typing import Dict, List, Optional


def normalize_col(col: str) -> str:
    col = str(col).strip().lower()
    col = re.sub(r"[\s\-_]+", "", col)   # remove spaces, underscores, hyphens
    col = re.sub(r"[^a-z0-9]", "", col)  # remove special chars
    return col


def find_best_column(
    columns: List[str],
    candidates: List[str],
    allow_partial: bool = True
) -> Optional[str]:
    """
    Finds the best matching column from a list of candidates.
    - Exact normalized matches first
    - Optional partial matching fallback
    """

    norm_map = {normalize_col(c): c for c in columns}

    # 1) Exact match
    for cand in candidates:
        nc = normalize_col(cand)
        if nc in norm_map:
            return norm_map[nc]

    # 2) Partial match (optional)
    if not allow_partial:
        return None

    for col in columns:
        ncol = normalize_col(col)
        for cand in candidates:
            if normalize_col(cand) in ncol:
                return col

    return None


def detect_columns(df_columns: List[str]) -> Dict[str, Optional[str]]:
    return {
        "job_title": find_best_column(df_columns, ["jobtitle", "job title", "title", "position"]),
        "description": find_best_column(df_columns, ["jobdescription", "job description", "description", "details"]),
        "country": find_best_column(df_columns, ["country", "nation"]),
        "dataset_role": find_best_column(df_columns, ["category", "categories", "industry", "department", "role"]),
        "company": find_best_column(df_columns, ["company", "employer", "organization"]),
        "location": find_best_column(df_columns, ["location", "city", "state"]),
        "posted_date": find_best_column(df_columns, ["posteddate", "post date", "date", "createdat"]),
        "skills": find_best_column(df_columns, ["skills", "skill", "requiredskills", "keyskills", "key skills"]),
        "salary_min": find_best_column(df_columns, ["salarymin", "minsalary", "salary_min"]),
        "salary_max": find_best_column(df_columns, ["salarymax", "maxsalary", "salary_max"]),
    }