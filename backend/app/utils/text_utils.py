import re
from difflib import SequenceMatcher


SKILL_LEXICON = {
    "sql", "python", "excel", "tableau", "power bi", "r", "spark", "hadoop", "aws", "azure",
    "gcp", "machine learning", "deep learning", "statistics", "data analysis", "data visualization",
    "pandas", "numpy", "scikit-learn", "tensorflow", "pytorch", "etl", "airflow", "snowflake",
    "databricks", "docker", "kubernetes", "java", "javascript", "typescript", "react", "node.js",
    "c#", ".net", "go", "rust", "nlp", "llm", "prompt engineering", "git", "agile", "linux",
}


def normalize_text(value: str | None) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def smart_split_skills(value: str | None) -> list[str]:
    text = normalize_text(value)
    if not text:
        return []

    parts = re.split(r"[,;|/]", text)
    cleaned = []
    for part in parts:
        p = normalize_text(part).strip("- ")
        if not p:
            continue
        cleaned.append(p.lower())

    unique = []
    seen = set()
    for skill in cleaned:
        if skill in seen:
            continue
        seen.add(skill)
        unique.append(skill)
    return unique


def extract_skills_from_description(description: str | None) -> list[str]:
    text = normalize_text(description).lower()
    if not text:
        return []

    found = []
    for skill in SKILL_LEXICON:
        if re.search(rf"\b{re.escape(skill)}\b", text):
            found.append(skill)

    return sorted(found)


def similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()


def best_match(query_value: str | None, candidates: list[str], threshold: float = 0.55) -> tuple[str | None, float]:
    if not query_value:
        return None, 0.0

    q = query_value.lower().strip()
    if not q:
        return None, 0.0

    exact = [c for c in candidates if c and c.lower().strip() == q]
    if exact:
        return exact[0], 1.0

    contains = [c for c in candidates if c and c.lower().strip() in q or q in c.lower().strip()]
    if contains:
        best = max(contains, key=lambda c: len(c))
        return best, 0.92

    best_candidate = None
    best_score = 0.0
    for candidate in candidates:
        if not candidate:
            continue
        score = similarity(q, candidate)
        if score > best_score:
            best_score = score
            best_candidate = candidate

    if best_score < threshold:
        return None, best_score
    return best_candidate, best_score
