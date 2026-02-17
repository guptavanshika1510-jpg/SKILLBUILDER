import ast
import re
from typing import List


SKILL_DICTIONARY = [
    "python", "sql", "aws", "azure", "gcp", "docker", "kubernetes",
    "react", "fastapi", "django", "flask", "node", "express",
    "java", "c++", "c", "javascript", "typescript",
    "pandas", "numpy", "scikit-learn", "tensorflow", "pytorch",
    "power bi", "tableau", "excel", "spark", "hadoop",
    "mongodb", "postgresql", "mysql", "sqlite",
    "git", "linux", "rest", "graphql"
]


def parse_skills_cell(value) -> List[str]:
    """
    Handles:
    - "Python, SQL, AWS"
    - "Python|SQL|AWS"
    - "Python;SQL"
    - "['Python','SQL']"
    """

    if value is None:
        return []

    s = str(value).strip()
    if not s or s.lower() in ["nan", "none"]:
        return []

    # if looks like python list
    if s.startswith("[") and s.endswith("]"):
        try:
            arr = ast.literal_eval(s)
            if isinstance(arr, list):
                return [str(x).strip() for x in arr if str(x).strip()]
        except:
            pass

    # split by common delimiters
    parts = re.split(r"[,\|;]+", s)
    return [p.strip() for p in parts if p.strip()]


def extract_skills_from_text(text: str) -> List[str]:
    """
    Dictionary-based skill extraction from description.
    """
    if not text:
        return []

    t = text.lower()
    found = []

    for skill in SKILL_DICTIONARY:
        if skill in t:
            found.append(skill.title())

    return sorted(list(set(found)))