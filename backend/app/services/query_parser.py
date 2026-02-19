import re
from dataclasses import dataclass
from datetime import timedelta


@dataclass
class ParsedQuery:
    intent: str | None
    role_hint: str | None
    country_hint: str | None
    time_value: int | None
    time_unit: str | None


INTENT_RULES = {
    "rising_skills": ["rising", "increase", "growing", "fastest growing"],
    "skill_trends": ["trend", "trends", "over time", "monthly"],
    "top_skills": ["top", "most", "best", "leading"],
}


def detect_intent(query: str) -> str | None:
    q = query.lower()
    for intent, words in INTENT_RULES.items():
        if any(w in q for w in words):
            return intent
    if "skills" in q:
        return "top_skills"
    return None


def parse_time_range(query: str) -> tuple[int | None, str | None]:
    q = query.lower()
    match = re.search(r"last\s+(\d+)\s+(day|days|week|weeks|month|months|year|years)", q)
    if match:
        return int(match.group(1)), match.group(2)

    match = re.search(r"(\d+)\s+(day|days|week|weeks|month|months|year|years)", q)
    if match:
        return int(match.group(1)), match.group(2)

    return None, None


def parse_role_country_hints(query: str) -> tuple[str | None, str | None]:
    q = query.strip()
    role_hint = None
    country_hint = None

    boundary = r"(?=\s+(?:for\s+last|last|during|over)\b|\?|$)"
    match = re.search(rf"\bfor\s+(.+?)\s+\bin\s+(.+?){boundary}", q, flags=re.IGNORECASE)
    if match:
        role_hint = match.group(1).strip()
        country_hint = match.group(2).strip()
        return role_hint, country_hint

    role_match = re.search(r"\bfor\s+(.+?)(?:\s+for\b|\s+in\b|\?|$)", q, flags=re.IGNORECASE)
    if role_match:
        role_hint = role_match.group(1).strip()

    country_match = re.search(rf"\bin\s+(.+?){boundary}", q, flags=re.IGNORECASE)
    if country_match:
        country_hint = country_match.group(1).strip()

    return role_hint, country_hint


def parse_query(query: str) -> ParsedQuery:
    intent = detect_intent(query)
    role_hint, country_hint = parse_role_country_hints(query)
    time_value, time_unit = parse_time_range(query)
    return ParsedQuery(
        intent=intent,
        role_hint=role_hint,
        country_hint=country_hint,
        time_value=time_value,
        time_unit=time_unit,
    )


def time_delta_from_value(value: int | None, unit: str | None) -> timedelta:
    if not value or not unit:
        return timedelta(days=180)
    u = unit.lower()
    if "day" in u:
        return timedelta(days=value)
    if "week" in u:
        return timedelta(weeks=value)
    if "year" in u:
        return timedelta(days=365 * value)
    return timedelta(days=30 * value)
