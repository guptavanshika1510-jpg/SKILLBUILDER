import re

def clean_role(value: str) -> str:
    if value is None:
        return "Unknown"

    s = str(value).strip()

    if not s:
        return "Unknown"

    # bad patterns
    if "www." in s.lower():
        return "Unknown"

    if "*" in s:
        return "Unknown"

    # too long
    if len(s) > 60:
        return "Unknown"

    # too many words (likely paragraph)
    words = s.split()
    if len(words) > 10:
        return "Unknown"

    # cleanup multiple spaces
    s = re.sub(r"\s+", " ", s).strip()

    return s