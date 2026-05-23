"""
Utility for extracting competition start dates from PDFs and calendar.json.
"""

import json
import re
from pathlib import Path
from typing import List, Optional

import pdfplumber

_MONTH_NAMES = (
    "january|february|march|april|may|june|july|august"
    "|september|october|november|december"
    "|jan|feb|mar|apr|jun|jul|aug|sep|oct|nov|dec"
)

_MONTH_TO_NUM = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}

# "5 May 2025" / "05 May 2025" — also catches start of same-month range "5-7 May 2025"
_DATE_RE = re.compile(
    r"\b(\d{1,2})\s+(" + _MONTH_NAMES + r")\s+(\d{4})\b",
    re.IGNORECASE,
)

# DD/MM/YYYY (Australian format)
_SLASH_RE = re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b")

# Collapsed spaced-PDF format: "Mar15-16,2025" or "March15,2025"
# (produced after collapsing "M a r 1 5 - 1 6 , 2 0 2 5")
_COLLAPSED_DATE_RE = re.compile(
    r"\b(" + _MONTH_NAMES + r")(\d{1,2})(?:-\d{1,2})?[,\s]*(\d{4})\b",
    re.IGNORECASE,
)


def _collapse_spaced_chars(text: str) -> str:
    """Collapse 'M a r 1 5' PDF spaced-character encoding into 'Mar15'."""
    def _join(m):
        tokens = m.group(0).split(" ")
        if all(len(t) == 1 for t in tokens):
            return "".join(tokens)
        return m.group(0)
    return re.sub(r"\S(?: \S)+", _join, text)


def _to_iso(day: str, month_str: str, year: str) -> Optional[str]:
    m = _MONTH_TO_NUM.get(month_str.lower())
    if not m:
        return None
    d, y = int(day), int(year)
    if not (1 <= d <= 31 and 2015 <= y <= 2035):
        return None
    return f"{y:04d}-{m:02d}-{d:02d}"


def extract_date_from_pdf(pdf_path) -> Optional[str]:
    """Return the earliest date (YYYY-MM-DD) found in the first 2 pages, or None."""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            text = "\n".join(
                (page.extract_text() or "") for page in pdf.pages[:2]
            )
    except Exception:
        return None

    dates = []

    for m in _DATE_RE.finditer(text):
        iso = _to_iso(m.group(1), m.group(2), m.group(3))
        if iso:
            dates.append(iso)

    for m in _SLASH_RE.finditer(text):
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= d <= 31 and 1 <= mo <= 12 and 2015 <= y <= 2035:
            dates.append(f"{y:04d}-{mo:02d}-{d:02d}")

    # Also try collapsed spaced-character text (e.g. "M a r 1 5 - 1 6 , 2 0 2 5")
    collapsed = _collapse_spaced_chars(text)
    if collapsed != text:
        for m in _COLLAPSED_DATE_RE.finditer(collapsed):
            iso = _to_iso(m.group(2), m.group(1), m.group(3))
            if iso:
                dates.append(iso)

    return min(dates) if dates else None


def lookup_calendar_date(comp_name: str, calendar_path: str = "data/calendar.json") -> Optional[str]:
    """Case-insensitive name match against calendar.json; returns date_start or None."""
    try:
        with open(calendar_path, encoding="utf-8") as f:
            cal = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None
    needle = comp_name.lower().strip()
    for entry in cal.get("events", []):
        if entry.get("name", "").lower().strip() == needle:
            return entry.get("date_start")
    return None


def resolve_comp_date(comp_name: str, pdfs: list, calendar_path: str = "data/calendar.json") -> Optional[str]:
    """Try PDFs first, then calendar.json. Returns YYYY-MM-DD or None."""
    for pdf in pdfs:
        date = extract_date_from_pdf(pdf)
        if date:
            return date
    return lookup_calendar_date(comp_name, calendar_path)
