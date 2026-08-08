"""
scrape_calendar.py — Scrape the GymVic Timely calendar into data/calendar.json.

Usage:
    playwright install chromium          (first time only)
    python scrape_calendar.py            # fetch live page → data/calendar.json
    python scrape_calendar.py --dump     # also save rendered HTML to data/calendar_dump.html
"""

import argparse
import json
import re
from html import unescape
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

TIMELY_URL  = "https://events.timely.fun/1cr4n8dt/"
DUMP_FILE   = Path("data/calendar_dump.html")
OUTPUT_FILE = Path("data/calendar.json")

MONTH_MAP = {
    "jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,
    "jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12,
}


def _parse_date_str(raw, fallback_year=None):
    """Parse a date string into YYYY-MM-DD. Returns None if unparseable."""
    raw = re.sub(r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\s*", "", raw, flags=re.IGNORECASE).strip()
    # DD/MM/YYYY or D/M/YYYY
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", raw)
    if m:
        return f"{m.group(3)}-{int(m.group(2)):02d}-{int(m.group(1)):02d}"
    # DD Month YYYY
    m = re.match(r"(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})", raw)
    if m:
        mon = MONTH_MAP.get(m.group(2).lower()[:3])
        if mon:
            return f"{m.group(3)}-{mon:02d}-{int(m.group(1)):02d}"
    # DD Month (no year — use fallback_year)
    m = re.match(r"(\d{1,2})\s+([A-Za-z]+)", raw)
    if m and fallback_year:
        mon = MONTH_MAP.get(m.group(2).lower()[:3])
        if mon:
            return f"{fallback_year}-{mon:02d}-{int(m.group(1)):02d}"
    return None


def _parse_date_range(excerpt_text, id_date_str):
    """
    Extract start and end dates from an event excerpt.
    id_date_str is YYYYMMDD from the element ID — used as the start date fallback.
    Returns (date_start, date_end) as YYYY-MM-DD strings.
    """
    year = id_date_str[:4]

    # Start date from element ID is authoritative
    date_start = f"{year}-{id_date_str[4:6]}-{id_date_str[6:8]}"

    # Look for "Date: X - Y" pattern in excerpt
    m = re.search(r"Date:\s*(.+?)(?:\n|Levels|LAT|Entries|$)", excerpt_text, re.IGNORECASE | re.DOTALL)
    if not m:
        return date_start, date_start

    date_part = m.group(1).strip()

    # Multi-day: "16/5/2026 - 17/5/2026"
    m2 = re.match(r"(.+?)\s*[-–]\s*(.+)", date_part)
    if m2:
        end = _parse_date_str(m2.group(2).strip(), fallback_year=year)
        if end:
            # If end month/day only (e.g. "Sunday 19 July"), attach same year
            return date_start, end

    # Single day — just use start
    return date_start, date_start


def _detect_sport(name, excerpt):
    text = (name + " " + excerpt).upper()
    has_wag = "WAG" in text or "WOMEN" in text
    has_mag = "MAG" in text or "MEN'S ARTISTIC" in text or "MEN'S GYM" in text
    if has_wag and has_mag:
        return "ALL"
    if has_wag:
        return "WAG"
    if has_mag:
        return "MAG"
    return "ALL"


def parse_events(html):
    """Extract events from rendered Timely HTML. Returns list of dicts."""
    # Each event is a div.timely-stream-event with an id like timely-event-list-{id}-{YYYYMMDD}000000
    pattern = re.compile(
        r'<div[^>]*class="timely-stream-event"[^>]*id="timely-event-list-\d+-(\d{8})\d*"[^>]*aria-label="([^"]+)"[^>]*>'
        r'(.*?)(?=<div[^>]*class="timely-stream-event"|</body>)',
        re.DOTALL
    )

    events = []
    for m in pattern.finditer(html):
        id_date  = m.group(1)           # YYYYMMDD
        name     = unescape(m.group(2).strip())
        body     = m.group(3)

        # Extract excerpt text
        exc_m = re.search(r'class="timely-stream-event-excerpt[^"]*">(.*?)</div>', body, re.DOTALL)
        excerpt = re.sub(r'<[^>]+>', '', exc_m.group(1)).replace("&amp;", "&").strip() if exc_m else ""

        date_start, date_end = _parse_date_range(excerpt, id_date)
        sport = _detect_sport(name, excerpt)

        events.append({
            "name":       name,
            "date_start": date_start,
            "date_end":   date_end,
            "location":   "",
            "sport":      sport,
        })

    events.sort(key=lambda e: e["date_start"])
    return events


MODAL_DUMP_FILE = Path("data/calendar_modal_dump.html")

# Selectors to try for the modal/detail panel after clicking an event
MODAL_SELECTORS = [
    "[class*='timely-single-event']",
    "[class*='timely-event-detail']",
    "[class*='timely-modal']",
    "[class*='modal'][class*='open']",
    "[class*='timely-overlay']",
    "mat-dialog-container",
    "[role='dialog']",
]


def _get_location_from_modal(page):
    """Extract location from the open Timely event popup."""
    try:
        venue_el = page.query_selector("#timely-event-venue")
        if not venue_el:
            return ""
        # Venue name (e.g. "Maffra Gymnastic Club")
        name_el = venue_el.query_selector(".timely-venue-name")
        venue_name = name_el.inner_text().strip() if name_el else ""
        # Address span (e.g. "Cameron Sporting Complex, Morison St, Maffra VIC 3860")
        spans = venue_el.query_selector_all("span")
        address = ""
        for s in spans:
            t = s.inner_text().strip()
            if t and t != venue_name:
                address = t
                break
        if venue_name and address:
            return f"{venue_name}, {address}"
        return venue_name or address
    except Exception:
        return ""


def _close_modal(page):
    """Close the Timely event popup."""
    try:
        btn = page.query_selector("#timely-event-details-close-button")
        if btn and btn.is_visible():
            btn.click()
            page.wait_for_timeout(400)
            return
    except Exception:
        pass
    page.keyboard.press("Escape")
    page.wait_for_timeout(400)


def scrape(dump=False, dump_modal=False):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        )

        print(f"Loading {TIMELY_URL} ...")
        page.goto(TIMELY_URL, wait_until="networkidle", timeout=30000)

        try:
            page.wait_for_selector(".timely-stream-event", timeout=15000)
        except PWTimeout:
            print("  [WARN] Timed out waiting for .timely-stream-event — proceeding anyway")

        if dump:
            DUMP_FILE.parent.mkdir(exist_ok=True)
            DUMP_FILE.write_text(page.content(), encoding="utf-8")
            print(f"  List HTML dumped to {DUMP_FILE}")

        # Parse list-view data (names, dates, sport)
        events = parse_events(page.content())
        if not events:
            print("  No events found in list view.")
            browser.close()
            return

        if dump_modal:
            # Click the first event and dump the resulting HTML for inspection
            print("  Clicking first event to capture modal HTML...")
            first = page.query_selector(".timely-stream-event")
            if first:
                first.click()
                page.wait_for_timeout(2000)
                MODAL_DUMP_FILE.parent.mkdir(exist_ok=True)
                MODAL_DUMP_FILE.write_text(page.content(), encoding="utf-8")
                print(f"  Modal HTML dumped to {MODAL_DUMP_FILE}")
            browser.close()
            return

        # Collect element IDs first — we re-query each one fresh after each modal
        # close to avoid stale handles from Timely's virtual DOM re-rendering.
        card_ids = [
            el.get_attribute("id")
            for el in page.query_selector_all(".timely-stream-event[id]")
        ]
        print(f"  Fetching location for {len(events)} events...")

        for i, card_id in enumerate(card_ids):
            name = events[i]["name"] if i < len(events) else f"event {i}"
            try:
                card = page.query_selector(f"#{card_id}")
                if not card:
                    print(f"  [{i+1}/{len(card_ids)}] {name}: element not found, skipping")
                    continue
                card.scroll_into_view_if_needed()
                page.wait_for_timeout(400)
                try:
                    card.click()
                except Exception:
                    # Re-query after brief wait in case of DOM re-render
                    page.wait_for_timeout(800)
                    card = page.query_selector(f"#{card_id}")
                    if card:
                        card.click()
                    else:
                        raise
                page.wait_for_timeout(1500)

                location = _get_location_from_modal(page)
                events[i]["location"] = location
                status = location if location else "(no location found)"
                print(f"  [{i+1}/{len(card_ids)}] {name}: {status}")

                _close_modal(page)
                page.wait_for_timeout(600)

            except Exception as e:
                print(f"  [{i+1}/{len(card_ids)}] {name}: error — {e}")

        browser.close()

    for e in events:
        sport_tag = f"[{e['sport']}]" if e['sport'] != 'ALL' else ""
        print(f"  {e['date_start']} -> {e['date_end']}  {sport_tag} {e['name']} | {e['location'] or '-'}")

    OUTPUT_FILE.parent.mkdir(exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump({"events": events}, f, indent=2, ensure_ascii=False)

    print(f"\nDone — {len(events)} event(s) written to {OUTPUT_FILE}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dump",       action="store_true", help="Save list-view HTML to data/calendar_dump.html")
    parser.add_argument("--dump-modal", action="store_true", help="Click first event and save modal HTML to data/calendar_modal_dump.html")
    args = parser.parse_args()
    scrape(dump=args.dump, dump_modal=args.dump_modal)
