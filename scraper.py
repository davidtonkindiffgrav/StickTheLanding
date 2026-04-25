"""
scraper.py — Fetches WAG competition PDFs from Gymnastics Victoria results page.

Pipeline:
  1. Fetch vic.gymnastics.org.au/events/results → parse __NUXT_DATA__
  2. Find WAG <year> HTML content block → extract competition links
  3. For PDF links (:b:): download directly
  4. For folder links (:f:): resolve path via redirect, use SharePoint REST API
     to list PDF files, then download each one
  5. Save manifest to data/links_<year>_wag.json
  6. Update data/url_map.json with {comp_name: {source_key: download_url}} entries

Usage:
  python scraper.py             # defaults to current calendar year
  python scraper.py --year 2026
"""

import datetime
import json
import re
import sys
import time
from pathlib import Path
from urllib.parse import urlparse, parse_qs, unquote, quote

import requests
from bs4 import BeautifulSoup

RESULTS_PAGE = "https://vic.gymnastics.org.au/events/results"
SP_BASE = "https://gymnasticsvictoria.sharepoint.com"
SP_SITE = "/sites/GymnasticsVictoriaHub"
DATA_DIR = Path("data")
URL_MAP_FILE = DATA_DIR / "url_map.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


# ---------------------------------------------------------------------------
# Page fetch and Nuxt data parsing
# ---------------------------------------------------------------------------

def fetch_results_page(session):
    print("Fetching results page...")
    resp = session.get(RESULTS_PAGE, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.text


def parse_nuxt_data(html):
    soup = BeautifulSoup(html, "lxml")
    script = soup.find("script", {"id": "__NUXT_DATA__"})
    if not script or not script.string:
        raise ValueError("Could not find __NUXT_DATA__ on results page")
    return json.loads(script.string)


def find_wag_year_html(nuxt, year):
    """
    Walk the Nuxt devalue array to find WAG <year> competition HTML content.

    Structure (discovered by inspection):
      - '<year>' string → accordion container → children list
      - Children include {title: idx_of_womens_gymnastics} section
      - That section's content is the HTML table
    """
    year_str = str(year)

    # Find "Women's Gymnastics" title index
    womens_idx = None
    for i, v in enumerate(nuxt):
        if isinstance(v, str) and v in ("Women's Gymnastics", "Women\u2019s Gymnastics"):
            womens_idx = i
            break
    if womens_idx is None:
        raise ValueError("Could not find Women's Gymnastics section in Nuxt data")

    # Find year string
    year_idx = None
    for i, v in enumerate(nuxt):
        if v == year_str:
            year_idx = i
            break
    if year_idx is None:
        raise ValueError(f"Could not find '{year_str}' year in Nuxt data")

    # From the year entry, walk forward to find the accordion with the Women's section
    for i in range(year_idx, min(len(nuxt), year_idx + 30)):
        v = nuxt[i]
        if isinstance(v, list) and len(v) > 2:
            for child_ref in v:
                if not isinstance(child_ref, int) or child_ref >= len(nuxt):
                    continue
                child = nuxt[child_ref]
                if isinstance(child, dict) and child.get("title") == womens_idx:
                    ch_children = nuxt[child.get("children", -1)]
                    if isinstance(ch_children, list) and ch_children:
                        content_item = nuxt[ch_children[0]]
                        if isinstance(content_item, dict):
                            content_str = nuxt[content_item.get("content", -1)]
                            if isinstance(content_str, str) and "<table" in content_str:
                                return content_str
    raise ValueError(f"Could not locate WAG {year_str} HTML content block in Nuxt data")


# ---------------------------------------------------------------------------
# Competition extraction
# ---------------------------------------------------------------------------

def extract_competitions(wag_html):
    """Parse the WAG 2025 HTML table into competition records."""
    soup = BeautifulSoup(wag_html, "lxml")
    comps = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "sharepoint.com" not in href and "incrowdsports.com" not in href:
            continue
        name = a.get_text(strip=True)
        row = a.find_parent("tr")
        date = ""
        if row:
            cells = [td.get_text(strip=True) for td in row.find_all("td")]
            date = cells[0] if cells else ""
        link_type = "folder" if "/:f:/" in href else "pdf"
        comps.append({"name": name, "date": date, "url": href, "link_type": link_type})
    return comps


# ---------------------------------------------------------------------------
# SharePoint folder resolution and file listing
# ---------------------------------------------------------------------------

def resolve_folder_path(url, session):
    """Follow a sharing link redirect to extract the server-relative folder path."""
    try:
        r = session.get(url, timeout=30, allow_redirects=True)
        parsed = urlparse(r.url)
        qs = parse_qs(parsed.query)
        folder_id = qs.get("id", [None])[0]
        return unquote(folder_id) if folder_id else None
    except Exception as exc:
        print(f"    [WARN] Could not resolve folder path: {exc}")
        return None


def list_sharepoint_folder(server_relative_path, session):
    """Use SharePoint REST API to list PDF files in a folder."""
    # Escape single quotes for OData string literal
    escaped = server_relative_path.replace("'", "''")
    api_url = (
        f"{SP_BASE}{SP_SITE}/_api/web"
        f"/GetFolderByServerRelativeUrl('{escaped}')/Files"
        f"?$select=Name,ServerRelativeUrl&$format=json"
    )
    try:
        r = session.get(
            api_url,
            headers={"Accept": "application/json;odata=verbose"},
            timeout=30,
        )
        if r.status_code == 200:
            data = r.json()
            files = data.get("d", {}).get("results", data.get("value", []))
            return [f for f in files if f["Name"].lower().endswith(".pdf")]
        else:
            print(f"    [WARN] REST API {r.status_code}: {r.text[:200]}")
    except Exception as exc:
        print(f"    [WARN] REST API error: {exc}")
    return []


SP_VIEW_ID = "e6406da1-023f-4119-9f6c-dba429c0526b"
SP_LIBRARY_FORMS = f"{SP_BASE}{SP_SITE}/Shared%20Documents/Forms/Viewing.aspx"


def pdf_download_url(server_relative_path):
    """Direct download URL — used by the scraper to fetch the file bytes."""
    encoded = quote(server_relative_path, safe="/:")
    return f"{SP_BASE}{encoded}?download=1"


def pdf_view_url(server_relative_path):
    """
    SharePoint Viewing.aspx URL — what gets stored in url_map and shown to users.
    The ?download=1 format returns Access Denied; this viewer URL works.
    """
    parent = server_relative_path.rsplit("/", 1)[0]
    encoded_id = quote(server_relative_path, safe="")
    encoded_parent = quote(parent, safe="")
    return (
        f"{SP_LIBRARY_FORMS}"
        f"?viewid={SP_VIEW_ID}"
        f"&id={encoded_id}"
        f"&parent={encoded_parent}"
    )


# ---------------------------------------------------------------------------
# PDF download
# ---------------------------------------------------------------------------

def download_pdf(url, dest, session):
    if dest.exists():
        print(f"    [SKIP] {dest.name}")
        return True

    dest.parent.mkdir(parents=True, exist_ok=True)

    # Try with ?download=1 appended
    dl_url = url if "download=1" in url else url + ("&" if "?" in url else "?") + "download=1"

    try:
        r = session.get(dl_url, timeout=60, allow_redirects=True)
        r.raise_for_status()
        ct = r.headers.get("Content-Type", "")
        if "pdf" not in ct and "octet-stream" not in ct:
            # Try without download=1 flag
            r2 = session.get(url, timeout=60, allow_redirects=True)
            if "pdf" in r2.headers.get("Content-Type", ""):
                r = r2
            else:
                print(f"    [WARN] Unexpected content-type: {ct}")
                return False
        dest.write_bytes(r.content)
        print(f"    [OK]   {dest.name} ({len(r.content) // 1024} KB)")
        return True
    except Exception as exc:
        print(f"    [ERR]  {exc}")
        return False


def slugify(text):
    return re.sub(r"[^a-zA-Z0-9]+", "-", text).strip("-").lower() or "unnamed"


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--no-download", action="store_true",
        help="Update url_map with fresh GymVic URLs without downloading any PDFs"
    )
    parser.add_argument(
        "--year", type=int, default=datetime.date.today().year,
        help="Season year to scrape (default: current calendar year)"
    )
    args = parser.parse_args()
    year = args.year
    pdf_dir = Path(f"pdfs/{year}/WAG")

    session = requests.Session()
    session.headers.update(HEADERS)

    html = fetch_results_page(session)
    nuxt = parse_nuxt_data(html)

    print(f"Locating WAG {year} competition section...")
    wag_html = find_wag_year_html(nuxt, year)

    comps = extract_competitions(wag_html)
    print(f"  Found {len(comps)} competition entries")
    print(f"  Folders: {sum(1 for c in comps if c['link_type'] == 'folder')}  "
          f"PDFs: {sum(1 for c in comps if c['link_type'] == 'pdf')}")

    DATA_DIR.mkdir(exist_ok=True)
    manifest = []
    ok_total = 0
    fail_total = 0

    # Load existing url_map to merge into
    url_map = {}
    if URL_MAP_FILE.exists():
        with open(URL_MAP_FILE, encoding="utf-8") as f:
            url_map = json.load(f)

    for i, comp in enumerate(comps, 1):
        name = comp["name"]
        date = comp["date"]
        url = comp["url"]
        ltype = comp["link_type"]
        comp_slug = slugify(name)

        print(f"\n[{i:2d}/{len(comps)}] {name} ({date}) [{ltype}]")

        if ltype == "pdf":
            filename = f"{comp_slug}.pdf"
            dl_url = url if "download=1" in url else url + ("&" if "?" in url else "?") + "download=1"
            if not args.no_download:
                dest = pdf_dir / name / filename
                if download_pdf(dl_url, dest, session):
                    ok_total += 1
                    manifest.append({**comp, "files": [str(dest)]})
                else:
                    fail_total += 1
            url_map.setdefault(name, {})["__pdf__"] = dl_url
        else:
            # Folder — resolve path and list PDFs
            path = resolve_folder_path(url, session)
            if not path:
                print(f"    [SKIP] Could not resolve folder path")
                fail_total += 1
                continue

            print(f"    Path: .../{'/'.join(path.split('/')[-3:])}")
            pdf_files = list_sharepoint_folder(path, session)
            print(f"    Found {len(pdf_files)} PDF(s) in folder")

            # Replace stale entries for this competition with fresh data
            url_map[name] = {}
            comp_dir = pdf_dir / name
            downloaded = []
            for pf in pdf_files:
                sr_path = pf["ServerRelativeUrl"]
                filename = pf["Name"]
                view_url = pdf_view_url(sr_path)
                url_map[name][filename] = view_url
                if not args.no_download:
                    dl_url = pdf_download_url(sr_path)
                    dest = comp_dir / filename
                    if download_pdf(dl_url, dest, session):
                        ok_total += 1
                        downloaded.append(str(dest))
                    else:
                        fail_total += 1
                time.sleep(0.3)

            if downloaded:
                manifest.append({**comp, "resolved_path": path, "files": downloaded})
            time.sleep(0.5)

    manifest_path = DATA_DIR / f"links_{year}_wag.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)

    with open(URL_MAP_FILE, "w", encoding="utf-8") as f:
        json.dump(url_map, f, indent=2, ensure_ascii=False)

    pdf_dir.mkdir(parents=True, exist_ok=True)
    print(f"\n{'='*60}")
    print(f"Done. {ok_total} PDFs downloaded, {fail_total} failed.")
    print(f"Manifest saved to {manifest_path}")
    print(f"URL map saved to {URL_MAP_FILE}")
    print(f"PDFs in {pdf_dir}/")


if __name__ == "__main__":
    main()
