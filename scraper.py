"""
scraper.py — Fetches 2025 WAG competition PDFs from Gymnastics Victoria results page.

Pipeline:
  1. Fetch vic.gymnastics.org.au/events/results → parse __NUXT_DATA__
  2. Find WAG 2025 HTML content block → extract competition links
  3. For PDF links (:b:): download directly
  4. For folder links (:f:): resolve path via redirect, use SharePoint REST API
     to list PDF files, then download each one
  5. Save manifest to data/links_2025_wag.json
  6. Update data/url_map.json with {comp_name: {source_key: download_url}} entries
"""

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
PDF_DIR = Path("pdfs/2025/WAG")
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


def find_wag_2025_html(nuxt):
    """
    Walk the Nuxt devalue array to find WAG 2025 competition HTML content.

    Structure (discovered by inspection):
      - '2025' string → accordion container → children list
      - Children include {title: idx_of_womens_gymnastics} section
      - That section's content is the HTML table at index ~712
    """
    # Find "Women's Gymnastics" and "Men's Gymnastics" title indices
    womens_idx = None
    for i, v in enumerate(nuxt):
        if isinstance(v, str) and v in ("Women's Gymnastics", "Women\u2019s Gymnastics"):
            womens_idx = i
            break
    if womens_idx is None:
        raise ValueError("Could not find Women's Gymnastics section in Nuxt data")

    # Find '2025' year string
    year_idx = None
    for i, v in enumerate(nuxt):
        if v == "2025":
            year_idx = i
            break
    if year_idx is None:
        raise ValueError("Could not find '2025' year in Nuxt data")

    # From the '2025' entry, walk forward to find the accordion that has
    # a child section with title == womens_idx.
    # Structure: [year_idx+2] is the 2025 accordion container → children list
    # → list of section objects → one has 'title': womens_idx
    for i in range(year_idx, min(len(nuxt), year_idx + 30)):
        v = nuxt[i]
        if isinstance(v, list) and len(v) > 2:
            # This might be the children list for the 2025 sections
            for child_ref in v:
                if not isinstance(child_ref, int) or child_ref >= len(nuxt):
                    continue
                child = nuxt[child_ref]
                if isinstance(child, dict) and child.get("title") == womens_idx:
                    # Found the Women's section; get its content
                    ch_children = nuxt[child.get("children", -1)]
                    if isinstance(ch_children, list) and ch_children:
                        content_item = nuxt[ch_children[0]]
                        if isinstance(content_item, dict):
                            content_str = nuxt[content_item.get("content", -1)]
                            if isinstance(content_str, str) and "<table" in content_str:
                                return content_str
    raise ValueError("Could not locate WAG 2025 HTML content block in Nuxt data")


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


def pdf_download_url(server_relative_path):
    """Construct a direct download URL from a SharePoint server-relative path."""
    encoded = quote(server_relative_path, safe="/:")
    return f"{SP_BASE}{encoded}?download=1"


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
    session = requests.Session()
    session.headers.update(HEADERS)

    html = fetch_results_page(session)
    nuxt = parse_nuxt_data(html)

    print("Locating WAG 2025 competition section...")
    wag_html = find_wag_2025_html(nuxt)

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
            # Single PDF — store in a comp subfolder so update.py can find it
            filename = f"{comp_slug}.pdf"
            dest = PDF_DIR / name / filename
            dl_url = url if "download=1" in url else url + ("&" if "?" in url else "?") + "download=1"
            success = download_pdf(url, dest, session)
            if success:
                ok_total += 1
                manifest.append({**comp, "files": [str(dest)]})
                url_map.setdefault(name, {})[filename] = dl_url
            else:
                fail_total += 1
        else:
            # Folder — resolve path and list PDFs
            path = resolve_folder_path(url, session)
            if not path:
                print(f"    [SKIP] Could not resolve folder path")
                fail_total += 1
                continue

            if "2025" not in path:
                print(f"    [SKIP] Not a 2025 event (path: {path.split('/')[-3]})")
                continue

            print(f"    Path: .../{'/'.join(path.split('/')[-3:])}")
            pdf_files = list_sharepoint_folder(path, session)
            print(f"    Found {len(pdf_files)} PDF(s) in folder")

            comp_dir = PDF_DIR / name
            downloaded = []
            for pf in pdf_files:
                sr_path = pf["ServerRelativeUrl"]
                filename = pf["Name"]
                dl_url = pdf_download_url(sr_path)
                dest = comp_dir / filename
                if download_pdf(dl_url, dest, session):
                    ok_total += 1
                    downloaded.append(str(dest))
                    url_map.setdefault(name, {})[filename] = dl_url
                else:
                    fail_total += 1
                time.sleep(0.3)

            if downloaded:
                manifest.append({**comp, "resolved_path": path, "files": downloaded})
            time.sleep(0.5)

    manifest_path = DATA_DIR / "links_2025_wag.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)

    with open(URL_MAP_FILE, "w", encoding="utf-8") as f:
        json.dump(url_map, f, indent=2, ensure_ascii=False)

    PDF_DIR.mkdir(parents=True, exist_ok=True)
    print(f"\n{'='*60}")
    print(f"Done. {ok_total} PDFs downloaded, {fail_total} failed.")
    print(f"Manifest saved to {manifest_path}")
    print(f"URL map saved to {URL_MAP_FILE}")
    print(f"PDFs in {PDF_DIR}/")


if __name__ == "__main__":
    main()
