"""Run this after publishing a new article to regenerate sitemap.xml."""
import json, datetime
from pathlib import Path

BASE = "https://stickthelanding.com.au"
ROOT = Path(__file__).parent

STATIC_PAGES = [
    (f"{BASE}/",                    "weekly",  "1.0"),
    (f"{BASE}/news/",               "weekly",  "0.8"),
    (f"{BASE}/competitions/wag/",   "weekly",  "0.7"),
    (f"{BASE}/competitions/mag/",   "weekly",  "0.7"),
    (f"{BASE}/about/",              "monthly", "0.5"),
    (f"{BASE}/contact/",            "monthly", "0.4"),
    (f"{BASE}/privacy/",            "monthly", "0.3"),
    (f"{BASE}/stickit/",            "monthly", "0.4"),
]

posts = json.loads((ROOT / "news" / "posts" / "index.json").read_text(encoding="utf-8"))

results_index_path = ROOT / "data" / "results_index.json"
results_pages = json.loads(results_index_path.read_text(encoding="utf-8")) if results_index_path.exists() else []

lines = ['<?xml version="1.0" encoding="UTF-8"?>',
         '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']

for loc, freq, pri in STATIC_PAGES:
    lines += [
        "  <url>",
        f"    <loc>{loc}</loc>",
        f"    <changefreq>{freq}</changefreq>",
        f"    <priority>{pri}</priority>",
        "  </url>",
    ]

for p in posts:
    loc  = f"{BASE}/news/posts/{p['slug']}/"
    last = p.get("date", datetime.date.today().isoformat())
    lines += [
        "  <url>",
        f"    <loc>{loc}</loc>",
        f"    <lastmod>{last}</lastmod>",
        "    <changefreq>monthly</changefreq>",
        "    <priority>0.7</priority>",
        "  </url>",
    ]

for r in results_pages:
    loc  = f"{BASE}/results/{r['sport']}/{r['comp_id']}/"
    last = r.get("date") or datetime.date.today().isoformat()
    lines += [
        "  <url>",
        f"    <loc>{loc}</loc>",
        f"    <lastmod>{last}</lastmod>",
        "    <changefreq>monthly</changefreq>",
        "    <priority>0.6</priority>",
        "  </url>",
    ]

lines.append("</urlset>")

out = ROOT / "sitemap.xml"
out.write_text("\n".join(lines) + "\n", encoding="utf-8")
print(f"Wrote {out} ({len(posts)} articles + {len(results_pages)} result pages + {len(STATIC_PAGES)} static pages)")
