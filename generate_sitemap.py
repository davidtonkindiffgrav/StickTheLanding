"""Run this after publishing a new article to regenerate sitemap.xml."""
import json, datetime
from pathlib import Path

BASE = "https://stickthelanding.com.au"
ROOT = Path(__file__).parent

STATIC_PAGES = [
    (f"{BASE}/",         "weekly",  "1.0"),
    (f"{BASE}/news/",    "weekly",  "0.8"),
    (f"{BASE}/about/",   "monthly", "0.5"),
    (f"{BASE}/privacy/", "monthly", "0.3"),
    (f"{BASE}/stickit/", "monthly", "0.4"),
]

posts = json.loads((ROOT / "news" / "posts" / "index.json").read_text(encoding="utf-8"))

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

lines.append("</urlset>")

out = ROOT / "sitemap.xml"
out.write_text("\n".join(lines) + "\n", encoding="utf-8")
print(f"Wrote {out} ({len(posts)} articles + {len(STATIC_PAGES)} static pages)")
