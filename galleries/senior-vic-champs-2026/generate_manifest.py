"""
Run once after dropping all photos into photos/full/:
  python generate_manifest.py

Writes photos.json with a sorted list of filenames.
"""
import os, json

src = os.path.join(os.path.dirname(__file__), 'photos', 'full')
exts = {'.jpg', '.jpeg', '.png', '.webp'}
files = sorted(f for f in os.listdir(src) if os.path.splitext(f)[1].lower() in exts)
out = os.path.join(os.path.dirname(__file__), 'photos.json')
with open(out, 'w') as fh:
    json.dump(files, fh)
print(f"Wrote {len(files)} photos to photos.json")
