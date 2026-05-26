"""
Generates height-250px thumbnails into photos/thumbs/ from photos/full/.
Run from the gallery folder:  python generate_thumbs.py
"""
import os
from pathlib import Path
from PIL import Image

THUMB_HEIGHT = 250
QUALITY = 75
SRC = Path(__file__).parent / 'photos' / 'full'
DST = Path(__file__).parent / 'photos' / 'thumbs'
EXTS = {'.jpg', '.jpeg', '.png', '.webp'}

DST.mkdir(parents=True, exist_ok=True)
files = [f for f in sorted(SRC.iterdir()) if f.suffix.lower() in EXTS]

print(f"Generating {len(files)} thumbnails...")
for i, src_path in enumerate(files, 1):
    dst_path = DST / (src_path.stem + '.jpg')
    if dst_path.exists():
        print(f"  [{i}/{len(files)}] skip (exists): {src_path.name}")
        continue
    with Image.open(src_path) as img:
        img = img.convert('RGB')
        ratio = THUMB_HEIGHT / img.height
        new_w = max(1, int(img.width * ratio))
        img = img.resize((new_w, THUMB_HEIGHT), Image.LANCZOS)
        img.save(dst_path, 'JPEG', quality=QUALITY, optimize=True)
    print(f"  [{i}/{len(files)}] {src_path.name} → {new_w}×{THUMB_HEIGHT}")

print("Done.")
