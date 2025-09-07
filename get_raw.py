from datasets import load_dataset
import json
import random
import os
import sys

# Config
TARGET_SIZE_MB = 100        # aim for ~100MB
DOMAINS = ["Finance", "Sports", "Science", "Politics", "History",
           "Movies", "Technology", "Geography", "Art", "Health"]
OUT_FILE = "/Users/kabir/Desktop/Research/Implementation/bad_raw.jsonl"

# Load with streaming (no full dump download!)
ds = load_dataset("wikipedia", "20220301.en", split="train", streaming=True, trust_remote_code=True)

# Shuffle stream manually (not perfect, but good enough)
ds_iter = iter(ds)

# Open file
with open(OUT_FILE, "w", encoding="utf-8") as f:
    total_bytes = 0
    domain_cycle = iter(DOMAINS)

    for article in ds_iter:
        try:
            title = article["title"]
            text = article["text"]

            # Cycle through domains (simulate stratified sampling)
            try:
                domain = next(domain_cycle)
            except StopIteration:
                domain_cycle = iter(DOMAINS)
                domain = next(domain_cycle)

            record = {"domain": domain, "title": title, "text": text}
            line = json.dumps(record, ensure_ascii=False) + "\n"
            f.write(line)

            total_bytes += len(line.encode("utf-8"))
            size_mb = total_bytes / (1024 * 1024)

            if size_mb >= TARGET_SIZE_MB:
                print(f"✅ Reached target size: {size_mb:.2f} MB")
                break
        except Exception as e:
            print("Skipping article:", e)

print(f"Saved {OUT_FILE} with size ~{os.path.getsize(OUT_FILE)/(1024*1024):.2f} MB")