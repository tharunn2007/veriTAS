# src/ingest_bronze.py
import os
import sys
import uuid
import hashlib
from datetime import datetime

import pandas as pd

# Let Python find config.py even though this script lives in src/
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import FILES_TO_INGEST, TARGET_COLUMNS, COLUMN_MAPPING


def generate_hash(text):
    """MD5 fingerprint of the article body -> used for dedupe in Silver."""
    if pd.isna(text) or str(text).strip() == "":
        return None
    return hashlib.md5(str(text).encode("utf-8")).hexdigest()

def ingest_one_file(path, source_name, source_id):
    """Read one raw CSV and land it in Bronze as Parquet."""
    print(f"\n📥 Reading {path} ...")
    # on_bad_lines='skip': real-world CSVs have broken rows; don't crash on them
    df = pd.read_csv(path, on_bad_lines="skip")
    print(f"   raw rows: {len(df):,}")

    # Translate Kaggle columns into our schema
    df = df.rename(columns=COLUMN_MAPPING)

    # Columns Kaggle doesn't provide -> fill with honest placeholders
    df["source_name"] = source_name
    df["source_id"] = source_id
    df["author"] = "Unknown"
    df["url"] = "N/A"
    df["language"] = "en"
    df["country"] = "US"
    df["description"] = df["content"].astype(str).str[:200] + "..."

    # Pipeline metadata (this is what makes it a DATA LAKE, not just a copy)
    df["article_id"] = [str(uuid.uuid4()) for _ in range(len(df))]  # unique ID per row
    df["ingested_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # when it entered the lake
    print("   🔐 hashing content (a few seconds)...")
    df["content_hash"] = df["content"].apply(generate_hash)          # dedupe fingerprint

    # Enforce the contract: keep ONLY the 14 agreed columns, in order
    df = df[TARGET_COLUMNS]

    # Land in Bronze as Parquet (columnar, compressed, schema-aware)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = f"data/bronze/bronze_{source_name.lower()}_{ts}.parquet"
    df.to_parquet(out, index=False)
    print(f"   💾 wrote {out}  ({len(df):,} rows)")
    return out


def main():
    print("🚀 Bronze ingestion start")
    for f in FILES_TO_INGEST:
        ingest_one_file(f["path"], f["source_name"], f["source_id"])
    print("\n🎉 Bronze layer complete.")


if __name__ == "__main__":
    main()