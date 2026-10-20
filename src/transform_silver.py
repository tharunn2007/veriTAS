"""Validate and deduplicate existing Bronze snapshots without changing them.

Run: .venv/bin/python -m src.transform_silver
Outputs: data/silver/runs/<input-and-rule-fingerprint>/ and current.json.
This v1 adapter handles the existing Kaggle CSV-derived Bronze schema only.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import shutil
import tempfile
from datetime import datetime, timezone

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
VERSION = "silver-v1"
HASH_VERSION = "body-v1"
CATEGORY_MAP = {"politicsNews": "politics", "politics": "politics", "worldnews": "world"}
SOURCES = {1: "Kaggle_RealNews", 2: "Kaggle_FakeNews"}
# Kept here to avoid config.py's directory-creation side effects on import.
REQUIRED_COLUMNS = {
    "article_id", "source_id", "source_name", "title", "description", "content",
    "author", "category", "published_at", "url", "language", "country",
    "ingested_at", "content_hash",
}
ARTIFACTS = ("articles.parquet", "rejected.parquet", "occurrences.parquet", "quality_report.json")


def file_hash(path):
    """Hash file bytes in chunks, rather than loading a whole file into memory."""
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def normalize_body(value):
    if pd.isna(value):
        return pd.NA
    return str(value).replace("\r\n", "\n").replace("\r", "\n").strip()


def load_bronze(folder):
    """Load all snapshots and attach an exact file/row pointer to every record."""
    paths = sorted(folder.glob("*.parquet"))
    if not paths:
        raise FileNotFoundError(f"No Bronze Parquet files in {folder}")
    frames, inputs = [], []
    for path in paths:
        fingerprint = file_hash(path)
        frame = pd.read_parquet(path)
        missing = REQUIRED_COLUMNS - set(frame.columns)
        if missing:
            raise ValueError(f"{path.name}: missing columns {sorted(missing)}")
        if file_hash(path) != fingerprint:
            raise ValueError(f"Input changed while reading: {path}")
        frame["bronze_file"] = path.name
        frame["bronze_row_number"] = range(len(frame))
        inputs.append({"file": path.name, "sha256": fingerprint, "rows": len(frame)})
        frames.append(frame)
    return pd.concat(frames, ignore_index=True), inputs


def transform(bronze):
    """Return canonical articles, original rejected rows, valid occurrences and metrics."""
    df = bronze.copy().rename(columns={
        "article_id": "bronze_article_id", "content_hash": "bronze_content_hash",
    })
    for column in ("published_at", "ingested_at", "category"):
        df[column + "_raw"] = df[column]
    for column in ("title", "category", "source_name"):
        df[column] = df[column].astype("string").str.strip()
    df["content"] = df["content"].map(normalize_body).astype("string")
    df["category"] = df["category"].replace(CATEGORY_MAP)
    # Date-only historical dataset: midnight is a representation, not a known hour.
    df["published_at"] = pd.to_datetime(
        df["published_at_raw"].astype("string").str.strip(),
        format="mixed", errors="coerce",
    ).dt.normalize()
    df["publication_precision"] = "day"
    local_time = pd.to_datetime(
        df["ingested_at_raw"], format="%Y-%m-%d %H:%M:%S", errors="coerce",
    )
    # Existing Bronze strings have no timezone; use the documented local assumption.
    df["ingested_at"] = local_time.dt.tz_localize("Asia/Kolkata").dt.tz_convert("UTC")
    for column in ("author", "url"):
        values = df[column].astype("string").str.strip()
        df[column] = values.mask(values.str.lower().isin(["", "unknown", "n/a"]))
    df["country"] = pd.NA  # Bronze injected US; it was not supplied by these CSVs.
    df["language_is_assumed"] = True

    reasons = pd.Series("", index=df.index, dtype="string")

    def reject_when(mask, label):
        reasons.loc[mask] = reasons.loc[mask] + label + ";"

    for column in ("title", "content", "source_name"):
        reject_when(df[column].fillna("").eq(""), "missing_" + column)
    source_id = pd.to_numeric(df["source_id"], errors="coerce")
    reject_when(~source_id.isin(SOURCES), "invalid_source_id")
    reject_when(
        df["source_name"].ne(source_id.map(SOURCES)).fillna(True),
        "source_identity_mismatch",
    )
    reject_when(df["published_at"].isna(), "invalid_published_at")
    reject_when(df["ingested_at"].isna(), "invalid_ingested_at")
    invalid = reasons.ne("")
    rejected = bronze.loc[invalid].copy()
    rejected["rejection_reasons"] = reasons.loc[invalid].str.rstrip(";")
    valid = df.loc[~invalid].copy()
    valid["description"] = valid["content"].str[:200] + valid["content"].map(
        lambda text: "..." if len(text) > 200 else ""
    )
    valid["content_hash"] = valid["content"].map(
        lambda text: hashlib.sha256(text.encode("utf-8")).hexdigest()
    )
    valid["hash_version"] = HASH_VERSION
    valid["article_id"] = HASH_VERSION + ":" + valid["content_hash"]
    valid = valid.sort_values(["ingested_at", "bronze_file", "bronze_row_number"])
    valid["is_duplicate"] = valid.duplicated("article_id", keep="first")
    articles = valid.loc[~valid["is_duplicate"]].copy()
    # Every valid appearance survives, including cross-source duplicates and replays.
    occurrences = valid[[
        "article_id", "bronze_article_id", "bronze_content_hash", "source_id",
        "source_name", "bronze_file", "bronze_row_number", "ingested_at",
        "published_at_raw", "category_raw", "is_duplicate",
    ]].copy()
    counts = {
        "input_rows": len(bronze), "rejected_rows": len(rejected),
        "canonical_rows": len(articles),
        "duplicate_occurrence_rows": int(occurrences["is_duplicate"].sum()),
    }
    if counts["input_rows"] != sum(counts[key] for key in (
        "rejected_rows", "canonical_rows", "duplicate_occurrence_rows"
    )):
        raise ValueError("Row accounting failed")
    if not articles["article_id"].is_unique or not articles["content_hash"].is_unique:
        raise ValueError("Canonical keys are not unique")
    if not occurrences["article_id"].isin(articles["article_id"]).all():
        raise ValueError("Unresolved occurrence reference")
    if occurrences.duplicated(["bronze_file", "bronze_row_number"]).any():
        raise ValueError("Repeated physical input row")

    def source_counts(name):
        return {
            "input_rows": int(bronze["source_name"].eq(name).sum()),
            "rejected_rows": int(rejected["source_name"].eq(name).sum()),
            "valid_occurrences": int(occurrences["source_name"].eq(name).sum()),
            "distinct_articles_present": int(
                occurrences.loc[occurrences["source_name"].eq(name), "article_id"].nunique()
            ),
        }

    quality = {
        **counts,
        "rejection_reason_counts": {
            str(reason): int(count) for reason, count in
            rejected["rejection_reasons"].str.split(";").explode().value_counts().items()
        },
        "per_source": {name: source_counts(name) for name in SOURCES.values()},
        "missing_canonical_metadata": {
            column: int(articles[column].isna().sum()) for column in ("author", "url", "country")
        },
        "assumed_language_rows": len(articles),
        "publication_min": str(articles["published_at"].min()),
        "publication_max": str(articles["published_at"].max()),
        "duplicate_definition": "Additional valid normalized-body occurrences, including ingestion replays; not publisher duplicate rate.",
        "timezone_assumption": "Existing timezone-free Bronze ingestion times are Asia/Kolkata.",
    }
    return articles, rejected, occurrences, quality


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def run(root=ROOT):
    """Publish a complete immutable run, then atomically replace its current pointer."""
    root = Path(root)
    bronze, inputs = load_bronze(root / "data" / "bronze")
    # Including this module's bytes invalidates the run identity after code changes.
    identity = {
        "version": VERSION, "hash_version": HASH_VERSION,
        "code_sha256": file_hash(Path(__file__)), "inputs": inputs,
        "category_map": CATEGORY_MAP, "sources": SOURCES,
        "ingestion_timezone": "Asia/Kolkata",
    }
    run_id = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    silver = root / "data" / "silver"
    runs = silver / "runs"
    runs.mkdir(parents=True, exist_ok=True)
    destination = runs / run_id
    if destination.exists():
        manifest = json.loads((destination / "manifest.json").read_text())
        for name in ARTIFACTS:
            if file_hash(destination / name) != manifest["artifacts"][name]:
                raise ValueError(f"Existing run artifact is corrupt: {name}")
        quality = json.loads((destination / "quality_report.json").read_text())
    else:
        articles, rejected, occurrences, quality = transform(bronze)
        staging = Path(tempfile.mkdtemp(prefix=".staging-", dir=runs))
        try:
            for name, frame in (("articles", articles), ("rejected", rejected), ("occurrences", occurrences)):
                frame.to_parquet(staging / (name + ".parquet"), index=False)
                if len(pd.read_parquet(staging / (name + ".parquet"))) != len(frame):
                    raise ValueError(f"Parquet verification failed: {name}")
            write_json(staging / "quality_report.json", quality)
            # Detect changes to Bronze while transformation was in progress.
            for item in inputs:
                if file_hash(root / "data" / "bronze" / item["file"]) != item["sha256"]:
                    raise ValueError("Bronze changed during transformation")
            write_json(staging / "manifest.json", {
                **identity, "run_id": run_id,
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "artifacts": {name: file_hash(staging / name) for name in ARTIFACTS},
            })
            staging.rename(destination)
        finally:
            if staging.exists():
                shutil.rmtree(staging)
    # Unique temporary pointer avoids exposing a partially written JSON file.
    with tempfile.NamedTemporaryFile(mode="w", dir=silver, prefix=".current-", delete=False) as stream:
        pointer = Path(stream.name)
        json.dump({"run_id": run_id, "path": "runs/" + run_id}, stream)
        stream.write("\n")
    pointer.replace(silver / "current.json")
    print(json.dumps({"run_id": run_id, **quality}, indent=2))
    return destination


if __name__ == "__main__":
    run()
