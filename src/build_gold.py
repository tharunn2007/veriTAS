"""Build dashboard-ready Gold aggregates from exactly one completed Silver run.

Run from project root: .venv/bin/python -m src.build_gold
Bronze and Silver files are read-only. Use one local writer at a time.
"""
from pathlib import Path
import hashlib
import json
import shutil
import tempfile
from datetime import datetime, timezone

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
VERSION = "gold-v1"

# Each query produces a small table with an explicitly defined grain.
QUERIES = {
    "daily_articles": """
        SELECT CAST(published_at AS DATE) AS publication_date,
               COUNT(*) AS article_count
        FROM articles GROUP BY 1 ORDER BY 1
    """,
    "category_distribution": """
        SELECT COALESCE(NULLIF(TRIM(category), ''), 'uncategorized') AS category,
               COUNT(*) AS article_count
        FROM articles GROUP BY 1 ORDER BY article_count DESC, category
    """,
    "daily_category_articles": """
        SELECT CAST(published_at AS DATE) AS publication_date,
               COALESCE(NULLIF(TRIM(category), ''), 'uncategorized') AS category,
               COUNT(*) AS article_count
        FROM articles GROUP BY 1, 2 ORDER BY 1, 2
    """,
    "source_participation": """
        WITH valid AS (
            SELECT source_id, source_name, COUNT(*) AS valid_occurrences,
                   COUNT(DISTINCT article_id) AS distinct_articles_present
            FROM occurrences GROUP BY 1, 2
        ), rejected_counts AS (
            SELECT source_id, source_name, COUNT(*) AS rejected_rows
            FROM rejected GROUP BY 1, 2
        )
        SELECT COALESCE(v.source_id, r.source_id) AS source_id,
               COALESCE(v.source_name, r.source_name) AS source_name,
               COALESCE(valid_occurrences, 0) AS valid_occurrences,
               COALESCE(distinct_articles_present, 0) AS distinct_articles_present,
               COALESCE(rejected_rows, 0) AS rejected_rows,
               COALESCE(valid_occurrences, 0) + COALESCE(rejected_rows, 0) AS input_rows,
               COALESCE(valid_occurrences, 0) - COALESCE(distinct_articles_present, 0)
                   AS additional_within_source_occurrences,
               100.0 * COALESCE(rejected_rows, 0) /
                   NULLIF(COALESCE(valid_occurrences, 0) + COALESCE(rejected_rows, 0), 0)
                   AS rejection_rate_pct,
               100.0 * (COALESCE(valid_occurrences, 0) - COALESCE(distinct_articles_present, 0)) /
                   NULLIF(valid_occurrences, 0) AS additional_occurrence_rate_pct
        FROM valid v FULL OUTER JOIN rejected_counts r
             ON v.source_id IS NOT DISTINCT FROM r.source_id
             AND v.source_name IS NOT DISTINCT FROM r.source_name
        ORDER BY source_id, source_name
    """,
    "rejection_reasons": """
        SELECT reason, COUNT(*) AS rejected_rows_with_reason
        FROM rejected, UNNEST(STRING_SPLIT(rejection_reasons, ';')) AS labels(reason)
        WHERE reason <> '' GROUP BY reason ORDER BY reason
    """,
}


def file_hash(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def load_silver(root):
    """Follow current.json, restrict it to Silver runs, and verify stored checksums."""
    silver = (root / "data" / "silver").resolve()
    pointer = json.loads((silver / "current.json").read_text())
    run_id = pointer["run_id"]
    expected = Path("runs") / run_id
    if not isinstance(run_id, str) or len(run_id) != 64 or any(c not in '0123456789abcdef' for c in run_id):
        raise ValueError("Invalid Silver run ID")
    if pointer["path"] != expected.as_posix():
        raise ValueError("Silver pointer path does not match run ID")
    folder = (silver / expected).resolve()
    if not folder.is_relative_to(silver / "runs"):
        raise ValueError("Silver run escapes its runs directory")
    manifest_path = folder / "manifest.json"
    manifest_bytes = manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes)
    if manifest["run_id"] != run_id:
        raise ValueError("Silver manifest does not match current pointer")
    fingerprints = manifest["artifacts"]
    for name in ("articles.parquet", "occurrences.parquet", "rejected.parquet", "quality_report.json"):
        if file_hash(folder / name) != fingerprints[name]:
            raise ValueError(f"Silver checksum mismatch: {name}")
    frames = {name: pd.read_parquet(folder / (name + ".parquet"))
              for name in ("articles", "occurrences", "rejected")}
    quality = json.loads((folder / "quality_report.json").read_text())
    lineage = {"silver_run_id": run_id,
               "silver_manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
               "silver_artifacts": fingerprints}
    return frames, quality, lineage


def aggregate(frames, quality):
    """Execute SQL in an in-memory database and reconcile every summary."""
    articles, occurrences, rejected = (frames[name] for name in ("articles", "occurrences", "rejected"))
    required = {
        "articles": {"article_id", "published_at", "category", "author", "url", "country", "language", "language_is_assumed"},
        "occurrences": {"article_id", "source_id", "source_name", "is_duplicate"},
        "rejected": {"source_id", "source_name", "rejection_reasons"},
    }
    for name, columns in required.items():
        if missing := columns - set(frames[name].columns):
            raise ValueError(f"{name} missing columns: {sorted(missing)}")
    if not articles.article_id.is_unique or articles.article_id.isna().any():
        raise ValueError("Silver canonical IDs must be unique and non-null")
    if articles.published_at.isna().any():
        raise ValueError("Missing canonical publication date")
    if not occurrences.article_id.isin(articles.article_id).all():
        raise ValueError("Occurrence references an unknown article")
    if set(occurrences.article_id) != set(articles.article_id):
        raise ValueError("Canonical article lacks occurrences")
    n, valid, bad = len(articles), len(occurrences), len(rejected)
    if (quality["canonical_rows"], quality["rejected_rows"], quality["input_rows"],
        quality["duplicate_occurrence_rows"]) != (n, bad, valid + bad, valid - n):
        raise ValueError("Silver quality counts disagree with its tables")
    if int(occurrences.is_duplicate.sum()) != valid - n:
        raise ValueError("Silver duplicate flags disagree with canonical count")

    with duckdb.connect(":memory:") as con:
        for name, frame in frames.items():
            con.register(name, frame)
        tables = {name: con.execute(sql).fetchdf() for name, sql in QUERIES.items()}
        metadata_parts = []
        for field in ("author", "url", "country", "language"):
            # Field names come from this fixed allowlist, never external input.
            available = f"{field} IS NOT NULL AND TRIM(CAST({field} AS VARCHAR)) <> ''"
            verified = available + (" AND NOT COALESCE(language_is_assumed, TRUE)" if field == "language" else "")
            metadata_parts.append(f"""
                SELECT '{field}' AS field, COUNT(*) AS total_articles,
                       COUNT(*) FILTER (WHERE {available}) AS populated_rows,
                       COUNT(*) FILTER (WHERE {verified}) AS verified_rows,
                       100.0 * COUNT(*) FILTER (WHERE {verified}) / NULLIF(COUNT(*), 0)
                           AS verified_completeness_pct
                FROM articles
            """)
        tables["metadata_completeness"] = con.execute(" UNION ALL ".join(metadata_parts) + " ORDER BY field").fetchdf()
    pct = lambda numerator, denominator: 100.0 * numerator / denominator if denominator else None
    tables["overview"] = pd.DataFrame([{
        "input_rows": valid + bad, "canonical_articles": n,
        "valid_occurrences": valid, "rejected_rows": bad,
        "additional_valid_occurrences": valid - n,
        "active_dataset_partitions": int(occurrences.source_id.nunique()),
        "rejection_rate_pct": pct(bad, valid + bad),
        "additional_occurrence_rate_pct": pct(valid - n, valid),
    }])
    for name in ("daily_articles", "category_distribution", "daily_category_articles"):
        if int(tables[name].article_count.sum()) != n:
            raise ValueError(f"{name}: article total does not reconcile")
    sources = tables["source_participation"]
    if int(sources.valid_occurrences.sum()) != valid or int(sources.rejected_rows.sum()) != bad:
        raise ValueError("Source participation totals do not reconcile")
    actual_reasons = dict(zip(tables["rejection_reasons"].reason,
                              tables["rejection_reasons"].rejected_rows_with_reason.astype(int)))
    if actual_reasons != quality["rejection_reason_counts"]:
        raise ValueError("Rejection reason totals do not reconcile")
    return tables


def run(root=ROOT):
    root = Path(root)
    frames, quality, lineage = load_silver(root)
    identity = {"version": VERSION, "code_sha256": file_hash(Path(__file__)),
                "duckdb_version": duckdb.__version__, "pandas_version": pd.__version__, **lineage}
    run_id = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    runs = root / "data" / "gold" / "runs"
    runs.mkdir(parents=True, exist_ok=True)
    destination = runs / run_id
    tables = aggregate(frames, quality)
    artifact_names = [name + ".parquet" for name in tables] + ["metric_definitions.json"]
    if destination.exists():
        manifest = json.loads((destination / "manifest.json").read_text())
        if manifest["run_id"] != run_id:
            raise ValueError("Existing Gold run identity mismatch")
        for name in artifact_names:
            if file_hash(destination / name) != manifest["artifacts"][name]:
                raise ValueError(f"Existing Gold artifact corrupt: {name}")
    else:
        staging = Path(tempfile.mkdtemp(prefix=".staging-", dir=runs))
        try:
            for name, frame in tables.items():
                path = staging / (name + ".parquet")
                frame.to_parquet(path, index=False)
                pd.testing.assert_frame_equal(frame, pd.read_parquet(path))
            write_json(staging / "metric_definitions.json", {
                "daily_articles": "One row per publication day present; canonical article counts; absent days are not filled.",
                "category_distribution": "One row per normalized canonical representative category.",
                "daily_category_articles": "One row per publication day/category combination present.",
                "source_participation": "Dataset partitions, not publishers. Distinct participation may overlap across sources; additional within-source occurrences include replays.",
                "overview": "Rejection denominator: physical input rows. Additional occurrence denominator: valid occurrences. Percentages use 0-100; empty denominators are null.",
                "metadata_completeness": "Verified rows divided by canonical articles; assumed language excluded. No composite quality score.",
                "rejection_reasons": "Rows with each reason; one rejected row can have multiple reasons.",
                "limitations": "No separate replay classification, hourly publication trends, live ingestion latency, or truth classification. Historical CSV dataset only.",
            })
            # Verify that the selected Silver artifacts stayed unchanged during computation.
            _, _, after = load_silver(root)
            if after != lineage:
                raise ValueError("Silver current run changed during Gold build")
            write_json(staging / "manifest.json", {
                **identity, "run_id": run_id,
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "output_rows": {name: len(frame) for name, frame in tables.items()},
                "artifacts": {name: file_hash(staging / name) for name in artifact_names},
            })
            staging.rename(destination)
        finally:
            if staging.exists():
                shutil.rmtree(staging)
    with tempfile.NamedTemporaryFile(mode="w", dir=runs.parent, prefix=".current-", delete=False) as stream:
        pointer = Path(stream.name)
        json.dump({"run_id": run_id, "path": "runs/" + run_id}, stream)
        stream.write("\n")
    pointer.replace(runs.parent / "current.json")
    print(json.dumps({"gold_run_id": run_id, "silver_run_id": lineage["silver_run_id"],
                      "output_rows": {name: len(frame) for name, frame in tables.items()},
                      "overview": tables["overview"].iloc[0].to_dict()}, indent=2))
    return destination


if __name__ == "__main__":
    run()
