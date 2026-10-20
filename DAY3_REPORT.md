# VeriTAS — Day 3 Implementation Report
## From Validated Silver Articles to Gold Analytics

**Project:** VeriTAS — Scalable News Aggregation & Analytics Data Pipeline  
**Scope:** CSV v1, local batch processing, Gold analytics with DuckDB and Parquet.  
**Report status:** Completed. Plan and walkthrough were prepared first; implementation and verification results are recorded below.  
**Inputs:** Exactly one completed Silver run, selected by `data/silver/current.json`.  
**Code:** New `src/build_gold.py`; Bronze and Silver implementation files remain unchanged.

## 1. Where Day 2 ended

Silver validated and deduplicated 134,694 physical Bronze rows. It produced 38,633 canonical articles, 132,771 valid occurrences, and 1,923 rejected rows. Of valid occurrences, 94,138 are additional appearances beyond the canonical representatives. These figures include repeated ingestion batches.

The current Silver run is `69446f64535553aa7c343de354ad9b138bac560dc4bda7d100682b7c40868bd5`. Its `manifest.json` stores checksums for the article, occurrence, rejected and quality files. Gold must read that one completed snapshot, not combine every historical Silver run.

## 2. What Gold means

**Analogy:** Bronze receives deliveries; Silver inspects individual items; Gold prepares the manager's daily summary sheets.

Silver answers “what is a usable article?” Gold answers “how many usable articles were published on each day?” Gold does not download news or repeat the cleaning rules. It groups and counts Silver records into small, dashboard-ready tables.

A table's **grain** means what one row represents. A daily table has one row per publication day present. A category table has one row per category. Confusing grains can silently double-count articles, especially when joining canonical articles to their many occurrences.

DuckDB is a local SQL analytics engine. This implementation uses an in-memory DuckDB connection, registers pandas tables, executes SQL and writes results to Parquet. No database server or persistent `.duckdb` file is necessary for this milestone.

## 3. Plan established before implementation

1. Verify Silver's pointer, manifest identity and artifact checksums.
2. Load canonical articles, valid occurrences, rejected rows and quality metadata.
3. Check canonical keys, occurrence references and row counts against the Silver quality report.
4. Run explicitly defined SQL aggregations.
5. Check grouped totals against Silver and check Parquet round trips.
6. Publish a completed Gold run and update its current pointer only after success.
7. Verify repeat-run behavior and preserve Bronze/Silver input bytes.

## 4. Output contract and metric definitions

| Output | One row represents | Important definition |
|---|---|---|
| `daily_articles.parquet` | A publication day present | Count canonical articles; days with no articles are not filled |
| `category_distribution.parquet` | A normalized category | Canonical representative's category; missing categories become `uncategorized` |
| `daily_category_articles.parquet` | A day/category combination present | Canonical article count |
| `source_participation.parquet` | A dataset source identity | Input and rejected rows, valid occurrences, distinct articles present and rates |
| `rejection_reasons.parquet` | A rejection reason | One record can contribute to several reasons |
| `metadata_completeness.parquet` | An optional metadata field | Populated and verified counts; assumed language is excluded from verified counts |
| `overview.parquet` | The selected Silver snapshot | Input, valid, canonical and rejected totals plus rates |

Additional outputs: `metric_definitions.json` describes the metrics, `manifest.json` records lineage/checksums, and `data/gold/current.json` points to the completed run under `data/gold/runs/<run_id>/`.

### Rate denominators

- Overall rejection rate = rejected physical rows / all physical input rows × 100.
- Overall additional-occurrence rate = additional valid occurrences / all valid occurrences × 100.
- Per-source rejection rate = rejected source rows / all source input rows × 100.
- Per-source additional-occurrence rate = (valid source occurrences − distinct source articles) / valid source occurrences × 100.
- Verified metadata completeness = canonical articles with verified nonblank field / all canonical articles × 100.

Percentages are on a 0–100 scale. An empty denominator produces null, because a rate over no records is undefined. Source participation can overlap: an article present in two sources counts in both source participation totals, but once in canonical article totals. Canonical representative categories may likewise differ from the raw categories on other occurrences.

## 5. Scope decisions and limitations

`Kaggle_RealNews` and `Kaggle_FakeNews` are dataset partitions, not original news publishers. Gold does not infer factual accuracy from their names. Additional occurrences include ingestion replays and original dataset duplication; the current Silver contract does not independently classify those two mechanisms. We report a precisely named combined metric and defer replay attribution.

Historical publication dates have day precision. Gold therefore avoids hourly publication trends and live ingestion latency. Assumed `en` is populated language metadata but not verified language metadata. Author, URL and country remain unavailable. No arbitrary composite quality score is invented.

Only one local writer should publish at a time. Distributed/concurrent orchestration remains outside v1. The new DuckDB dependency must be installed in the project's `.venv`; existing pandas and the Parquet engine are reused.

---

# VeriTAS — Code Walkthrough: Gold Layer
## Full implementation and block-by-block explanation

## 6. Full code: `src/build_gold.py`

The following is the exact implementation prepared for this milestone. Read its shape first, then use the explanation blocks below. `QUERIES` holds SQL; helper functions verify/read inputs; `aggregate()` calculates tables; `run()` publishes outputs.

```python
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
```

## 7. Block-by-block explanation

### Block 1 — Imports, paths and versions

`Path(__file__).resolve().parents[1]` locates the project root from a file saved under `src/`. Paths therefore do not depend on which folder started the terminal. `hashlib` makes fingerprints, `json` handles manifests, `tempfile` makes temporary locations and `shutil` removes an unfinished staging directory. The only new analytics dependency is `duckdb`; pandas transports tables to and from Parquet.

`VERSION` names the transformation. The run identity also includes a fingerprint of the code and runtime versions, so changing implementation or input creates a new run identity. This is traceability, not a complete environment lockfile.

### Block 2 — Understanding the SQL dictionary

`QUERIES` maps output names to SQL strings. Python loops over that dictionary to execute each query. Keeping the queries together makes their metrics reviewable.

```sql
SELECT CAST(published_at AS DATE) AS publication_date,
       COUNT(*) AS article_count
FROM articles
GROUP BY 1
ORDER BY 1;
```

Read this as: start with `articles`; turn the timestamp representation into a date; place records with the same date together; count each group; then sort the output by date. `AS` gives a readable column name. `COUNT(*)` counts rows, even if an unrelated field is null. `GROUP BY 1` refers to the first selected expression, and `ORDER BY 1` sorts by it.

**Example:** Three articles on May 1 and two on May 2 become two Gold rows: May 1 → 3, May 2 → 2. Five input article rows become two summary rows; no articles are being rejected here.

### Block 3 — Categories and missing values

`TRIM(category)` removes surrounding spaces. `NULLIF(value, '')` turns blank text into null. `COALESCE(value, 'uncategorized')` supplies a visible group name when a category is unavailable. This ensures even missing-category articles contribute to totals.

`GROUP BY 1, 2` in the daily-category query creates one group for every day/category pair. It does not mean “group first by day and then count the day again.” Each article belongs to exactly one pair in this table.

### Block 4 — Source participation without join multiplication

The source query first calculates two summaries in `WITH` clauses, also called common table expressions (CTEs). `valid` groups occurrences by source; `rejected_counts` groups invalid rows by source. Only then are those summaries joined.

Joining raw articles, occurrences and rejections on source before aggregation would multiply rows. Aggregating each side first avoids that mistake. `COUNT(DISTINCT article_id)` counts each article once within a source even if delivered several times.

A `FULL OUTER JOIN` retains sources appearing on either side, including a source with rejected records only. `IS NOT DISTINCT FROM` treats two null keys as matching for this comparison. `COALESCE(..., 0)` fills a missing side's count with zero. `NULLIF(denominator, 0)` prevents division by zero and gives a null rate instead.

**Example:** Source A has five valid appearances of three articles and one rejected row. Its input count is six, rejection rate is 1/6 × 100, and additional-occurrence rate is (5−3)/5 × 100. This does not determine how many original publishers duplicated the story.

### Block 5 — Counting overlapping rejection reasons

`STRING_SPLIT(rejection_reasons, ';')` makes a list from labels such as `missing_content;invalid_published_at`. `UNNEST(...)` expands the list into rows so each reason can be grouped and counted.

One invalid input with two reasons adds one to both reason counts but still represents one rejected row. Consequently, the sum of reason counts may exceed the rejected-row total. The current dataset's two reason categories do not overlap, but the query supports overlap.

### Block 6 — Checksums and the Silver pointer

`file_hash()` reads chunks and updates a SHA-256 digest. Input fingerprints identify exact files; they do not deduplicate article content in Gold. That task was already completed in Silver.

`load_silver()` reads `current.json` and requires its run ID to be a 64-character hexadecimal value. Its path must equal `runs/<run_id>` and resolve within the Silver runs directory. These checks catch malformed references rather than silently reading a different folder.

The manifest's run ID must agree with the pointer. Every expected artifact is checksum-verified before use. Missing files, changed bytes and invalid JSON fail explicitly; they are not silently skipped. The manifest fingerprint and Silver artifact hashes are copied into Gold lineage.

### Block 7 — Registering pandas tables in DuckDB

```python
with duckdb.connect(":memory:") as con:
    for name, frame in frames.items():
        con.register(name, frame)
```

`:memory:` creates a temporary database in process memory. `register()` exposes a DataFrame as a named relation SQL can read. The `with` block closes the connection automatically when processing ends, including after an error. `.execute(sql).fetchdf()` executes a query and returns its result as a pandas DataFrame.

The tables are modest in this v1, so loading them into memory is suitable. Later, DuckDB can read Parquet directly if memory or volume makes that useful; changing the reader need not change metric definitions.

### Block 8 — Validating the Silver contract again

`required` lists the columns each table needs. `columns - set(frame.columns)` finds absent columns. The `:=` expression assigns that result to `missing` and checks whether it is nonempty.

Canonical IDs must be unique and non-null; publication dates must exist. Every occurrence must reference a canonical article and every canonical article must have an occurrence. Input/rejected/canonical/duplicate counts must agree with Silver's quality report. These checks protect Gold against accidentally mixing files from different snapshots.

### Block 9 — Metadata completeness

The loop uses a fixed field list: author, URL, country and language. It builds one small SQL query per field and combines them with `UNION ALL`, which stacks result rows without removing any.

`COUNT(*) FILTER (WHERE condition)` counts only rows satisfying that condition. `populated_rows` counts nonblank values; `verified_rows` additionally excludes assumed language. `COALESCE(language_is_assumed, TRUE)` treats an unknown assumption flag cautiously as assumed. Other fields rely on Silver's documented removal of injected placeholders and unsupported country values.

The field names interpolated into SQL come from a fixed internal allowlist, not user input. The output is transparent per-field completeness, not a weighted “quality score.”

### Block 10 — Overview and zero denominators

The overview uses table lengths for totals. `pct` is a small lambda: return numerator/denominator × 100 when a denominator exists, otherwise return `None`, which is written as a missing value.

Active dataset partitions are counted from valid occurrences, so a partition with rejected rows only can appear in source participation without being counted as active. `valid − canonical` counts additional valid appearances across the entire snapshot. It differs from summing per-source extras if articles occur in multiple sources.

### Block 11 — Reconciliation before publishing

All daily, category and daily-category article counts must sum to the canonical article count. Source input components must sum to valid and rejected totals. Rejection reason counts must agree with the Silver quality report.

`raise ValueError(...)` stops the build immediately when an invariant fails. These checks are much stronger than merely seeing an output file exist: they verify the meaning of its totals.

### Block 12 — Saving a complete run safely

`run()` computes a deterministic run ID from input lineage, code and runtime versions. It calls `aggregate()` to build and check the tables. An existing run is checksum-verified and reused; it is not appended to.

For a new run, `tempfile.mkdtemp()` makes a staging directory. `.to_parquet(..., index=False)` writes data without pandas' row index. The saved file is read back and compared with the original table. Metric definitions and a manifest are then written. Silver is rechecked before publication so the build does not announce outputs after its selected inputs changed.

`staging.rename(destination)` publishes the completed directory on the same filesystem. A temporary `current.json` is then replaced atomically, so readers see a complete old or new pointer. A failure removes this build's incomplete staging directory; it never deletes Bronze, Silver or completed Gold runs. This local approach assumes one writer at a time and does not promise distributed transaction or power-loss durability.

### Block 13 — Execution and imports

`if __name__ == '__main__': run()` executes the pipeline only when launched as a script/module. Importing `aggregate()` for focused verification does not start the build. The intended command is:

```bash
cd /home/tharunn/Desktop/project_DE
.venv/bin/python -m src.build_gold
```

`run()` returns the completed output path; the printed JSON summarizes the selected run, output row counts and overview. Terminal text is a convenience; persisted tables and manifests are the durable artifacts.

## 8. Beginner learning checkpoints

1. Predict daily counts for five hand-written articles before running SQL.
2. Give an article two occurrences and compare `COUNT(*)` with `COUNT(DISTINCT article_id)`.
3. Give one rejected row two reasons and predict the reason table and rejected total separately.
4. Set language to `en` with an assumed flag and explain why populated and verified completeness differ.
5. Compare grouped sums to canonical totals, and explain why source participation may overlap.
6. Run the pipeline twice and confirm that a new snapshot is not appended each time.
7. Trace one output back through the Gold manifest to the exact Silver run.

## 9. Mini cheatsheet

| Construct | Meaning |
|---|---|
| `SELECT` / `FROM` | Choose output expressions / choose input table |
| `AS` | Name an expression or table alias |
| `GROUP BY` | Put rows sharing keys into groups |
| `COUNT(*)` | Count rows |
| `COUNT(DISTINCT ...)` | Count unique non-null values |
| `ORDER BY` | Sort the result |
| `WITH` | Name intermediate query results |
| `FULL OUTER JOIN` | Keep matching and unmatched rows from both sides |
| `COALESCE` | Use the first non-null value |
| `NULLIF(x, 0)` | Return null when x is zero |
| `FILTER (WHERE ...)` | Restrict an aggregate to matching rows |
| `UNNEST` | Expand list elements into rows |
| `UNION ALL` | Stack query results without deduplication |
| `con.register()` | Expose a DataFrame to SQL |
| `fetchdf()` | Return a SQL result as a DataFrame |

---

# Day 3 — Execution Results and Verification

## 10. Environment preparation and concrete work completed

DuckDB was absent from the existing virtual environment. The environment also had no `pip` module, so the existing `uv` executable was used to install DuckDB into `.venv`:

```bash
uv pip install --python /home/tharunn/Desktop/project_DE/.venv/bin/python 'duckdb>=1.0,<2'
```

Installation resolved **DuckDB 1.5.6**. Existing pandas was **3.0.6** and PyArrow was **25.0.1**. `requirements.txt` records these exact direct runtime versions for the CSV v1 layers. Install them with:

```bash
uv pip install --python .venv/bin/python -r requirements.txt
```

These are the observed working versions, not a full transitive dependency lock. The runtime used Python 3.12.3. Only DuckDB was newly installed during this milestone.

The report and proposed code were prepared first, then saved as `DAY3_REPORT.md` and `src/build_gold.py`. The actual Gold module was executed from the repository root. Bronze ingestion and Silver transformation source were left unchanged; no Bronze or Silver rebuild was run.

## 11. Measured Gold output

Gold run ID: `cccd1c4d487efd3c750b9e1cedeef1dd13256041b4b388bb31f4c3392a9b1686`.

| Output table | Rows |
|---|---:|
| Daily publication counts | 1,010 |
| Category distribution | 6 |
| Daily/category counts | 3,151 |
| Source participation | 2 |
| Rejection reasons | 2 |
| Metadata completeness | 4 |
| Overview | 1 |

The 1,010 daily rows represent dates present, not every calendar day between the first and last article. The publication range is 2015-03-31 through 2018-02-19. Daily, category and daily/category article counts each sum independently to **38,633**.

| Overview metric | Value |
|---|---:|
| Physical input rows | 134,694 |
| Valid occurrences | 132,771 |
| Canonical articles | 38,633 |
| Rejected rows | 1,923 |
| Additional valid occurrences | 94,138 |
| Active dataset partitions | 2 |
| Rejection rate | 1.4276805203% |
| Additional-occurrence rate among valid appearances | 70.9025314263% |

### Category distribution

| Category | Canonical articles |
|---|---:|
| politics | 17,627 |
| world | 9,978 |
| News | 9,050 |
| US_News | 783 |
| left-news | 681 |
| Government News | 514 |

These categories reflect the canonical representative selected in Silver. Raw input labels not represented here should not be invented as zero-count “observed categories.”

### Dataset-partition participation

| Partition | Input rows | Valid appearances | Distinct articles present | Rejected rows |
|---|---:|---:|---:|---:|
| Kaggle_RealNews | 64,251 | 64,248 | 21,190 | 3 |
| Kaggle_FakeNews | 70,443 | 68,523 | 17,443 | 1,920 |

For this snapshot, the distinct partition counts sum to the canonical total. The pipeline does not assume this must hold for future overlapping sources. Per-source additional-occurrence rates are approximately 67.0184% and 74.5443%, respectively; rejection rates are approximately 0.00467% and 2.72561%.

Rejected reason counts are **1,893 missing-content rows** and **30 invalid-publication-date rows**. Author, URL and country each have zero populated/verified canonical rows. Language has 38,633 populated rows but zero verified rows because all language values are assumed. Thus verified completeness is 0% for each of the four measured optional metadata fields; this is not a statement that required article fields are missing.

## 12. Verification performed and its evidence

Focused synthetic fixtures were executed from a temporary verification script. They covered:

1. Correct daily grouping and a missing category assigned to `uncategorized`.
2. An article appearing in multiple source partitions, so summed distinct source participation can exceed canonical total.
3. A source with rejected rows only, retained by the full outer join.
4. One rejected row with two reasons, counted once as rejected but once under each reason.
5. Populated assumed language excluded from verified completeness; genuinely unassumed language included.
6. Empty tables producing null percentage denominators without a divide-by-zero failure.
7. Inconsistent Silver quality counts failing before output publication.
8. Two identical temporary-project runs reusing the same run directory with unchanged artifact/manifest checksums.
9. Deliberately changed input bytes being rejected by checksum validation.

Full-data verification then reloaded the selected Silver snapshot, checked its artifact hashes, rebuilt all aggregates, checked every Gold artifact checksum, and compared every saved Parquet table to the freshly calculated DataFrame. A second SQL aggregation produced identical tables. All checks passed. The focused verification script was temporary rather than added as a permanent automated test suite.

The source integrity check confirmed `ingest_bronze.py` still matches its committed content. `transform_silver.py` retained SHA-256 `bc85ae7417fd07cbdbc6517eb92ecf3bafde1d43af54887a7d62c9da8d55fa23`, which also matches the module fingerprint recorded by the completed Silver run. Gold reads and fingerprints Silver data but does not modify it. Bronze data is not read or written by Gold.

## 13. Completion boundary and remaining limitations

Completed artifacts: the Day 3 learning/report document, new Gold implementation, pinned direct runtime requirements, seven Gold Parquet tables, definitions, manifest and current pointer. No Git commit/push, dashboard, new acquisition adapter or cloud deployment was performed.

Canonical and grouped metrics are reproducible for unchanged input/code/runtime versions. A current pointer selects the active completed run. If a future Silver run is published, rerun Gold before using it for the dashboard. The pipeline has one-writer local semantics and a full-snapshot model; it does not implement incremental SQL updates or concurrent scheduler coordination.

---

# Day 4 — Upcoming Phase: Dashboard

**Entry condition:** Gold must be published and its totals verified. Day 4 will make the completed analytics understandable to a human through a local Streamlit dashboard.

Planned work:

1. Create `app/dashboard.py` and read one completed Gold run through `data/gold/current.json`; verify its manifest and show which dataset/run is being viewed.
2. Add cards for canonical articles, valid dataset partitions, rejected-row rate and additional-occurrence rate. Make the latter's replay-inclusive meaning visible.
3. Add daily publication and category charts using the existing aggregate tables. Fill missing dates with zero only as an explicit presentation choice.
4. Add source participation, rejection reason and metadata completeness views. Present assumed metadata separately from verified completeness.
5. Use daily-category aggregates for date/category filtering. Define which cards respond to filters; global rejection/completeness cards cannot be filtered by article date using the current Gold tables. Add scoped Gold outputs first if filtered versions are required.
6. Add an empty/missing-data screen with instructions to run the pipeline. Avoid triggering ingestion or changing lake data from dashboard reads.
7. Verify displayed totals against Gold, test filter behavior and write a beginner walkthrough plus actual validation results in `DAY4_REPORT.md`.

CSV remains the only acquisition mechanism. RSS/API adapters come after the local Bronze → Silver → Gold → dashboard path is demonstrated. Dashboard work is planned here only; no dashboard is implemented in Day 3.
