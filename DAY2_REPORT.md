# VeriTAS — Day 2 Repository Review and Silver Layer Plan

**Review date:** 2026-10-20 (Asia/Kolkata)  
**Scope completed today:** Read the Day 1 report, inspect current code and Git state, read the project architecture PDF, profile local CSV/Bronze data, and define the next implementation milestone.  
**Implementation status:** Silver is planned below; no Silver transformation or Gold/dashboard implementation was performed during this review.

## 1. Project purpose and v1 boundary

The PDF, `Design and Implementation of a Scalable News Aggregation.pdf`, describes a news aggregation and analytics platform: collect heterogeneous inputs, store Bronze, clean/transform/deduplicate into Silver, curate Gold, and expose analytics through a database/dashboard. Its 14-field article schema matches `TARGET_COLUMNS` in `config.py`.

For v1, use the downloaded `True.csv` and `Fake.csv` only. Prove the entire local batch pipeline with pandas, Parquet, DuckDB, and a dashboard before adding RSS/API adapters. Fake/real dataset labels do not make this a misinformation classifier; the requested product is aggregation and data analytics.

The PDF asks for volume, category, source, duplicate, quality, completeness, and latency analytics. Some cannot be interpreted honestly from this dataset: the two configured sources are dataset partitions, not original publishers; author and URL are unavailable; publication times have day precision; historical publication-to-ingestion gaps are backfill age rather than live collection performance.

## 2. Current repository status, verified locally

- Location: `/home/tharunn/Desktop/project_DE`.
- Latest local commit: `4a978f3 Update bronze ingestion pipeline`; preceding commits include `c20944f` and `e3238eb`.
- Working tree was clean before this report was created. Remote synchronization was not checked over the network.
- Tracked files: `.gitignore`, `DAY1_REPORT.md`, `app/.gitkeep`, `config.py`, `src/__init__.py`, and `src/ingest_bronze.py`.
- Bronze ingestion exists and produces the ordered 14-column schema. Silver and Gold folders are empty; the app has no implementation.
- Bronze now contains six Parquet files: one file per source on October 11, 12, and 19. This is more data on disk than the single batch described in Day 1.
- For each source, title, content, category, raw date, and content hash match exactly across its three batches. UUIDs are new on ingestion, so unique article IDs do not detect replayed articles.
- Existing pandas in `.venv`: 3.0.6. No dependency lock/manifest is tracked.

## 3. Data profiling performed today

Read both CSVs with `pd.read_csv(..., on_bad_lines='error')`: both loaded successfully, with 21,417 True rows and 23,481 Fake rows. This verifies the current CSVs parse strictly; it does not prove every date or article is semantically valid.

Read each Bronze file, checked its column list, concatenated all six files, checked blank content/title, parsed publication dates with `pd.to_datetime(..., format='mixed', errors='coerce')`, and inspected non-null content-hash duplicates. Profiling was read-only; Bronze was not rerun or modified.

| Check | One original batch | All current Bronze files |
|---|---:|---:|
| Input rows | 44,898 | 134,694 |
| Empty/whitespace body | 631 | 1,893 |
| Missing content hash | 631 | 1,893 |
| Empty title | 0 | 0 |
| Unparseable publication date | 10 | 30 |
| Empty-body/bad-date overlap | 0 | 0 |
| Rejected by proposed required-field/date rules | 641 | 1,923 |
| Valid before deduplication | 44,257 | 132,771 |
| Excess valid rows by existing raw-body MD5 | 5,619 | 94,133 |
| Valid unique raw-body hashes | 38,638 | 38,638 |

Across all Bronze files, there are 94,157 excess non-null raw-body hashes before validation and no duplicate UUIDs. Parsed publication dates range from 2015-03-31 to 2018-02-19. Bad date values include image/article URLs and a headline; these must be quarantined, not guessed into dates.

All author values are `Unknown`; all URL values are `N/A`; all language values are `en`; all country values are `US`. The latter two were injected by ingestion and are assumptions, not observations. Category values are `politicsNews`, `worldnews`, `News`, `politics`, `left-news`, `Government News`, `US_News`, and `Middle-east`.

**These are measured baseline counts, not promised future Silver counts.** Normalized-body hashing can merge additional rows; its results must be measured when implemented.

## 4. Corrections to Day 1 assumptions

1. Timestamped append-only files preserve ingestion history, but do not make ingestion idempotent. Replays currently triple the input. Silver must produce the same canonical result when replayed inputs are added.
2. `Unknown`, `N/A`, and assumed geography must not inflate metadata completeness. Convert unavailable values to null in Silver and retain the originals in Bronze.
3. A timezone-free `datetime.now()` string is not UTC. Record that existing batches used the local environment, Asia/Kolkata; convert with that explicit assumption and document it. Future ingestion should emit timezone-aware UTC timestamps.
4. Midnight dates are not evidence of publication at midnight. Do not build hourly publication trends or live ingestion-latency claims from these CSVs.
5. `on_bad_lines='skip'` can hide structural ingestion loss. Current strict parsing succeeded, but future ingestion should fail loudly or record malformed-row counts.
6. The current description can become `nan...` for empty content. Rebuild descriptions only after validating content.

## 5. Recommended next milestone: a reproducible, auditable Silver batch

**Goal:** Transform the existing Bronze snapshots into validated, typed, deduplicated articles, with complete row accounting and evidence for every rejection or duplicate. This is the best next step because Gold/dashboard metrics would otherwise count replays and placeholders as real information.

### Step 1 — Make input selection and schema explicit

Create `src/transform_silver.py`, runnable as `.venv/bin/python -m src.transform_silver` from the repository root. Resolve paths relative to the repository, not the caller's working directory. Read a sorted list of Bronze Parquet files and fail clearly on no inputs, unreadable files, or missing required schema columns. Do not silently skip bad files.

Record input filenames, file SHA-256 values, row counts, transformation version, rule version, and relevant settings in a run manifest. Existing Bronze has no source row number: add `bronze_file` and zero-based `bronze_row_number` on reading for lineage. Do not infer a shared ingestion timestamp from filename timestamps; file and row metadata can differ.

### Step 2 — Normalize without destroying evidence

Keep Bronze immutable. Trim surrounding title/category whitespace; normalize line endings and outer whitespace in content while preserving prose, punctuation, and case. Rebuild a description from valid content, adding an ellipsis only when truncated.

Parse mixed publication-date strings into a typed date; record day-level precision. Convert existing ingestion timestamps from documented Asia/Kolkata local time to UTC. Keep raw dates and original values in lineage/rejected output.

Convert author/URL placeholders to null. For these two known Kaggle sources, set unsupported country to null and mark language as assumed if retaining `en`. Keep a raw category and an explicit versioned category map. A cautious initial map can combine `politicsNews`/`politics` into `politics`, map `worldnews` to `world`, and retain other source labels pending review; never force vague `News` or `left-news` into an invented factual topic.

### Step 3 — Validate and quarantine

Require nonblank title/content, usable source identity, valid publication date, and parseable ingestion timestamp. Invalid records go to a rejected Parquet output with original values, lineage, and all applicable rejection reasons. Missing author/URL/country is expected optional metadata, not grounds to discard an article. Never repair a malformed date by guessing.

Count rejected rows once even if they have multiple reasons; maintain separate reason counts that may overlap. Determine validity before canonical duplicate selection so an invalid copy cannot displace a valid one.

### Step 4 — Deduplicate deterministically

For v1 use exact normalized-body matches, not fuzzy/semantic matching. Generate a versioned SHA-256 hash after documented normalization; keep the original Bronze MD5 for traceability. A hash is an equality key, not a fact-checking verdict.

Choose the representative by earliest UTC ingestion timestamp, then Bronze filename and row number as stable tie breakers. Generate the canonical article ID deterministically from the normalized content key instead of the random Bronze UUID. Preserve every valid input occurrence in an occurrence/duplicate table pointing to the canonical ID, including source identity and original UUID. This retains cross-source evidence even if one article appears in both partitions.

Distinguish ingestion replay from within-source dataset duplicates by carrying file/row lineage. For the current files, the verified identical row ordering permits explicit replay analysis; for future adapters, use a stable upstream record ID or ingestion key. Never call all repeated input rows publisher duplication.

### Step 5 — Write outputs and an auditable quality summary

Proposed artifacts under ignored `data/silver/runs/<run_id>/`:

- `articles.parquet`: canonical valid articles, typed columns, normalized hash and provenance fields.
- `rejected.parquet`: invalid input rows and reasons.
- `occurrences.parquet`: every valid input row linked to its canonical article, with a duplicate flag.
- `quality_report.json`: global and per-source counts, missing-metadata rates, date range, validation rules and hash version.
- `manifest.json`: exact inputs and transformation settings.

Write to a staging run directory; publish a completed run/current pointer only after all invariants pass. Downstream jobs read that one completed run, never glob all historical Silver runs. Identical input and rules should resolve to the same logical run identity or safely reuse the completed result; do not append canonical rows on every execution. Publication timestamps in manifests may vary without changing the canonical data.

Use a simple full rebuild for this 45k-record dataset. Incremental state, orchestration and distributed compute are unnecessary for this milestone.

### Step 6 — Verify before calling Silver complete

Add focused fixtures for mixed date formats, malformed dates, blank bodies, missing schema, placeholder metadata, normalized-body equality, cross-source duplicates, replayed batches, and deterministic representative selection.

Acceptance conditions:

- Bronze files remain byte-for-byte unchanged.
- Every physical input row is either rejected or present exactly once in occurrences.
- `input_rows = rejected_rows + canonical_rows + duplicate_occurrence_rows`.
- All canonical rows have valid required fields, typed dates/timestamps, and unique IDs and normalized hashes.
- All occurrence references resolve; all published artifacts belong to one completed manifest.
- Re-running identical inputs produces identical canonical data, counts and IDs.
- Adding an identical ingestion replay does not increase canonical article count; replay/occurrence counts do increase transparently.
- Missing optional metadata remains missing and affects completeness honestly.
- Compare measured output to today's 38,638 valid raw-hash baseline; explain any reduction introduced by normalization rather than hard-coding that count.

## 6. Sequence after Silver

1. **Gold:** use DuckDB over one completed Silver run. Produce daily publication volumes, category distribution, dataset-partition counts, validation rates, metadata completeness, and separately defined replay/content-duplicate rates. Store aggregates as Parquet.
2. **Dashboard:** present article totals, dataset partitions, category trends and quality results; expose metric definitions and dataset limitations. Defer hourly trends and live collection latency until source timestamps support them.
3. **Reproducibility:** pin the required runtime dependencies and document clean-environment commands before claiming v1 can run on another machine. Keep data, environment files, credentials and generated lake artifacts ignored.
4. **RSS/API extension:** add acquisition adapters that land raw payloads and real source identity, upstream IDs/URLs, timezone-aware timestamps and collection metadata. Map each adapter to the shared article contract, then reuse Silver validation and Gold logic. Add retries, rate-limit handling and incremental ingestion state when live collection begins.
5. **Scale when justified:** move storage to object storage and scheduling to an orchestrator after local correctness is proven. Keep business transformation rules independent of transport.

## 7. Work completed versus work remaining

Completed today: read the entire Day 1 report and both PDF pages; inspect configuration, Bronze ingestion, tracked files and local Git history; verify six Bronze schemas and both strict CSV reads; measure quality issues and duplicates; establish replay equivalence; and document the Silver contract, sequence, artifacts and completion criteria here.

Remaining: implement and run Silver, write its focused verification checks, and record actual normalized/deduplicated output counts. No new datasets were downloaded, no Bronze ingest was executed, no existing code or data was changed, and no Git commit or push was performed during this review. This report is the sole intended repository change.

---

# VeriTAS — Code Walkthrough: Silver Layer
## Beginner learning companion to the implementation plan

**Status:** The code below is a proposed teaching implementation of the core transformation. It has not been installed as `src/transform_silver.py` or run against the full dataset. The production input manifest, staging/publication mechanism, per-source quality summaries and replay classification specified above remain implementation tasks. This section explains the internals before those tasks are undertaken; it does not claim Silver is complete.

## 8. First understand what Silver does

Think of Bronze as a shelf holding every delivery exactly as received by the ingestion pipeline. Silver is the inspection desk: it opens each delivery, checks the fields, puts usable records into a consistent format, sets aside unusable records, and identifies repeated articles.

A **DataFrame** is pandas' in-memory table. A **Series** is one column. A **Boolean mask** is a column of `True`/`False` values that tells pandas which rows to select. A **null** means a value is unavailable. A **canonical article** is the single representative we choose for a group of matching article bodies. An **occurrence** is one appearance of that article in an input file.

The important separation is between invalid records and duplicates. An empty article body is invalid. Two usable rows with the same normalized body are duplicates. A duplicate still contains useful source and ingestion history, so its occurrence is retained.

```text
Bronze files
    -> load + attach file/row references
    -> normalize + parse types
    -> validate
         -> invalid rows: rejected table + reasons
         -> valid rows: hash + deterministic sorting
              -> one representative per body: articles table
              -> every valid appearance: occurrences table
    -> check row accounting
    -> publish a completed Silver run (implementation task)
```

## 9. Proposed core code, in one place

Read the full code once to see its shape, then use the block explanations below. This example uses the existing `TARGET_COLUMNS` contract and pandas available in the project's virtual environment. It intentionally returns tables instead of writing files: the publication wrapper must implement the run layout and completion rules in Section 5 before this becomes the runnable Silver pipeline.

```python
from pathlib import Path
import hashlib
import pandas as pd

from config import TARGET_COLUMNS

# When placed in src/transform_silver.py, go up to the project root.
ROOT = Path(__file__).resolve().parents[1]
HASH_VERSION = "body-v1"
CATEGORY_MAP = {
    "politicsNews": "politics",
    "politics": "politics",
    "worldnews": "world",
}
KAGGLE_SOURCES = {"Kaggle_RealNews", "Kaggle_FakeNews"}


def normalize_body(value):
    if pd.isna(value):
        return pd.NA
    return str(value).replace("\r\n", "\n").replace("\r", "\n").strip()


def body_hash(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def load_bronze(folder):
    paths = sorted(folder.glob("*.parquet"))
    if not paths:
        raise FileNotFoundError(f"No Bronze Parquet files in {folder}")

    frames = []
    for path in paths:
        frame = pd.read_parquet(path)
        missing = set(TARGET_COLUMNS) - set(frame.columns)
        if missing:
            raise ValueError(f"{path.name}: missing columns {sorted(missing)}")
        frame["bronze_file"] = path.relative_to(ROOT).as_posix()
        frame["bronze_row_number"] = range(len(frame))
        frames.append(frame)
    return pd.concat(frames, ignore_index=True)


def transform(bronze):
    df = bronze.copy()
    df = df.rename(columns={
        "article_id": "bronze_article_id",
        "content_hash": "bronze_content_hash",
    })
    df["published_at_raw"] = df["published_at"]
    df["ingested_at_raw"] = df["ingested_at"]
    df["category_raw"] = df["category"]

    for column in ["title", "category", "source_name"]:
        df[column] = df[column].astype("string").str.strip()
    df["content"] = df["content"].map(normalize_body).astype("string")
    df["category"] = df["category"].replace(CATEGORY_MAP)

    df["published_at"] = pd.to_datetime(
        df["published_at_raw"].astype("string").str.strip(),
        format="mixed", errors="coerce",
    ).dt.normalize()
    df["publication_precision"] = "day"

    # This parser is for the existing timezone-free Bronze strings only.
    local_time = pd.to_datetime(
        df["ingested_at_raw"],
        format="%Y-%m-%d %H:%M:%S", errors="coerce",
    )
    df["ingested_at"] = (
        local_time.dt.tz_localize("Asia/Kolkata").dt.tz_convert("UTC")
    )

    for column in ["author", "url"]:
        values = df[column].astype("string").str.strip()
        missing = values.str.lower().isin(["", "unknown", "n/a"])
        df[column] = values.mask(missing)
    kaggle = df["source_name"].isin(KAGGLE_SOURCES)
    df.loc[kaggle, "country"] = pd.NA
    df["language_is_assumed"] = kaggle

    reasons = pd.Series("", index=df.index, dtype="string")

    def reject_when(mask, label):
        reasons.loc[mask] = reasons.loc[mask] + label + ";"

    for column in ["title", "content", "source_name"]:
        reject_when(
            df[column].fillna("").eq(""), f"missing_{column}"
        )
    source_id = pd.to_numeric(df["source_id"], errors="coerce")
    reject_when(
        ~source_id.isin([1, 2]), "invalid_source_id"
    )
    expected_source = source_id.map({1: "Kaggle_RealNews", 2: "Kaggle_FakeNews"})
    reject_when(
        df["source_name"].ne(expected_source).fillna(True),
        "source_identity_mismatch",
    )
    reject_when(df["published_at"].isna(), "invalid_published_at")
    reject_when(df["ingested_at"].isna(), "invalid_ingested_at")

    invalid = reasons.ne("")
    # Start rejected output from Bronze, so original values remain available.
    rejected = bronze.loc[invalid].copy()
    rejected["rejection_reasons"] = reasons.loc[invalid].str.rstrip(";")
    valid = df.loc[~invalid].copy()

    text = valid["content"]
    valid["description"] = text.str[:200] + text.map(
        lambda value: "..." if len(value) > 200 else ""
    )
    valid["content_hash"] = text.map(body_hash)
    valid["hash_version"] = HASH_VERSION
    valid["article_id"] = HASH_VERSION + ":" + valid["content_hash"]

    valid = valid.sort_values([
        "ingested_at", "bronze_file", "bronze_row_number"
    ])
    valid["is_duplicate"] = valid.duplicated("article_id", keep="first")
    articles = valid.loc[~valid["is_duplicate"]].copy()
    occurrences = valid[[
        "article_id", "bronze_article_id", "bronze_content_hash",
        "source_id", "source_name", "bronze_file", "bronze_row_number",
        "ingested_at", "published_at_raw", "category_raw", "is_duplicate",
    ]].copy()

    counts = {
        "input_rows": len(bronze),
        "rejected_rows": len(rejected),
        "canonical_rows": len(articles),
        "duplicate_occurrence_rows": int(occurrences["is_duplicate"].sum()),
    }
    accounted = (
        counts["rejected_rows"] + counts["canonical_rows"]
        + counts["duplicate_occurrence_rows"]
    )
    if counts["input_rows"] != accounted:
        raise ValueError("Silver row accounting failed")
    if not articles["article_id"].is_unique:
        raise ValueError("Canonical article IDs are not unique")
    return articles, rejected, occurrences, counts
```

**Scope of this example:** Source IDs 1 and 2 and their names are checked explicitly because this is the CSV v1. When RSS/API inputs arrive, replace this fixed lookup with a configured source registry. The publication parser assumes the current date-only dataset, and the ingestion parser assumes the current local-time format. Future timezone-aware sources need their own explicit timestamp mapping before entering shared processing.

## 10. Block-by-block explanation

### Block 1 — Imports and the project address

```python
ROOT = Path(__file__).resolve().parents[1]
```

**Analogy:** The script carries its own address book, so it can find the warehouse regardless of where the terminal started.

**Literally:** `__file__` is the script's filename. `resolve()` makes an absolute path. `parents[0]` is `src/`; `parents[1]` is `project_DE/`. `ROOT / "data" / "bronze"` builds the input folder path. `hashlib` creates fingerprints; pandas supplies the tables and transformations. `__file__` is intended for a saved Python file, not a directly pasted notebook cell.

`HASH_VERSION` names the normalization/hashing rule. If the rule changes later, a version makes the change visible. `CATEGORY_MAP` is an explicit translation dictionary; an unlisted category is retained rather than guessed.

### Block 2 — Cleaning an article body

```python
return str(value).replace("\r\n", "\n").replace("\r", "\n").strip()
```

**Analogy:** Straighten a sheet of paper and trim its blank edges without rewriting its story.

Some systems use different line-ending characters. The replacements give them one representation; `strip()` removes whitespace at the beginning and end. Internal spaces, paragraph structure, case and punctuation remain intact. `pd.isna()` is checked first so a missing body stays missing instead of becoming the literal text `"nan"`.

For example, `"  Hello\r\nworld  "` becomes `"Hello\nworld"`. `"Hello world"` and `"hello world"` remain different. Our duplicate definition is deliberately exact after these limited changes.

### Block 3 — Loading files and checking the contract

`folder.glob("*.parquet")` finds matching files; `sorted(...)` gives them a predictable order. An empty list triggers `FileNotFoundError` instead of a misleading successful empty run.

`set(TARGET_COLUMNS) - set(frame.columns)` asks: which required column names are absent? `raise ValueError(...)` stops processing and explains the problem. This checks column presence; later validation checks the values inside them.

`frames.append(frame)` collects each file's table. `pd.concat(..., ignore_index=True)` stacks the tables vertically and creates a fresh row index. It does not remove duplicates.

The filename and zero-based row number are the record's **lineage**: a pointer back to its exact Bronze location. Row 0 means the first data row in that Parquet file, not the first physical line of the original CSV, whose quoted bodies may contain line breaks.

### Block 4 — Copying before changing and preserving original fields

`df = bronze.copy()` creates a separate working table. Assigning new values to this table does not rewrite the source Parquet files. Renaming the old UUID and MD5 makes their meaning clear: they came from Bronze. New canonical IDs and normalized hashes will have different meanings.

The `_raw` columns retain values before parsing or category mapping. Rejected output starts from the original `bronze` table, so inspection can recover the incoming title, content, metadata and dates as well as its rejection reasons.

### Block 5 — String columns and category mapping

`astype("string")` uses pandas' nullable string type. `.str.strip()` applies whitespace trimming to every cell without writing a manual row loop. `.map(normalize_body)` calls our helper once for each body. `.replace(CATEGORY_MAP)` translates only the category labels listed in the dictionary.

The raw category remains available. Grouping two labels is a business rule that should be reviewed, not an automatic discovery by pandas.

### Block 6 — Parsing publication dates

```python
pd.to_datetime(values, format="mixed", errors="coerce")
```

A CSV string such as `"December 31, 2017 "` is text. Date parsing turns it into a typed timestamp that can be filtered and grouped correctly. `format="mixed"` allows the existing date formats to be parsed individually. `errors="coerce"` converts unparseable values into `NaT`, pandas' missing timestamp marker. It does not repair them; the validation block rejects them explicitly.

`.dt.normalize()` sets the time portion to midnight for daily grouping. This example stores a day represented by a timestamp; it does not claim that the article was published at midnight. `publication_precision = "day"` communicates that limitation. A production schema can instead use an Arrow date type when writing Parquet.

### Block 7 — Understanding ingestion timezones

`tz_localize("Asia/Kolkata")` tells pandas which timezone the old clock readings belonged to. `tz_convert("UTC")` expresses those same instants in UTC. These operations serve different purposes: assigning context, then changing representation.

For example, 19:05:33 in Asia/Kolkata corresponds to 13:35:33 UTC on the same day. The conversion is based on the documented environment assumption; the stored Bronze string itself contains no timezone evidence. A future RSS/API adapter must preserve the timezone supplied by the source rather than reuse this assumption.

### Block 8 — Honest missing metadata

`values.str.lower().isin([...])` builds a mask for placeholders regardless of letter case. `values.mask(missing)` replaces those placeholders with null. An actual author name is retained.

`df.loc[kaggle, "country"] = pd.NA` clears the unsupported country assumption only for these dataset partitions. `language_is_assumed` records that their `en` label was injected, not measured. Downstream completeness metrics must exclude assumed language from verified metadata completeness.

### Block 9 — Building rejection reasons

`reasons` starts as an empty string for every input row. The nested `reject_when()` function adds a label wherever its mask is true. It can access and update the existing `reasons` Series; it does not create a new table for each rule.

`.fillna("").eq("")` treats missing values and empty strings as blank. `pd.to_numeric(..., errors="coerce")` turns unusable source IDs into missing numeric values. The mapping verifies that source ID 1 really names `Kaggle_RealNews` and source ID 2 names `Kaggle_FakeNews`.

A row can accumulate `missing_content;invalid_published_at`. We retain both reasons, but `invalid = reasons.ne("")` marks it once. `.loc[invalid]` selects rejected rows; `.loc[~invalid]` selects valid rows. Here `~` means logical NOT, reversing the Boolean mask.

### Block 10 — Descriptions and fingerprints

A lambda is a small unnamed function. Here it adds an ellipsis only when the body is longer than 200 characters. The description is generated after validation, so missing content cannot become `nan...`.

`text.encode("utf-8")` converts text to bytes. SHA-256 consumes those bytes, and `.hexdigest()` returns a 64-character hexadecimal fingerprint. Matching fingerprints are our practical exact-body grouping key. They do not prove that two titles, sources or truth labels agree.

The canonical ID is `body-v1:` followed by the fingerprint. Repeated ingestion creates fresh Bronze UUIDs but the same normalized text produces the same canonical ID. If the normalization rule changes, version the ID rule and plan the migration.

### Block 11 — Choosing one representative and retaining occurrences

`sort_values([...])` first orders by ingestion time; ties are resolved by filename and row number. `duplicated("article_id", keep="first")` returns `False` for the first appearance of each ID and `True` for later ones. Selecting `~is_duplicate` gives one representative per body.

Suppose article A appears at rows 5 and 20 in one snapshot and both rows are ingested again in another snapshot. The result is one canonical article and four occurrences, of which three are marked duplicate occurrences. Further replay analysis distinguishes the repeated delivery from duplication already present inside the original snapshot.

The canonical representative's source alone cannot answer “which dataset partitions contained this article?” Use occurrences for that question. Otherwise, selecting one representative could hide a cross-source match.

### Block 12 — Row accounting as a safety check

The four counts are a receipt for the transformation:

```text
input = rejected + canonical + additional valid occurrences
```

For today's existing raw-hash baseline:

```text
134,694 = 1,923 + 38,638 + 94,133
```

This illustrates accounting using measured raw-hash profiling; the teaching code's normalized hashes may yield different counts. Never paste these numbers in as expected output without measuring the actual implementation.

`int(...)` converts pandas' count into a normal Python integer, useful when serializing JSON. `raise ValueError(...)` prevents a bad result from being published. `return` hands four objects back to the caller; it does not save them to disk.

## 11. Connecting the core to a complete pipeline

The intended caller will load input, invoke the transformation, check the additional invariants in Section 5, and publish the outputs:

```python
bronze = load_bronze(ROOT / "data" / "bronze")
articles, rejected, occurrences, counts = transform(bronze)
```

Tuple unpacking assigns the four returned objects to four names in the same order. After the publication wrapper is implemented, it will use calls such as `articles.to_parquet(output_path, index=False)` to save a table without adding pandas' row index as an extra column.

That wrapper still needs to compute input-file SHA-256 fingerprints, record settings, create a staging directory, write every artifact, validate them together, and atomically publish the completed-run reference. Input-file fingerprints identify exact source files; body fingerprints identify repeated article content. They answer different questions even though both use SHA-256.

Do not create a script by copying only the two caller lines: this report has intentionally kept publication as a separate implementation task. The intended command `.venv/bin/python -m src.transform_silver` becomes usable only after that module and its `main()` function exist. As explained in Day 1, `if __name__ == "__main__": main()` lets direct execution run the pipeline while imports only expose its functions.

## 12. How to learn and verify this implementation

Work in small checkpoints rather than pasting the whole implementation and trusting a final row count:

1. Load one file and print its shape, columns and lineage. Explain what the index and row number each mean.
2. Try the normalization helper on missing text, whitespace and Windows line endings. Explain which differences it preserves.
3. Parse a valid date and a URL-shaped date. Inspect `NaT` before applying validation.
4. Build a small table containing one good row, one empty body, one bad date and one repeated good body. Predict each output before running the transformation.
5. Check that a row with two failures has two reason labels but counts as one rejected row.
6. Repeat the same usable article under another source and ingestion timestamp. Check canonical choice and preserved source occurrences.
7. Reorder inputs and confirm the same representative is chosen. Add a replay and confirm canonical count remains unchanged.
8. Only then run the full Bronze input through the completed publication wrapper and record the actual counts, output paths and verification results here.

Printing `df.head()` shows example values; `df.dtypes` shows stored types; `df.isna().sum()` counts nulls per column. Use these tools to inspect what each stage actually changed. There is no need to print full article bodies to understand the table structure.

## 13. Mini cheatsheet: Silver constructs

| Construct | Plain-language meaning |
|---|---|
| `Path(...).glob("*.parquet")` | Find Parquet files in a folder |
| `set(required) - set(actual)` | Find missing column names |
| `pd.concat(frames, ignore_index=True)` | Stack tables and reset row labels |
| `df.copy()` | Make a separate working table |
| `.astype("string")` | Use nullable text values |
| `.map(function)` | Apply a function to each column value |
| `.str.strip()` | Remove surrounding whitespace |
| `errors="coerce"` | Represent failed parsing as missing |
| `.dt` | Access timestamp operations for a column |
| `.loc[mask]` | Select rows by a true/false condition |
| `~mask` | Reverse a true/false condition |
| `.mask(condition)` | Replace matching values with missing values |
| `.sort_values([...])` | Order rows by explicit keys |
| `.duplicated(key)` | Mark repeated keys after their first appearance |
| `raise ValueError(...)` | Stop with an explanatory failure |
| `return a, b, c, d` | Give four results back to the caller |

---

# Day 3 — Upcoming Step and Stopping Point

**Entry condition:** The Silver implementation must first be completed, verified and documented with actual output counts. If it is still incomplete at the start of Day 3, finish those checks before proceeding to Gold.

Once Silver passes, Day 3's next layer is **Gold analytics with DuckDB**. Silver stores usable article-level records; Gold turns them into small tables answering specific questions.

Planned Day 3 work:

1. Read exactly one completed Silver run and its quality report.
2. Learn the SQL pattern `SELECT ... GROUP BY ...`: group articles by publication day or category, then count each group.
3. Create daily article-volume and category-distribution aggregates from canonical articles.
4. Use occurrences for dataset-partition participation, with explicit definitions for unique articles versus input appearances. Use validation counters for rejected-row rates.
5. Create metadata-completeness metrics that treat nulls and assumptions honestly. Separate ingestion replay from original dataset duplication when reporting duplicate rates.
6. Save the Gold tables to Parquet and reconcile their totals with Silver. Document the SQL, each field's meaning, denominator choices, output counts and verification in `DAY3_REPORT.md`.

For example, a daily Gold table might contain `publication_date` and `article_count`. Its counts should sum to the canonical article total if every canonical row has exactly one valid publication day. A source-participation table may sum to more than that total when one article appears in multiple partitions; the report must explain that metric rather than force the totals to match.

**Stop here for this request.** This update adds the Silver teaching walkthrough and the Day 3 preview only. It does not create or execute a Silver module, start Gold, build a dashboard, acquire RSS/API data, or commit/push changes.

---

# Silver Implementation — Completed Follow-up

This section supersedes the earlier planning-only status for implementation completion. The walkthrough above remains a teaching reference; the executable implementation is now `src/transform_silver.py`. Bronze ingestion code was not modified and matched its committed version. Gold and Day 3 work have not been started.

## Code created and how to run it

From the repository root:

```bash
.venv/bin/python -m src.transform_silver
```

The new module loads all existing Bronze snapshots, checks the 14-column contract, records input file fingerprints and row lineage, normalizes and validates values, preserves rejected records, generates deterministic content IDs, and retains every valid occurrence. The actual implementation also includes file hashing, quality summaries, staged output publication and completed-run reuse beyond the core teaching example above.

`file_hash()` reads a file in 1 MB chunks and updates a SHA-256 digest. Its result identifies the exact bytes of an input or output file. `load_bronze()` checks that a file did not change during reading. `transform()` performs the table operations explained earlier. `write_json()` saves readable metadata. `run()` coordinates these steps, checks output files and publishes the run. The `__main__` guard starts `run()` only when the module is executed directly.

A run ID hashes the input inventory, rules and module code fingerprint. Changing code or inputs therefore creates a different run directory. For unchanged inputs/code, an existing run is checksum-verified and reused. A temporary staging folder prevents partial outputs from being announced as completed; a temporary pointer is replaced atomically after publication. Run only one writer at a time in this local v1; concurrent job coordination is not implemented.

Outputs are saved beneath `data/silver/runs/<run_id>/`: `articles.parquet`, `rejected.parquet`, `occurrences.parquet`, `quality_report.json`, and `manifest.json`. `data/silver/current.json` identifies the completed run to read. Do not concatenate all historical run directories, because each is a full snapshot.

## Actual full-dataset results

| Metric | Measured result |
|---|---:|
| Bronze input rows | 134,694 |
| Rejected rows | 1,923 |
| Missing-content rejection reason | 1,893 |
| Invalid-publication-date rejection reason | 30 |
| Canonical articles | 38,633 |
| Additional valid occurrences | 94,138 |
| All valid occurrences | 132,771 |
| RealNews partition distinct articles present | 21,190 |
| FakeNews partition distinct articles present | 17,443 |

Row accounting: **134,694 = 1,923 + 38,633 + 94,138**. Limited whitespace/line-ending normalization merges five additional article groups compared with the 38,638 raw-body-hash baseline. All canonical author, URL and country fields remain missing; language is marked assumed. The publication date range remains 2015-03-31 through 2018-02-19.

The completed run ID is `69446f64535553aa7c343de354ad9b138bac560dc4bda7d100682b7c40868bd5`.

## Verification performed

Small in-memory fixtures exercised normalization, multiple rejection reasons on one row, category mapping, local-to-UTC conversion, missing metadata, cross-source deduplication and deterministic representative selection after shuffling. A temporary fixture run verified staged publication and checksum-verified repeat-run reuse.

Full-data checks verified every input fingerprint and every output artifact checksum, repeated the transformation and compared its tables, checked persisted canonical values against their in-memory equivalents, and appended a simulated replay of every input. The replay doubled occurrences and rejected rows while leaving canonical IDs and count unchanged. Parquet restores the all-null object country column with `None` values instead of `pd.NA`; both represent missing data, and missingness was explicitly checked during comparison. No country values were introduced.

Limitations retained intentionally: this adapter accepts only the current two Kaggle source IDs and their date/time formats; duplicate-occurrence counts include ingestion replays and are not publisher duplicate rates; independent replay-versus-original-duplicate classification is not implemented. Reports provide missing counts rather than a subjective quality score. The source registry, richer source adapters and concurrent execution controls remain future work. No dependencies were added, no Git commit/push was performed, and Bronze files were unchanged.

**Next step remains Day 3 Gold analytics after reviewing these Silver outputs. Stop before Gold implementation.**
