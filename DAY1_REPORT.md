# VeriTAS — Day 1 Implementation Report
## From Project Inception to First Cloud Commit

**Project:** VeriTAS — Scalable News Aggregation & Analytics Data Pipeline
**Coverage:** All work performed from initial planning through the first successful push to GitHub. No planned or future work is included.
**Environment:** Linux (Mint), VS Code, Bash terminal, Python virtual environment located at `~/Desktop/project_DE/.venv`
**Final state of day:** Private GitHub repository `github.com/tharunn2007/veriTAS`, branch `main`, root commit `fa64f35`, plus a locally generated Bronze data layer of 44,898 rows across two Parquet files.

---

## 1. Scope and Purpose of This Report

This report records, in chronological order, every concrete action, decision, command, output, and error resolution executed during Day 1 of the VeriTAS project. The day began with an architectural review of an external industrial reference repository and ended with the project's foundational code being committed and pushed to a newly created GitHub repository named `veriTAS`. The work covered six phases: (1) strategy definition, (2) environment audit and workspace preparation, (3) dataset acquisition and raw schema inspection, (4) authoring of the configuration contract, (5) implementation and execution of the Bronze ingestion layer, and (6) establishment of version control including two significant incident resolutions.

---

## 2. Phase 0 — Reference Analysis and Strategy Definition

### 2.1 Review of the industrial reference repository
The session opened with an examination of the GitHub repository `github.com/ab-dx/pathway-platform` ("Velora"), supplied as an industrial benchmark. The repository was analysed and documented as an enterprise-grade, real-time streaming platform comprising Apache Kafka topics, Pathway stream processing, Kubernetes manifests, Docker Compose stacks, Rust-based backtesting services, Firecrawl scraping, vector stores, Prometheus/Grafana observability, and per-layer microservices for news, stock, and social-media pipelines. A formal scope decision was taken: the repository would **not** be replicated or cloned as a starting point, because its toolchain contradicts the project's stated constraints (beginner-friendly batch processing with pandas, DuckDB, Parquet, and Streamlit before any streaming infrastructure). Instead, three reusable design patterns were extracted from it and adopted into VeriTAS: (a) the medallion layering of Bronze → Silver → Gold, (b) centralised configuration management (mirroring its `config.yaml` and `.env` patterns), and (c) explicit deduplication state tracking (mirroring its `dedup_state.json`), implemented in VeriTAS as a `content_hash` column.

### 2.2 The "mini-to-industrial" continuity strategy
A mapping table was defined so that every local component has a predetermined industrial successor, guaranteeing that no work is discarded when scaling. The agreed mappings were: local CSV inputs → Kafka topics or RSS producers; local Python scripts → Airflow tasks or Databricks notebooks; local `data/bronze|silver|gold` folders → S3 prefixes or Delta Lake tables; `config.py` → environment files and a schema registry; the local Streamlit app → a deployed dashboard; and the local `.venv` → container images. This mapping was recorded as an interview-facing rationale: validate logic locally, then swap transport and compute without rewriting business logic.

### 2.3 Execution principles agreed
Three working principles were fixed before any code was written: learn-by-building with understanding checkpoints rather than tutorial consumption; no "vibe-coding" (every line must be explainable); and strict sequential phase order (CSV → Bronze → Silver → Gold → Dashboard → live RSS), with cloud and streaming tools deferred until the local pipeline is proven.

---

## 3. Phase 1 — Environment Audit and Workspace Preparation

### 3.1 Audit of the existing workspace
The existing workspace was inspected via a VS Code Explorer screenshot and terminal commands. It was confirmed that `~/Desktop/project_DE` already contained a Python virtual environment (`.venv`, with `bin`, `lib`, `pyvenv.cfg`, and an auto-generated internal `.gitignore`), created earlier with `uv` and pre-loaded with project packages. The interpreter selection in VS Code was verified to point at this `.venv`, and activation was confirmed in the terminal by the `(project_DE)` prompt prefix after `source /home/tharunn/Desktop/project_DE/.venv/bin/activate`.

### 3.2 Terminal orientation and correction
The first layout inspection command, `find . -maxdepth 2 -not -path '*/\.venv/*'`, was accidentally executed from the home directory (`~`), producing a full listing of the user account (Downloads, Documents, `.cache`, `airflow`, etc.). The mistake was identified from the prompt, the project location was located in the output (`./Desktop/project_DE`), and the command was re-run after `cd Desktop/project_DE`. The corrected output showed that the project folder contained **only** `.venv`, confirming that no pipeline files existed yet and that the skeleton still had to be created. A dependency pre-check was also performed with `python -c "import pandas, pyarrow"` to confirm the Parquet engine was available.

### 3.3 Creation of the project skeleton
The folder architecture was created with a single command block:

```bash
mkdir -p src data/bronze data/silver data/gold app
touch src/__init__.py src/ingest_bronze.py config.py
```

The layout deliberately separates code that transforms data (`src/`), the data lake itself (`data/` with three medallion subfolders), and code that presents data to humans (`app/`). The distinction between `data/gold` (aggregated Parquet outputs) and `app/` (dashboard source code) was explicitly resolved during planning. Verification was repeated with the `find` command until the tree matched the design.

---

## 4. Phase 2 — Dataset Acquisition and Raw Schema Inspection

### 4.1 Dataset selection
The Kaggle dataset **"Fake and real news dataset"** (`clmentbisaillon/fake-and-real-news-dataset`) was selected as the Day-1 source: true CSV format, sufficient volume (~45k rows), and columns covering title, body, topic, and date — enough to exercise every planned dashboard metric later.

### 4.2 Download and relocation
The dataset was downloaded via the Kaggle API:

```bash
curl -L -o ~/Downloads/fake-and-real-news-dataset.zip \
  https://www.kaggle.com/api/v1/datasets/download/clmentbisaillon/fake-and-real-news-dataset
```

The transfer completed at 40.9 MB. Because the `-o` flag targeted `~/Downloads`, the archive was then relocated into the project root with `mv ~/Downloads/fake-and-real-news-dataset.zip .`.

### 4.3 Extraction and inventory
`unzip fake-and-real-news-dataset.zip` produced two files, confirmed by `ls -l *.csv`: `Fake.csv` (62,789,876 bytes) and `True.csv` (53,582,940 bytes). The presence of two files was recorded as a deliberate advantage: the ingestion script would have to handle multiple sources in one pass.

### 4.4 Header inspection and mapping decision
`head -n 2 True.csv` and `head -n 2 Fake.csv` revealed identical headers — `title,text,subject,date` — with long quoted bodies and human-formatted dates such as `"December 31, 2017 "`. A mapping decision was recorded against the project's 14-column target schema: `title→title`, `text→content`, `subject→category`, `date→published_at` (kept raw in Bronze); missing attributes filled with honest placeholders (`author="Unknown"`, `url="N/A"`, `language="en"`, `country="US"`); and four pipeline-generated columns added (`article_id`, `source_id`/`source_name`, `ingested_at`, `content_hash`, plus a truncated `description`).

---

## 5. Phase 3 — The Configuration Contract (`config.py`)

`config.py` was authored as the single source of truth. It performs directory auto-creation via `pathlib.Path(...).mkdir(parents=True, exist_ok=True)` for the three medallion folders, and defines three structures: `FILES_TO_INGEST` (a list of dictionaries holding `path`, `source_name`, and `source_id` for `True.csv`/`Kaggle_RealNews`/1 and `Fake.csv`/`Kaggle_FakeNews`/2), `TARGET_COLUMNS` (the ordered 14-column contract: `article_id, source_id, source_name, title, description, content, author, category, published_at, url, language, country, ingested_at, content_hash`), and `COLUMN_MAPPING` (the Kaggle-to-target translation table). The file was validated with a one-liner import test printing `14 columns` and both file paths.

---

## 6. Phase 4 — Bronze Layer Implementation (`src/ingest_bronze.py`)

The ingestion script was written with the following structure: standard imports (`pandas`, `hashlib`, `uuid`, `datetime`, `os`, `sys`); a `sys.path.append(...)` call resolving the parent directory so `config` can be imported from inside `src/`; a `generate_hash(text)` helper returning an MD5 hex digest of UTF-8-encoded article text (or `None` for empty values); and an `ingest_one_file(path, source_name, source_id)` function that (1) reads the CSV with `pd.read_csv(path, on_bad_lines="skip")`, (2) renames columns via `COLUMN_MAPPING`, (3) injects placeholder and source-identity columns, (4) builds `description` from the first 200 characters of `content`, (5) generates a `uuid4` per row as `article_id`, (6) stamps `ingested_at` with the current timestamp, (7) applies `generate_hash` row-wise to produce `content_hash`, (8) reindexes the frame to exactly `TARGET_COLUMNS`, and (9) writes a timestamped Parquet file into `data/bronze/`. A `main()` loop iterates `FILES_TO_INGEST`.

---

## 7. Phase 5 — Bronze Execution and Verification

Running `python src/ingest_bronze.py` produced: 21,417 raw rows from `True.csv` and 23,481 from `Fake.csv`; two outputs, `data/bronze/bronze_kaggle_realnews_20261011_190533.parquet` (33 MB) and `data/bronze/bronze_kaggle_fakenews_20261011_190533.parquet` (40 MB); `ls -lh data/bronze` reporting 72 MB total. Independent verification with `pd.read_parquet('data/bronze')` returned shape **(44898, 14)** and the exact 14-column list in contract order, with preview rows showing valid UUIDs and 32-character hashes. The Bronze layer was declared complete and correct.

---

## 8. Phase 6 — Version Control Establishment

### 8.1 The `.gitignore` misplacement incident
Work paused before the Silver layer to establish version control. The first `.gitignore` attempt failed: the pasted rules were placed into `.venv/.gitignore` (the venv's auto-generated file whose sole content was `*`), and the protective `*` was deleted. The error was diagnosed from the VS Code breadcrumb showing `.venv > .gitignore`. Remediation: the `*` was restored inside `.venv/.gitignore`, and the real root `.gitignore` was written with `printf ... > .gitignore` containing `.venv/`, `*.csv`, `data/bronze/`, `data/silver/`, `data/gold/`, `__pycache__/`, `*.pyc`. A subsequent review caught that the 41 MB dataset zip was still unignored; `printf '*.zip\n' >> .gitignore` appended the rule (the `>>` append operator versus `>` overwrite was noted).

### 8.2 Repository initialisation and ordering error
Verification commands (`git check-ignore`, `git status`) were run before `git init` and returned `fatal: not a git repository`. The sequence was corrected: `git init` (which emitted the default-branch hint for `master`), then `git branch -M main`, then re-verification.

### 8.3 Ignore-rule verification
`git check-ignore -v .venv/pyvenv.cfg Fake.csv fake-and-real-news-dataset.zip` returned three matches attributed to root rules: line 2 (`.venv/`), line 5 (`*.csv`), line 13 (`*.zip`). `git status --short` listed only `?? .gitignore`, `?? config.py`, `?? src/` — the empty `app/` folder was invisible because Git tracks files, not directories; this was resolved with `touch app/.gitkeep`.

### 8.4 First commit
`git add .` followed by `git commit -m "Initial commit: project structure, config, and Bronze ingestion layer"` produced `[main (root-commit) fa64f35]`, **5 files changed, 106 insertions(+)**. `git ls-files` confirmed exactly five tracked paths: `.gitignore`, `app/.gitkeep`, `config.py`, `src/__init__.py`, `src/ingest_bronze.py`.

### 8.5 Remote creation, the bracket incident, and successful push
An empty private repository named `veriTAS` was created on GitHub with all initialisation checkboxes left unchecked. The first push attempt failed because the remote URL was typed with placeholder angle brackets retained (`https://github.com/<tharunn2007>/VeriTAS.git`); Bash interpreted `<` and `>` as redirection operators, aborting with `bash: tharunn2007: No such file or directory`, after which `git push` reported `'origin' does not appear to be a git repository`. The command was re-issued without brackets using the exact URL from the repository's Quick Setup panel:

```bash
git remote add origin https://github.com/tharunn2007/veriTAS.git
git remote -v
git push -u origin main
```

The push succeeded: 8 objects enumerated, 6 compressed, 8 written, **2.29 KiB transferred**, `* [new branch] main -> main`, and `branch 'main' set up to track 'origin/main'`. The 2.29 KiB transfer size was recorded as proof that no dataset or environment files were uploaded.

---

## 9. End-of-Day Artefact Inventory and Metrics

**Tracked in repository (commit `fa64f35`):** `.gitignore`, `app/.gitkeep`, `config.py`, `src/__init__.py`, `src/ingest_bronze.py` (106 inserted lines).
**Local data lake:** two Bronze Parquet files, 44,898 combined rows, 14 columns, ~72 MB; source CSVs 62.8 MB and 53.6 MB; source zip 40.9 MB.
**Remote:** private repository `veriTAS`, branch `main` tracking `origin/main`.
**Key numeric checks:** 21,417 + 23,481 = 44,898 rows; 14/14 schema columns present; push payload 2.29 KiB.

## 10. Error Log and Resolutions

| # | Error observed | Root cause | Resolution |
|---|---|---|---|
| 1 | `find` listed entire home directory | Command run in `~` | `cd Desktop/project_DE` and re-run |
| 2 | Zip landed in Downloads | `curl -o` path target | `mv` into project root |
| 3 | venv contents un-ignored | Rules pasted into `.venv/.gitignore`, `*` deleted | Restored `*`; wrote root `.gitignore` |
| 4 | `fatal: not a git repository` | Checks run before `git init` | Ran `git init`, `git branch -M main` first |
| 5 | `bash: tharunn2007: No such file...` | Placeholder `< >` typed literally; Bash redirection | Re-ran `git remote add` with clean URL |
| 6 | `app/` absent from `git status` | Git ignores empty directories | Added `app/.gitkeep` |

## 11. Closing State

Day 1 concluded with a verified, schema-conformant Bronze data lake on disk and a clean, protected, cloud-backed repository. All code in the repository is explainable line-by-line, satisfying the project's no-vibe-coding requirement, and the architecture is positioned to continue sequentially into the Silver transformation layer without restructuring.

---

*End of report.*