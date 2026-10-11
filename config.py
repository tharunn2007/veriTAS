from pathlib import Path

#Folder autocreation as a safetey net - INCASE if the medallion folders not available
Path("data/bronze").mkdir(parents=True, exist_ok=True)
Path("data/silver").mkdir(parents=True, exist_ok=True)
Path("data/gold").mkdir(parents=True, exist_ok=True)

#Raw source files i.e. Fake.csv and True.csv as files to ingest into bronze
FILES_TO_INGEST = [
    {"path": "True.csv", "source_name": "Kaggle_RealNews", "source_id": 1},
    {"path": "Fake.csv", "source_name": "Kaggle_FakeNews", "source_id": 2},
]

#Columns present in Fake and True files
TARGET_COLUMNS = [
    "article_id", "source_id", "source_name", "title", "description",
    "content", "author", "category", "published_at", "url",
    "language", "country", "ingested_at", "content_hash",
]

#Kaggle column -> our column (the translation table)
COLUMN_MAPPING = {
    "title": "title",
    "text": "content",        # Kaggle 'text' = full article body
    "subject": "category",    # e.g. politicsNews, worldnews
    "date": "published_at",   # kept as raw string in Bronze; parsed in Silver
}
