"""
Research Library pipeline - INFO-H420 Assignment 3.

Every 10 minutes:
  detect new PDFs -> convert to Markdown -> extract metadata with a local LLM
  (Ollama, OpenAI-compatible API) -> store in SQLite -> regenerate a static
  HTML page -> notify Discord.
"""
import html
import json
import os
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote

from airflow.sdk import dag, task

# ---------------------------------------------------------------------------
# Configuration (can be overridden with environment variables)
# ---------------------------------------------------------------------------
LIBRARY_DIR = Path(os.environ.get("LIBRARY_DIR", Path.home() / "ResearchLibrary"))
PAPERS_DIR = LIBRARY_DIR / "papers"
OUTPUT_DIR = LIBRARY_DIR / "output"

DATA_DIR = Path(os.environ.get("DATA_DIR", Path.home() / "airflow" / "data"))
DB_PATH = DATA_DIR / "papers.db"
MARKDOWN_DIR = DATA_DIR / "markdown"

LLM_BASE_URL = os.environ.get("LLM_BASE_URL", "http://localhost:11434/v1")
LLM_MODEL = os.environ.get("LLM_MODEL", "qwen2.5:1.5b")
MAX_CHARS = 6000  # only the beginning of each paper is sent to the LLM

CATEGORIES = [
    "Machine Learning",
    "Natural Language Processing",
    "Computer Vision",
    "Data Engineering",
    "Databases",
    "Other",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def get_conn() -> sqlite3.Connection:
    """Open the SQLite database and create the table if needed."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS papers (
            filename     TEXT PRIMARY KEY,
            title        TEXT NOT NULL,
            summary      TEXT NOT NULL,
            keywords     TEXT NOT NULL,   -- JSON list stored as a string
            category     TEXT NOT NULL,
            processed_at TEXT NOT NULL
        )
        """
    )
    return conn


def validate_metadata(meta) -> None:
    """Basic checks: required fields exist and have the expected types."""
    if not isinstance(meta, dict):
        raise ValueError("output is not a JSON object")
    for field in ("title", "summary", "category"):
        value = meta.get(field)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"'{field}' is missing or not a non-empty string")
    keywords = meta.get("keywords")
    if not isinstance(keywords, list) or not all(isinstance(k, str) for k in keywords):
        raise ValueError("'keywords' is missing or not a list of strings")


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------
@task
def detect_new_pdfs() -> list[str]:
    """Return the PDFs in papers/ that are not yet in the database."""
    PAPERS_DIR.mkdir(parents=True, exist_ok=True)
    on_disk = sorted(p.name for p in PAPERS_DIR.iterdir() if p.suffix.lower() == ".pdf")

    conn = get_conn()
    try:
        done = {row[0] for row in conn.execute("SELECT filename FROM papers")}
    finally:
        conn.close()

    new = [f for f in on_disk if f not in done]
    print(f"{len(on_disk)} PDFs found, {len(new)} new: {new}")
    return new


@task
def convert_to_markdown(filename: str) -> dict:
    """Convert one PDF to Markdown and save it locally."""
    import pymupdf4llm

    MARKDOWN_DIR.mkdir(parents=True, exist_ok=True)
    markdown = pymupdf4llm.to_markdown(str(PAPERS_DIR / filename))
    md_path = MARKDOWN_DIR / f"{Path(filename).stem}.md"
    md_path.write_text(markdown, encoding="utf-8")
    print(f"Converted {filename}: {len(markdown)} characters")
    return {"filename": filename, "md_path": str(md_path)}


@task(max_active_tis_per_dag=1)  # one LLM call at a time (CPU-friendly)
def extract_metadata(doc: dict) -> dict:
    """Ask the local LLM for title, summary, keywords and category as JSON."""
    from openai import OpenAI

    text = Path(doc["md_path"]).read_text(encoding="utf-8")[:MAX_CHARS]
    client = OpenAI(base_url=LLM_BASE_URL, api_key="ollama")  # key is ignored by Ollama

    messages = [
        {
            "role": "system",
            "content": "You extract metadata from research papers. Reply with JSON only.",
        },
        {
            "role": "user",
            "content": (
                "Read the beginning of this research paper and return a JSON object "
                "with exactly these keys:\n"
                '- "title": the paper title (string)\n'
                '- "summary": a 2-3 sentence summary in English (string)\n'
                '- "keywords": 3 to 6 keywords (list of strings)\n'
                f'- "category": one of {CATEGORIES} (string)\n\n'
                f'Paper:\n"""\n{text}\n"""'
            ),
        },
    ]

    last_error = None
    for attempt in (1, 2):  # first try + one retry
        response = client.chat.completions.create(
            model=LLM_MODEL,
            messages=messages,
            response_format={"type": "json_object"},  # JSON mode
            temperature=0,
        )
        raw = response.choices[0].message.content or ""
        try:
            meta = json.loads(raw)
            validate_metadata(meta)
            if meta["category"] not in CATEGORIES:
                meta["category"] = "Other"
            meta["filename"] = doc["filename"]
            print(f"Metadata for {doc['filename']}: {meta}")
            return meta
        except (json.JSONDecodeError, ValueError) as err:
            last_error = err
            print(f"Attempt {attempt} failed: {err}. Raw output: {raw[:300]}")

    raise ValueError(f"Invalid LLM output for {doc['filename']} after 2 attempts: {last_error}")


@task
def store_metadata(meta: dict) -> dict:
    """Insert (or update) one paper in SQLite."""
    conn = get_conn()
    try:
        with conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO papers
                    (filename, title, summary, keywords, category, processed_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    meta["filename"],
                    meta["title"],
                    meta["summary"],
                    json.dumps(meta["keywords"], ensure_ascii=False),
                    meta["category"],
                    datetime.now().isoformat(timespec="seconds"),
                ),
            )
    finally:
        conn.close()
    return meta


HTML_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Research Library</title>
<style>
  body { font-family: system-ui, sans-serif; max-width: 900px; margin: 2rem auto; padding: 0 1rem; background: #f7f7f8; color: #222; }
  .meta { color: #666; font-size: .9rem; }
  .controls { display: flex; gap: .5rem; flex-wrap: wrap; margin: 1rem 0; }
  input, select { padding: .5rem; font-size: 1rem; border: 1px solid #ccc; border-radius: 6px; }
  input { flex: 1; min-width: 200px; }
  .paper { background: #fff; border: 1px solid #e3e3e3; border-radius: 8px; padding: 1rem 1.25rem; margin-bottom: 1rem; }
  .paper h2 { margin: .4rem 0; font-size: 1.15rem; }
  .cat { font-size: .8rem; background: #e8eefc; color: #2a4fa8; padding: .15rem .5rem; border-radius: 4px; }
  .kw span { display: inline-block; font-size: .8rem; background: #eee; padding: .1rem .45rem; border-radius: 4px; margin: 0 .25rem .25rem 0; }
</style>
</head>
<body>
<h1>Research Library</h1>
<p class="meta">__COUNT__ papers &middot; last updated __UPDATED__</p>
<div class="controls">
  <input id="q" type="search" placeholder="Search title, summary or keywords...">
  <select id="cat"><option value="">All categories</option>__OPTIONS__</select>
</div>
<main>
__CARDS__
</main>
<script>
  const q = document.getElementById('q');
  const cat = document.getElementById('cat');
  function filter() {
    const text = q.value.toLowerCase();
    const c = cat.value;
    document.querySelectorAll('.paper').forEach(p => {
      const ok = (!c || p.dataset.category === c) && p.textContent.toLowerCase().includes(text);
      p.style.display = ok ? '' : 'none';
    });
  }
  q.addEventListener('input', filter);
  cat.addEventListener('change', filter);
</script>
</body>
</html>
"""


@task
def build_html(_stored) -> str:
    """Regenerate output/index.html from everything in the database."""
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT filename, title, summary, keywords, category FROM papers ORDER BY title"
        ).fetchall()
    finally:
        conn.close()

    cards = []
    for filename, title, summary, keywords, category in rows:
        kw_html = " ".join(f"<span>{html.escape(k)}</span>" for k in json.loads(keywords))
        cards.append(
            f'<article class="paper" data-category="{html.escape(category)}">\n'
            f'  <span class="cat">{html.escape(category)}</span>\n'
            f"  <h2>{html.escape(title)}</h2>\n"
            f"  <p>{html.escape(summary)}</p>\n"
            f'  <p class="kw">{kw_html}</p>\n'
            f'  <a href="../papers/{quote(filename)}">{html.escape(filename)}</a>\n'
            f"</article>"
        )

    categories = sorted({row[4] for row in rows})
    options = "".join(f"<option>{html.escape(c)}</option>" for c in categories)

    page = (
        HTML_TEMPLATE.replace("__COUNT__", str(len(rows)))
        .replace("__UPDATED__", datetime.now().strftime("%Y-%m-%d %H:%M"))
        .replace("__OPTIONS__", options)
        .replace("__CARDS__", "\n".join(cards))
    )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "index.html"
    out_path.write_text(page, encoding="utf-8")
    print(f"Wrote {out_path} with {len(rows)} papers")
    return str(out_path)


@task
def notify_discord(papers) -> None:
    """Send one Discord message per newly processed paper."""
    import requests

    url = os.environ.get("DISCORD_WEBHOOK_URL")
    if not url:
        print("DISCORD_WEBHOOK_URL is not set: skipping notification")
        return

    for p in papers:
        message = (
            f"📄 **New paper processed:** {p['title']}\n"
            f"**Category:** {p['category']}\n"
            f"**Keywords:** {', '.join(p['keywords'])}\n"
            f"`{p['filename']}`"
        )
        response = requests.post(url, json={"content": message[:2000]}, timeout=10)
        response.raise_for_status()
        print(f"Discord notified for {p['filename']}")


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
@dag(
    dag_id="paper_pipeline",
    schedule=timedelta(minutes=10),
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    tags=["info-h420", "assignment-3"],
)
def paper_pipeline():
    new_files = detect_new_pdfs()
    docs = convert_to_markdown.expand(filename=new_files)
    metas = extract_metadata.expand(doc=docs)
    stored = store_metadata.expand(meta=metas)
    page = build_html(stored)
    page >> notify_discord(stored)


paper_pipeline()
