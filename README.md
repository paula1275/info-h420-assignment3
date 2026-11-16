# Research Library — INFO-H420 Assignment 3

A local Apache Airflow pipeline that turns research PDFs into a searchable study library.
Every 10 minutes the DAG `paper_pipeline` detects new PDFs, converts them to Markdown,
asks a local LLM (Ollama) for title, summary, keywords and category as JSON, stores the
metadata in SQLite, regenerates a static HTML page and sends a Discord notification.

## Pipeline

```
detect_new_pdfs → convert_to_markdown (mapped) → extract_metadata (mapped)
               → store_metadata (mapped) → build_html → notify_discord
```

- **PDF → Markdown:** PyMuPDF4LLM
- **LLM:** Ollama, model `qwen2.5:1.5b`, OpenAI-compatible API, JSON mode, one retry + field/type validation
- **Storage:** `sqlite3`, one row per paper (filename is the primary key), keywords stored as a JSON string
- **Output:** `output/index.html` with text search and category filter
- **Notification:** Discord webhook, one message per new paper

## Folder layout

```
Google Drive/ResearchLibrary/papers/   input PDFs (synced)
Google Drive/ResearchLibrary/output/   index.html (synced)
~/airflow/dags/paper_pipeline.py       the DAG
~/airflow/data/papers.db               SQLite database (local, not synced)
~/airflow/data/markdown/               converted Markdown (local)
```

## Requirements

- Linux or WSL2 (tested on Windows 11 + WSL2 Ubuntu, Python 3.12)
- Ollama
- Google Drive for desktop (optional: any local folder works)

## Setup

```bash
# 1. Ollama and the model
curl -fsSL https://ollama.com/install.sh | sh
ollama pull qwen2.5:1.5b

# 2. Airflow environment
export AIRFLOW_HOME=~/airflow
python3 -m venv ~/airflow/airflow_venv
source ~/airflow/airflow_venv/bin/activate
pip install -r requirements.txt
mkdir -p ~/airflow/dags && cp dags/paper_pipeline.py ~/airflow/dags/

# 3. Google Drive in WSL (repeat after every reboot)
sudo mkdir -p /mnt/g && sudo mount -t drvfs G: /mnt/g

# 4. Environment variables (see .env.example; never commit the real webhook)
export LIBRARY_DIR="/mnt/g/Mi unidad/ResearchLibrary"
export DISCORD_WEBHOOK_URL="https://discord.com/api/webhooks/..."
```

## Run

```bash
airflow standalone
```

Open http://localhost:8080, unpause `paper_pipeline`, and drop a PDF into `papers/`.
Within 10 minutes it appears in `output/index.html` and a Discord message is sent.
For a one-off run without the scheduler: `airflow dags test paper_pipeline`.

## Configuration

| Variable | Default |
|---|---|
| `LIBRARY_DIR` | `~/ResearchLibrary` |
| `DATA_DIR` | `~/airflow/data` |
| `LLM_BASE_URL` | `http://localhost:11434/v1` |
| `LLM_MODEL` | `qwen2.5:1.5b` |
| `DISCORD_WEBHOOK_URL` | unset (notification skipped) |

## Known limitations

- The 1.5B model categorises roughly (e.g. a vector-database paper was labelled *Data Engineering*).
- Summaries can contain Markdown leftovers from formulas.
- Only the first 6,000 characters of each paper are sent to the LLM.
- On WSL, the Google Drive mount must be redone after every reboot.
