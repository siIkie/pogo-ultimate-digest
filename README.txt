POKÉMON GO — Ultimate Automated Knowledge Pipeline
=================================================

What this repo gives you
------------------------
- **Automated scraping** from Niantic RSS + LeekDuck + Help Center + GO Hub
- **Structured datasets**: Events, Features, Balance, Wiki
- **Outputs**: CSV/JSON/NDJSON, Excel workbook, ICS calendar
- **Static API**: json endpoints under `api/`
- **Retrieval indices**: TF‑IDF per domain for RAG
- **Citations**: every row keeps Source + URL

Quick Start
-----------
1) Push to GitHub.
2) Repo Settings → Actions → General → Workflow permissions → **Read and write**.
3) Actions tab → Run “POGO Ultimate Pipeline” (or wait for cron).

Where things live
-----------------
- Config: `sources/sources.yaml`
- Entities (aliases/types/leagues): `data/entities.json`
- Builders: `build_*.py`
- Enrichment + merge: `tools/*.py`
- Retrieval: `index/*.py`
- Deterministic helpers: `calc/*.py`
- API snapshots: `api/*.json` (includes upcoming 30d events slice)
- Artifacts in Releases; stable copies can be added later to `outputs/` if desired

Extend it
---------
- Add a source: edit `sources/sources.yaml` (no code change needed)
- Add aliases/entities: edit `data/entities.json`
- Add API slices: edit `api/export_endpoints.py`

Ground rules
------------
- Respect site terms; fetch politely (cached HTTP, minimal selectors)
- Prefer exact dates in answers; don’t guess
- Always cite `Source URL` when surfacing info

Generated: 2025-09-23
