# Experiment Tracker

Local-first MVP for tracking customer experiment participation by GCID.

## What this app does

- Search by customer `GCID` with configurable timeline (default: `30` days)
- Return daily participation counts and experiment list
- Show clickable chart and table results
- Drill into an experiment to see:
  - experiment name
  - start/end date and running duration
  - overlap count with other experiments
  - variant list

## Project structure

- `backend/` FastAPI API + Databricks query layer
- `frontend/` Simple static web UI (Chart.js + vanilla JS)

## Backend setup

1. Create and activate virtual environment:

   - Windows PowerShell:
     - `cd backend`
     - `python -m venv .venv`
     - `.venv\\Scripts\\Activate.ps1`

2. Install dependencies:

   - `pip install -r requirements.txt`

3. Configure environment:

   - Copy `backend/.env.example` to `backend/.env`
   - Update the `DBX_*` table/column mapping values to your real Databricks schema
   - Keep `DBX_PROFILE` aligned with your `~/.databrickscfg` profile name

4. Run API server:

   - `uvicorn app.main:app --reload --port 8000`

API endpoints:

- `GET /api/health`
- `GET /api/connection-check`
- `POST /api/search`
- `GET /api/experiments/{experiment_id}?gcid=<GCID>&days=<N>`

Optional connectivity test:

- `python scripts/test_connection.py`

## Frontend setup

Use any static server from the `frontend/` folder:

- `cd frontend`
- `python -m http.server 8080`

Open:

- `http://localhost:8080`

The UI calls API at `http://localhost:8000/api` by default when served separately.
If served through FastAPI, open `http://localhost:8000/app`.

## Configurable Databricks contract

These env vars make schema mapping configurable without code edits:

- `DBX_TABLE`
- `DBX_GCID_COLUMN`
- `DBX_EVENT_TS_COLUMN`
- `DBX_EXPERIMENT_ID_COLUMN`
- `DBX_EXPERIMENT_NAME_COLUMN`
- `DBX_VARIANT_COLUMN`

## Deployment notes (initial checklist)

- Backend host: Render/Fly/other Python host
- Frontend host: Vercel/Netlify, or serve `/frontend` via FastAPI as static files
- Set production env vars:
  - `DBX_PROFILE` or explicit Databricks auth method
  - all `DBX_*` mapping vars
  - `ALLOWED_ORIGINS` with deployed frontend URL
- Ensure Databricks credentials are stored as platform secrets, never committed

## GitHub

- Repository: `https://github.com/bekakh-tr/experiment-tracker`
