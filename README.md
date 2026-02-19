# SkillMap Agentic Upgrade - Phase 1

Enterprise-ready Phase 1 implementation for goal-driven skill analytics with FastAPI + SQLAlchemy + Pandas and a modern API-driven frontend.

## Features
- Goal-based natural language queries (top skills, rising skills, trends)
- Intent + filters extraction (role, country, time range)
- Fuzzy matching and auto-correction for role/country
- Explicit execution plan generation before running analysis
- Dynamic strategy:
  - Uses `skills` column when available
  - Falls back to description-based skill extraction if skills missing
  - Graceful fallback + warning when date is missing
- One-response agent output with confidence score + warnings
- Clarification flow for incomplete queries
- Agent run logging in database (query, parsed intent, plan, outputs, status, timestamps)
- Dataset auto-discovery summary after upload

## Tech Stack
- Backend: FastAPI, SQLAlchemy, Pandas, Uvicorn, CORS enabled
- Database: SQLite (default) or PostgreSQL cloud (`DATABASE_URL`)
- Frontend: HTML, CSS, JavaScript

## Project Structure
```text
backend/
  app/
    main.py
    database.py
    models.py
    schemas.py
    services/
      dataset_service.py
      query_parser.py
      agent_service.py
    utils/
      text_utils.py
  requirements.txt
frontend/
  index.html
  styles.css
  app.js
README.md
```

## Run Locally
```bash
cd backend
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS/Linux
# source .venv/bin/activate

pip install -r requirements.txt
uvicorn app.main:app --reload
```

Open `http://127.0.0.1:8000`.

## Cloud Database (Free Tier)
Use a free PostgreSQL provider like Neon or Supabase.

1. Create a hosted PostgreSQL database.
2. Set `DATABASE_URL` before starting backend:

```bash
# PowerShell example
$env:DATABASE_URL="postgresql+psycopg://USER:PASSWORD@HOST:5432/DBNAME?sslmode=require"
uvicorn app.main:app --reload
```

If `DATABASE_URL` is not set, the app automatically uses local SQLite (`backend/skillmap.db`).

## API Endpoints
- `POST /api/upload` - Upload CSV/XLSX and auto-generate dataset summary
- `GET /api/dataset/summary` - Fetch current dataset summary
- `POST /api/agent/query` - Run agent workflow from a natural-language query
- `GET /api/agent/runs` - Retrieve recent run logs
- `GET /api/health` - Health check

## Example Queries
- `Show rising skills for Data Analyst in Canada for last 6 months`
- `What are the top skills for Data Scientist roles in USA?`
- `Give me skill trends for Software Engineer in Germany`

## Notes
- Phase 1 uses deterministic rule-based orchestration.
- LLM integration can be added later for richer query parsing.
