# App Source Layout

This project follows a **lakehouse**-oriented pipeline, with Spark/PySpark code organized by layer.

## Folders

- `bronze/`  
  Ingestion code: download/extract raw data and write **Delta** tables to `lakehouse/bronze/...`.

- `silver/`  
  Cleaning/conformance: schema normalization, deduplication, joins, standard dimensions; write **Delta** tables to `lakehouse/silver/...`.

- `gold/`  
  Business outputs: curated aggregates and indicators used by the web page; write **Delta** tables to `lakehouse/gold/...` and/or export lightweight artifacts for the frontend (CSV/JSON) into `app/web/public/` (gitignored if generated).

## Conventions

- Every pipeline should log provenance metadata (source URL, access date, and any dataset version).
- All tables are written in **Delta Lake** format.
- All downloaded data and lakehouse data are **local only** and should remain out of git (see `.gitignore`).
