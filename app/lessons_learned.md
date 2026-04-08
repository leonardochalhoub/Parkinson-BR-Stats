# Lessons Learned

## 2026-04-08

- Use **Delta Lake** format for all ingested datasets (bronze/silver/gold).
- Keep all downloaded/raw data out of git: store in `data/` or `lakehouse/` (both gitignored).
- Prefer reproducible ingestion: scripts should (1) download, (2) checksum/log source metadata, (3) write to Delta.
- Always capture source provenance (URL, access date, license/terms, dataset version) alongside the Bronze Delta tables.
- When a source is not directly downloadable (e.g., interactive portals / PDFs), document a fallback plan:
  - scrape via official APIs when available
  - otherwise export manually once and store locally (still gitignored) with a README + provenance

## Shell / CLI pitfalls (important)

- When running Python one-liners that contain loops/newlines, **don’t cram them into `python -c "..."`**.
  - Common symptom: `SyntaxError: unexpected character after line continuation character`
  - Root cause: quoting/escaping issues (newlines or backslashes interpreted by the shell / XML escaping, not by Python)
  - Fix: prefer a heredoc or a `.py` file.
- Prefer a heredoc:
  - `python - <<'PY'` (recommended; avoids quoting issues)
  - (paste Python code, multiple lines ok)
  - `PY`
- Safest alternative: put the logic into a `.py` file and run it (best for anything non-trivial).
- If you see: `bash: syntax error near unexpected token '('`, it usually means the shell parsed part of your Python snippet.
  - Fix by moving the logic into a `.py` file, or using a heredoc.
- If you pasted a Python heredoc and it started executing as plain bash (e.g. `print(...)` becomes `bash: syntax error near unexpected token ...` and `PY: command not found`), the heredoc was not started correctly.
  - Correct pattern (must be typed as a single command, with the final `PY` alone on its own line):
    - `python - <<'PY'`
    - (paste Python code)
    - `PY`
  - Safer alternative: put the code into a `.py` file and run `python path/to/script.py`.
- Avoid mixing shell operators like `;`, `&&`, `|` inside XML tool calls. They are often escaped/rewritten (`&` → `&`) and can surface as:
  - `bash: syntax error near unexpected token \`;&'`
- Prefer **single-command** executions in `execute_command`:
  - run one command at a time (e.g., `ls ...` then a separate `find ...`)
  - or put complex logic into a `.py` file and execute `python path/to/script.py`

## Open Questions / TODO

- Confirm which **DATASUS** dataset(s) will be used for MRI equipment counts and whether there is an official download/API endpoint for CNES / SIASUS.
- Confirm whether ELSI-Brazil microdata is accessible publicly or only via restricted access; if restricted, we will use the published estimates as “gold” reference values and compute projections using IBGE population.
