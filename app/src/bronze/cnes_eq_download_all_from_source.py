"""
Download CNES EQ (Equipamentos) files from DATASUS FTP for ALL competências
up to and including 2025-12 (Dez/2025).

Rationale:
- Avoids fragile `python -c` one-liners.
- Provides a reproducible, restartable raw download step.

What gets downloaded:
- All files matching pattern: EQ??YYMM.dbc
  - ?? = UF (27 files per competência)
  - YYMM = two-digit year + two-digit month
- Filtered so that (year, month) <= (2025, 12)

Output directory (gitignored):
- data/cnes_eq/  (flat directory with all downloaded .dbc files)

Usage:
  python app/src/bronze/cnes_eq_download_all_until_202512.py

Notes:
- This is a LOT of files (~27 * number_of_months). Expect time and disk usage.
- The script is idempotent: it skips files that already exist with size > 0.
"""

from __future__ import annotations

import ftplib
import pathlib
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterable, Tuple


HOST = "ftp.datasus.gov.br"
EQ_DIR = "/dissemin/publicos/CNES/200508_/Dados/EQ/"
OUTDIR = pathlib.Path("data/cnes_eq")

# inclusive upper bound
MAX_YEAR = 2025
MAX_MONTH = 12

# FTP timeout and retry settings
FTP_TIMEOUT = 300  # 5 minutes
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
PARALLEL_WORKERS = 100  # Download 100 files in parallel

# Example: EQSP2602.dbc
RE_EQ_FILE = re.compile(r"^EQ(?P<uf>[A-Z]{2})(?P<yy>\d{2})(?P<mm>\d{2})\.dbc$", re.IGNORECASE)


def _yy_mm_to_year_month(yy: int, mm: int) -> Tuple[int, int]:
    # CNES EQ goes back to 2005 (seen in path /200508_/), so 00-99 mapping is safe as 2000+
    return 2000 + yy, mm


def _is_leq_max(year: int, month: int) -> bool:
    return (year, month) <= (MAX_YEAR, MAX_MONTH)


def _iter_target_names(all_names: Iterable[str]) -> list[str]:
    targets: list[str] = []
    for n in all_names:
        m = RE_EQ_FILE.match(n)
        if not m:
            continue
        yy = int(m.group("yy"))
        mm = int(m.group("mm"))
        year, month = _yy_mm_to_year_month(yy, mm)
        if _is_leq_max(year, month):
            targets.append(n)
    return sorted(targets)


def _download_single_file(filename: str) -> tuple[str, bool, str]:
    """
    Download a single file from FTP.
    
    Returns: (filename, success, message)
    """
    path = OUTDIR / filename
    
    if path.exists() and path.stat().st_size > 0:
        return (filename, None, f"skipped (exists)")
    
    # Retry logic with exponential backoff
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            ftp = ftplib.FTP(HOST, timeout=FTP_TIMEOUT)
            ftp.login()
            ftp.cwd(EQ_DIR)
            
            with open(path, "wb") as f:
                ftp.retrbinary(f"RETR {filename}", f.write)
            
            ftp.quit()
            
            file_size = path.stat().st_size
            return (filename, True, f"saved ({file_size:,} bytes)")
        except (ftplib.all_errors, EOFError, OSError) as e:
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY * attempt)
            else:
                if path.exists():
                    path.unlink()
                return (filename, False, f"{type(e).__name__}: {str(e)[:50]}")
    
    return (filename, False, "unknown error")


def main() -> None:
    OUTDIR.mkdir(parents=True, exist_ok=True)

    ftp = ftplib.FTP(HOST, timeout=FTP_TIMEOUT)
    ftp.login()
    ftp.cwd(EQ_DIR)

    all_names = ftp.nlst()
    targets = _iter_target_names(all_names)

    ftp.quit()

    print(f"ftp_listed_files: {len(all_names)}")
    print(f"targets_leq_202512: {len(targets)}")
    print(f"Starting parallel download with {PARALLEL_WORKERS} workers...\n")

    downloaded = 0
    skipped = 0
    failed = []
    failed_list = []

    with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:
        futures = {executor.submit(_download_single_file, n): n for n in targets}
        
        for idx, future in enumerate(as_completed(futures), 1):
            filename, success, message = future.result()
            
            if success is None:
                skipped += 1
                status = "⊘"
            elif success:
                downloaded += 1
                status = "✓"
            else:
                failed.append((filename, message))
                status = "✗"
            
            if idx % 50 == 0 or idx == len(targets):
                print(f"[{idx:5d}/{len(targets)}] {status} {filename}: {message}")

    print(f"\n=== Summary ===")
    print(f"Downloaded: {downloaded}")
    print(f"Skipped (already exist): {skipped}")
    print(f"Failed: {len(failed)}")
    if failed:
        print(f"\nFailed files:")
        for fname, err in failed[:10]:
            print(f"  - {fname}: {err}")
        if len(failed) > 10:
            print(f"  ... and {len(failed) - 10} more")
    print(f"Output dir: {OUTDIR.as_posix()}")


if __name__ == "__main__":
    main()
