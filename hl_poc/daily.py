"""
Daily runner: one command a scheduler (cron / dagster / systemd timer) calls.

    python -m hl_poc.daily --wallets-file wallets.txt [--iceberg] [--s3-backfill] [--with-prices]

What it does, in order:
  1. extract.run(...) for every wallet in the file — incremental (watermark per
     wallet, 1h overlap), HIP-3 dexes included, l4x snapshot for today
     (replace-partition), optional S3 backfill for 10k-capped wallets, optional
     lift into Iceberg.
  2. optionally the shared L3 price job for the tokens those wallets hold.
Exit code is non-zero on a natural-key violation (extract raises) so the
scheduler sees the failure. A dagster asset template that wraps this lives in
deploy/dagster_hypercore_assets.py.
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from . import extract
from .s3_backfill import load_dotenv

log = logging.getLogger("hl_poc.daily")


def read_wallets(path: Path) -> list[str]:
    out = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.split("#")[0].strip()
        if line:
            out.append(line.lower())
    return out


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    ap = argparse.ArgumentParser(description="HyperCore daily extract for a wallet list")
    ap.add_argument("--wallets-file", default="wallets.txt")
    ap.add_argument("--days", type=int, default=2, help="default window when no watermark exists")
    ap.add_argument("--iceberg", action="store_true")
    ap.add_argument("--s3-backfill", action="store_true")
    ap.add_argument("--with-prices", action="store_true")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--out", default=str(extract.OUT_ROOT))
    args = ap.parse_args()
    load_dotenv()
    wallets = read_wallets(Path(args.wallets_file))
    if not wallets:
        log.error("no wallets in %s", args.wallets_file)
        return 2
    recon = extract.run(wallets, args.days, Path(args.out), with_prices=args.with_prices,
                        max_workers=args.workers, s3_backfill=args.s3_backfill,
                        iceberg=args.iceberg, incremental=True)
    mine = {w: recon["by_wallet"].get(w, {}) for w in wallets}
    skipped = [w for w, r in mine.items() if r.get("skipped")]
    done = [r for r in mine.values() if r and not r.get("skipped")]
    problems = []
    if not recon["natural_key_ok"]:
        problems.append("natural_key")
    if skipped:
        problems.append(f"skipped={len(skipped)}")
    for w, r in mine.items():
        if not r or r.get("skipped"):
            continue
        o = r.get("oracle") or {}
        if not r.get("fidelity_ok"):
            problems.append(f"{w[:10]}:fidelity")
        if not o.get("account_value_matches"):
            problems.append(f"{w[:10]}:perp_oracle")
        if o.get("networth_matches") is False:
            problems.append(f"{w[:10]}:networth_oracle")
        if r.get("fills_truncated_at_10k_cap"):
            problems.append(f"{w[:10]}:fills_truncated")
        if r.get("fetch_errors"):
            problems.append(f"{w[:10]}:fetch_errors={sorted(r['fetch_errors'])}")
    ok = not problems and len(done) == len(wallets)
    log.info("daily done: wallets=%d ok=%s problems=%s", len(wallets), ok, problems)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
