"""Benchmark: peak memory of the analyse phase's staged-ledger read, old vs new (v1.10.13).

Builds a synthetic l1_wallet_ledger.csv (default 2.5 M rows, 150 wallets, realistic
widths) under --dir (default /tmp) and measures, each in its OWN subprocess, the max RSS
(resource.getrusage) of

  old   extract._load_ledger_rows_by_wallet: every wallet's rows as Python dicts at once
        (what v1.10.12 held through analyse / write / lift), then one pass per wallet;
  new   extract.open_ledger_rows with HL_UPSERT_STREAM_MIN_BYTES=0: the file in a DuckDB
        scratch database under HL_UPSERT_DUCKDB_MEMORY, one wallet's rows as dicts at a time.

Both passes compute the same checksum over the rows they serve (row count, sum of
amount_signed, hash of tx_hash / event_index order), so the numbers come with a parity
check. Never touches a state dir: everything lives under --dir and is removed unless
--keep. Note: on a host where /tmp is tmpfs the file and the scratch database are RAM
too (host memory, not the measured process RSS); pass --dir on a disk for a cold read.

    .venv/bin/python scripts/bench_ledger_rows.py --rows 2500000 --wallets 150 --duckdb-memory 4GB
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import os
import random
import resource
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from blueblocx_hyperliquid import schema as S  # noqa: E402

T0 = 1_762_500_000_000       # 2026-11-07T06:40:00Z (ms)


def build_csv(path: Path, rows: int, wallets: int, seed: int = 1) -> list[str]:
    rnd = random.Random(seed)
    addrs = ["0x" + hashlib.sha1(f"w{i}".encode()).hexdigest()[:40] for i in range(wallets)]
    tokens = [S.USDC_ADDRESS] + [f"hlcore:token:{i}" for i in range(1, 40)]
    types = ["spot_fill", "perp_fill", "funding", "deposit", "withdraw", "transfer_in", "transfer_out",
             "perp_realized_pnl"]
    # a skewed wallet distribution: the market-maker wallets own most of the rows
    weights = [1.0 / (i + 1) ** 0.6 for i in range(wallets)]
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(S.L1_WALLET_LEDGER_COLUMNS)
        span_ms = 45 * 86_400_000
        for i in range(rows):
            ts = T0 - span_ms + (span_ms * i) // rows            # sorted by time like the staged file
            iso = dt.datetime.fromtimestamp(ts / 1000, tz=dt.timezone.utc).isoformat()
            wallet = rnd.choices(addrs, weights)[0]
            tx = "0x" + hashlib.sha256(f"{i}".encode()).hexdigest()
            amt = round(rnd.uniform(-5000, 5000), 6)
            w.writerow([str(ts // 1000), iso, iso[:10], tx, "0", str(i % 4), rnd.choice(types), wallet, "w",
                        ("market:BTC/USDC" if i % 5 == 0 else ""), rnd.choice(tokens), "t",
                        ("in" if amt >= 0 else "out"), str(int(abs(amt) * 1e8)), "8", "hlcore", str(abs(amt)),
                        str(amt), iso, iso])
    return addrs


def _checksum(rows: list[dict], state: dict) -> None:
    state["rows"] += len(rows)
    state["amount"] += sum(r["amount_signed"] for r in rows)
    h = state["hash"]
    for r in rows:
        h.update(r["tx_hash"].encode())
        h.update(str(r["event_index"]).encode())


def child(mode: str, path: Path, wallets: list[str], duckdb_memory: str | None, tmp: Path) -> dict:
    from blueblocx_hyperliquid import extract
    state = {"rows": 0, "amount": 0.0, "hash": hashlib.sha256()}
    wanted = set(wallets)
    t0 = time.monotonic()
    rss_after_load = None
    largest = 0
    if mode == "old":
        by_wallet = extract._load_ledger_rows_by_wallet(path, wanted)
        rss_after_load = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        for w in wallets:
            rows = by_wallet.get(w, [])
            largest = max(largest, len(rows))
            _checksum(extract.merge_rows_by_key(rows, []), state)
    elif mode == "new":
        os.environ["HL_UPSERT_STREAM_MIN_BYTES"] = "0"
        if duckdb_memory:
            os.environ["HL_UPSERT_DUCKDB_MEMORY"] = duckdb_memory
        src = extract.open_ledger_rows(path, wanted, temp_dir=tmp / ".duckdb" / "upsert")
        assert src.kind == "duckdb", src.kind
        rss_after_load = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        try:
            for w in wallets:
                rows = src.rows(w)
                largest = max(largest, len(rows))
                _checksum(extract.merge_rows_by_key(rows, []), state)
        finally:
            src.close()
    else:
        raise SystemExit(f"unknown mode {mode}")
    ru = resource.getrusage(resource.RUSAGE_SELF)
    return {"mode": mode, "seconds": round(time.monotonic() - t0, 1),
            "max_rss_mb": round(ru.ru_maxrss / 1024, 1),                 # Linux: ru_maxrss in KiB
            "max_rss_after_load_mb": round((rss_after_load or 0) / 1024, 1),
            "rows": state["rows"], "amount_sum": round(state["amount"], 2),
            "order_hash": state["hash"].hexdigest()[:16], "largest_wallet_rows": largest}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--rows", type=int, default=2_500_000)
    ap.add_argument("--wallets", type=int, default=150)
    ap.add_argument("--dir", default=tempfile.gettempdir())
    ap.add_argument("--duckdb-memory", default=None,
                    help="HL_UPSERT_DUCKDB_MEMORY for the new path (default: the env / 16GB)")
    ap.add_argument("--modes", default="old,new")
    ap.add_argument("--keep", action="store_true", help="keep the synthetic file and the scratch dir")
    ap.add_argument("--child", nargs=2, metavar=("MODE", "SPEC"), help=argparse.SUPPRESS)
    args = ap.parse_args(argv)
    if args.child:
        mode, spec_path = args.child
        spec = json.loads(Path(spec_path).read_text(encoding="utf-8"))
        res = child(mode, Path(spec["csv"]), spec["wallets"], spec.get("duckdb_memory"), Path(spec["tmp"]))
        print(json.dumps(res))
        return 0
    tmp = Path(tempfile.mkdtemp(prefix="hl-bench-ledger-", dir=args.dir))
    try:
        csv_path = tmp / "l1_wallet_ledger.csv"
        t0 = time.monotonic()
        wallets = build_csv(csv_path, args.rows, args.wallets)
        size_mb = csv_path.stat().st_size / 1e6
        print(f"synthetic ledger: {args.rows} rows, {args.wallets} wallets, {size_mb:.0f} MB, built in "
              f"{time.monotonic() - t0:.0f}s at {csv_path}", flush=True)
        spec = tmp / "spec.json"
        spec.write_text(json.dumps({"csv": str(csv_path), "wallets": wallets, "tmp": str(tmp),
                                    "duckdb_memory": args.duckdb_memory}), encoding="utf-8")
        results = []
        for mode in args.modes.split(","):
            out = subprocess.run([sys.executable, str(Path(__file__).resolve()), "--child", mode, str(spec)],
                                 check=True, capture_output=True, text=True, cwd=str(ROOT))
            res = json.loads(out.stdout.strip().splitlines()[-1])
            results.append(res)
            print(f"{mode:>4}: max RSS {res['max_rss_mb']:>9.1f} MB (after load {res['max_rss_after_load_mb']:.1f} MB), "
                  f"{res['seconds']}s, rows {res['rows']}, amount_sum {res['amount_sum']}, "
                  f"order_hash {res['order_hash']}, largest wallet {res['largest_wallet_rows']} rows", flush=True)
        if len(results) == 2:
            a, b = results
            same = (a["rows"], a["amount_sum"], a["order_hash"]) == (b["rows"], b["amount_sum"], b["order_hash"])
            print(f"parity: {'OK' if same else 'MISMATCH'}; peak {a['max_rss_mb']:.0f} MB -> {b['max_rss_mb']:.0f} MB "
                  f"({a['max_rss_mb'] / max(b['max_rss_mb'], 1):.1f}x less), file {size_mb:.0f} MB")
            return 0 if same else 2
        return 0
    finally:
        if args.keep:
            print(f"kept {tmp}")
        else:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
