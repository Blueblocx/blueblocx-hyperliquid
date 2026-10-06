"""
HyperCore fills backfill from Hyperliquid's node-data bucket.

The Info API serves only the ~10,000 most recent fills per wallet, so any
window older than that must come from S3:

    s3://hl-mainnet-node-data/node_fills_by_block/hourly/{YYYYMMDD}/{hour}.lz4

Verified live (2026-10-05): one hour file is ~27 MB lz4 -> ~130 MB of JSON
lines; each line is a block envelope {local_time, block_time, block_number,
events: [[wallet, fill], ...]}; the fill dict has the SAME shape as an Info API
fill (coin/px/sz/side/time/startPosition/dir/closedPnl/hash/oid/crossed/fee/
tid/feeToken/twapId) PLUS node-only fields: builderFee, builder, cloid. The
bucket is Requester Pays — every call passes RequestPayer="requester" and the
caller is billed for transfer, so hour files are fetched only for the gap the
API could not serve.

Because the natural key is content-addressed and wallet-scoped
(schema.fill_tx_hash: wallet + tid), a fill seen from BOTH sources maps to the
same row — the CSV/Iceberg upsert dedups it. block_number is available here but
deliberately left NULL on the ledger row so rows are byte-identical across
sources (see docs/HYPERCORE_LEDGER_SPEC.md §1).

Usage (module):
    fills = fetch_fills_s3({"0xabc..."}, start_ms, end_ms)
CLI (validation / ad-hoc pull):
    python -m hl_poc.s3_backfill --wallet 0xA,0xB --hours 3   # last N hour files
"""
from __future__ import annotations

import argparse
import datetime as dt
import gzip
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Iterable, Iterator

log = logging.getLogger("hl_poc.s3_backfill")

BUCKET = "hl-mainnet-node-data"
PREFIX = "node_fills_by_block/hourly"
_HOUR_MS = 3_600_000


def load_dotenv(path: str | Path = ".env") -> None:
    """Minimal .env loader (no dependency). Never logs values."""
    p = Path(path)
    if not p.exists():
        return
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def _client():
    import boto3  # lazy: the rest of the package must import without boto3
    return boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-1"))


def has_credentials() -> bool:
    try:
        import boto3
        return boto3.Session().get_credentials() is not None
    except Exception:  # noqa: BLE001
        return False


def hour_keys(start_ms: int, end_ms: int) -> list[str]:
    """Object keys for every UTC hour overlapping [start_ms, end_ms)."""
    keys: list[str] = []
    h0 = start_ms - (start_ms % _HOUR_MS)
    t = h0
    while t < end_ms:
        d = dt.datetime.fromtimestamp(t / 1000, tz=dt.timezone.utc)
        keys.append(f"{PREFIX}/{d.strftime('%Y%m%d')}/{d.hour}.lz4")
        t += _HOUR_MS
    return keys


def _download(key: str) -> bytes | None:
    s3 = _client()
    try:
        body = s3.get_object(Bucket=BUCKET, Key=key, RequestPayer="requester")["Body"].read()
    except Exception as e:  # noqa: BLE001 — missing hour (not yet published) is normal
        log.warning("s3 get %s failed: %s", key, str(e)[:120])
        return None
    if key.endswith(".lz4"):
        import lz4.frame
        return lz4.frame.decompress(body)
    if key.endswith(".gz"):
        return gzip.decompress(body)
    return body


def _iter_wallet_fills(raw: bytes, wallets: set[str], start_ms: int,
                       end_ms: int) -> Iterator[tuple[str, dict]]:
    """Yield (wallet, fill) for fills of `wallets` within the window.

    Cheap substring pre-filter per line before json.loads: an hour file holds
    ~50k block envelopes and most contain none of our wallets.
    """
    needles = [w.encode("utf-8") for w in wallets]
    for line in raw.split(b"\n"):
        if not line or not any(n in line for n in needles):
            continue
        try:
            doc = json.loads(line)
        except json.JSONDecodeError:
            continue
        for ev in doc.get("events", []) or []:
            if not (isinstance(ev, list) and len(ev) == 2):
                continue
            w, fill = str(ev[0]).lower(), ev[1]
            if w not in wallets or not isinstance(fill, dict) or "tid" not in fill:
                continue
            t = int(fill.get("time", 0) or 0)
            if start_ms <= t < end_ms:
                yield w, fill


def fetch_fills_s3(wallets: Iterable[str], start_ms: int, end_ms: int, *,
                   workers: int = 8, batch: int = 8) -> dict[str, list[dict]]:
    """Return {wallet: [fills]} for the window, read from S3 hour files.

    Downloads `batch` hour files concurrently, parses them one at a time and
    frees each immediately (~130 MB decompressed each), repeats.
    """
    wset = {w.lower() for w in wallets}
    out: dict[str, list[dict]] = {w: [] for w in wset}
    keys = hour_keys(start_ms, end_ms)
    log.info("s3 backfill: %d hour file(s) for %d wallet(s) [%s .. %s)", len(keys), len(wset),
             dt.datetime.fromtimestamp(start_ms / 1000, tz=dt.timezone.utc).isoformat(),
             dt.datetime.fromtimestamp(end_ms / 1000, tz=dt.timezone.utc).isoformat())
    seen: set[tuple[str, int]] = set()
    for i in range(0, len(keys), batch):
        chunk = keys[i:i + batch]
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futs = {pool.submit(_download, k): k for k in chunk}
            raws = {}
            for fut in as_completed(futs):
                raws[futs[fut]] = fut.result()
        for k in chunk:
            raw = raws.pop(k, None)
            if not raw:
                continue
            n = 0
            for w, fill in _iter_wallet_fills(raw, wset, start_ms, end_ms):
                key = (w, int(fill.get("tid") or 0) or hash((fill.get("time"), fill.get("coin"), fill.get("sz"))))
                if key in seen:
                    continue
                seen.add(key)
                out[w].append(fill)
                n += 1
            log.info("  %s -> %d fill(s) for tracked wallets", k.rsplit("/", 2)[-2] + "/" + k.rsplit("/", 1)[-1], n)
            del raw
    return out


def merge_fills(api_fills: list[dict], s3_fills: list[dict]) -> list[dict]:
    """Union by tid (API wins on overlap, both shapes are identical anyway)."""
    by_tid: dict = {}
    for f in s3_fills:
        by_tid[f.get("tid")] = f
    for f in api_fills:
        by_tid[f.get("tid")] = f
    return sorted(by_tid.values(), key=lambda f: (int(f.get("time", 0) or 0), f.get("tid") or 0))


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    ap = argparse.ArgumentParser(description="Pull HyperCore fills for wallets from S3 node data")
    ap.add_argument("--wallet", required=True, help="comma-separated addresses")
    ap.add_argument("--hours", type=int, default=2, help="how many most-recent hour files")
    ap.add_argument("--end-ms", type=int, default=None, help="window end (default: now)")
    ap.add_argument("--out", default=None, help="write fills JSON here (optional)")
    args = ap.parse_args()
    load_dotenv()
    now = int(dt.datetime.now(tz=dt.timezone.utc).timestamp() * 1000)
    end_ms = args.end_ms or now
    start_ms = end_ms - args.hours * _HOUR_MS
    wallets = [w.strip().lower() for w in args.wallet.split(",") if w.strip()]
    res = fetch_fills_s3(wallets, start_ms, end_ms)
    for w, fills in res.items():
        log.info("%s: %d fills", w[:10], len(fills))
    if args.out:
        Path(args.out).write_text(json.dumps(res), encoding="utf-8")


if __name__ == "__main__":
    main()
