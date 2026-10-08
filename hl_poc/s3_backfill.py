"""
HyperCore fills backfill from Hyperliquid's node-data bucket.

The Info API serves only the ~10,000 most recent fills per wallet, so any
window older than that must come from S3:

    s3://hl-mainnet-node-data/node_fills_by_block/hourly/{YYYYMMDD}/{hour}.lz4

Verified live (2026-10-05/07): one hour file is 25-70 MB lz4 (~4.8x -> up to
~330 MB of JSON lines); each line is a block envelope {local_time, block_time,
block_number, events: [[wallet, fill], ...]}; the fill dict has the SAME shape as
an Info API fill PLUS node-only fields: builderFee, builder, cloid, deployerFee,
priorityGas, liquidation. fill.time == block_time. Hour H is published ~H+1:02
UTC. Requester Pays — every call passes RequestPayer="requester".

Completeness is REPORTED, never assumed: fetch_fills_s3 returns the keys it
requested, the keys that were missing (not yet published / NoSuchKey) and the
keys that failed; callers must treat a non-empty missing/failed set as an
incomplete window. Credential / permission errors are raised immediately.

Memory: workers download COMPRESSED bytes only (<= batch x 70 MB); each file is
decompressed and parsed line-by-line in the main thread and released before the
next one.

Usage (module):
    res = fetch_fills_s3({"0xabc..."}, start_ms, end_ms)
    res.fills["0xabc..."], res.keys_missing, res.keys_failed, res.complete
CLI (validation / ad-hoc pull):
    python -m hl_poc.s3_backfill --wallet 0xA,0xB --hours 3
"""
from __future__ import annotations

import argparse
import datetime as dt
import gzip
import io
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Iterator

log = logging.getLogger("hl_poc.s3_backfill")

BUCKET = "hl-mainnet-node-data"
PREFIX = "node_fills_by_block/hourly"
_HOUR_MS = 3_600_000


class S3AccessError(RuntimeError):
    """Credentials missing or the account cannot pay for Requester-Pays access."""


@dataclass
class S3Result:
    fills: dict[str, list[dict]]
    keys_requested: list[str] = field(default_factory=list)
    keys_missing: list[str] = field(default_factory=list)
    keys_failed: list[str] = field(default_factory=list)

    @property
    def complete(self) -> bool:
        return not self.keys_missing and not self.keys_failed

    def summary(self) -> dict:
        return {"keys_requested": len(self.keys_requested), "keys_missing": self.keys_missing,
                "keys_failed": self.keys_failed, "complete": self.complete,
                "fills": {w: len(v) for w, v in self.fills.items()}}


def load_dotenv(path: str | Path | None = None) -> None:
    """Back-compat shim for config.load_env (explicit path, $HL_ENV_FILE, ./.env,
    <repo>/.env; never overrides the process environment; never logs values)."""
    from . import config
    config.load_env(path)


def _client():
    import boto3  # lazy: the rest of the package must import without boto3
    return boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-1"))


def has_credentials() -> bool:
    try:
        import boto3
        return boto3.Session().get_credentials() is not None
    except Exception:  # noqa: BLE001
        return False


def fill_key(f: dict):
    """Identity of a fill: tid, or for tid=0 dust conversions (time, coin, sz) —
    the SAME identity schema.fill_tx_hash uses, so merge/dedup never collapses
    several dust fills into one."""
    tid = f.get("tid")
    if tid:
        return ("tid", int(tid))
    return ("dust", int(f.get("time", 0) or 0), f.get("coin"), str(f.get("sz")))


def hour_keys(start_ms: int, end_ms: int) -> list[str]:
    """Object keys for every UTC hour overlapping [start_ms, end_ms)."""
    keys: list[str] = []
    t = start_ms - (start_ms % _HOUR_MS)
    while t < end_ms:
        d = dt.datetime.fromtimestamp(t / 1000, tz=dt.timezone.utc)
        keys.append(f"{PREFIX}/{d.strftime('%Y%m%d')}/{d.hour}.lz4")
        t += _HOUR_MS
    return keys


def _download_compressed(key: str) -> tuple[str, bytes | None]:
    """Return (status, compressed_bytes). status: ok | missing | failed.
    Raises S3AccessError on credential / permission problems (fail fast)."""
    from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
    s3 = _client()
    try:
        return "ok", s3.get_object(Bucket=BUCKET, Key=key, RequestPayer="requester")["Body"].read()
    except NoCredentialsError as e:
        raise S3AccessError(f"no AWS credentials: {e}") from e
    except ClientError as e:
        code = str(e.response.get("Error", {}).get("Code", ""))
        status = str(e.response.get("ResponseMetadata", {}).get("HTTPStatusCode", ""))
        if code in ("NoSuchKey", "404") or status == "404":
            return "missing", None
        if code in ("AccessDenied", "403", "InvalidAccessKeyId", "SignatureDoesNotMatch") or status == "403":
            raise S3AccessError(f"S3 access denied for {key}: {code}") from e
        log.warning("s3 get %s failed: %s", key, code or str(e)[:120])
        return "failed", None
    except BotoCoreError as e:
        log.warning("s3 get %s failed: %s", key, str(e)[:120])
        return "failed", None


def _decompress(key: str, body: bytes) -> bytes:
    if key.endswith(".lz4"):
        import lz4.frame
        return lz4.frame.decompress(body)
    if key.endswith(".gz"):
        return gzip.decompress(body)
    return body


def _iter_wallet_fills(raw: bytes, wallets: set[str], start_ms: int,
                       end_ms: int) -> Iterator[tuple[str, dict]]:
    """Yield (wallet, fill) for fills of `wallets` within the window.

    Streams lines from a BytesIO (no second full copy). Cheap substring
    pre-filter per line before json.loads: an hour file holds ~50k block
    envelopes and most contain none of our wallets. Addresses in node data are
    lowercase (verified on 1,807 distinct addresses); both cases are tried.
    """
    needles = []
    for w in wallets:
        needles.append(w.encode("utf-8"))
        needles.append(w.upper().replace("0X", "0x").encode("utf-8"))
    for line in io.BytesIO(raw):
        if not line.strip() or not any(n in line for n in needles):
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
                   workers: int = 8, batch: int = 8) -> S3Result:
    """Read fills for `wallets` in [start_ms, end_ms) from S3 hour files.

    Compressed bytes are downloaded `batch` at a time; each file is decompressed
    and parsed in the main thread, then released. Missing (unpublished) and
    failed keys are reported in the result — the caller decides completeness.
    """
    wset = {w.lower() for w in wallets}
    res = S3Result(fills={w: [] for w in wset})
    res.keys_requested = hour_keys(start_ms, end_ms)
    log.info("s3 backfill: %d hour file(s) for %d wallet(s) [%s .. %s)", len(res.keys_requested), len(wset),
             dt.datetime.fromtimestamp(start_ms / 1000, tz=dt.timezone.utc).isoformat(),
             dt.datetime.fromtimestamp(end_ms / 1000, tz=dt.timezone.utc).isoformat())
    seen: set = set()
    keys = res.keys_requested
    for i in range(0, len(keys), batch):
        chunk = keys[i:i + batch]
        compressed: dict[str, tuple[str, bytes | None]] = {}
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futs = {pool.submit(_download_compressed, k): k for k in chunk}
            for fut in as_completed(futs):
                compressed[futs[fut]] = fut.result()      # S3AccessError propagates
        del futs
        for k in chunk:
            status, body = compressed.pop(k, ("failed", None))
            if status == "missing":
                res.keys_missing.append(k)
                continue
            if status != "ok" or body is None:
                res.keys_failed.append(k)
                continue
            raw = _decompress(k, body)
            del body
            n = 0
            for w, fill in _iter_wallet_fills(raw, wset, start_ms, end_ms):
                key = (w, fill_key(fill))
                if key in seen:
                    continue
                seen.add(key)
                res.fills[w].append(fill)
                n += 1
            del raw
            log.info("  %s -> %d fill(s) for tracked wallets", "/".join(k.rsplit("/", 2)[-2:]), n)
    if not res.complete:
        log.warning("s3 backfill INCOMPLETE: %d missing, %d failed hour file(s)",
                    len(res.keys_missing), len(res.keys_failed))
    return res


def merge_fills(api_fills: list[dict], s3_fills: list[dict]) -> list[dict]:
    """Union by fill identity (tid, or the dust (time, coin, sz) key). API wins
    on overlap — both sources carry identical field values for the same fill."""
    merged: dict = {}
    for f in s3_fills:
        merged[fill_key(f)] = f
    for f in api_fills:
        merged[fill_key(f)] = f
    return sorted(merged.values(), key=lambda f: (int(f.get("time", 0) or 0), int(f.get("tid") or 0)))


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
    log.info("result: %s", json.dumps(res.summary()))
    if args.out:
        Path(args.out).write_text(json.dumps(res.fills), encoding="utf-8")


if __name__ == "__main__":
    main()
