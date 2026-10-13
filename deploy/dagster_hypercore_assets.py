"""
TEMPLATE — dagster assets for the HyperCore pipeline, shaped like the existing
atlas per-chain assets (assets/<chain>/l1_assets.py ...). Not wired into the
atlas repo from here; copy into services/dagster/pipeline/assets/hypercore/ and
register the group. Runs the CLI as a subprocess (the same way python-api runs
the Rust binaries), so the asset code stays thin.

Schedule suggestion: hypercore_l1_daily at 00:20 UTC (after HL's midnight dust
conversions settle), hypercore_l3_prices right after.
"""
from __future__ import annotations

import os
import subprocess
import sys

from dagster import AssetExecutionContext, asset  # type: ignore

PY = os.environ.get("HYPERCORE_PYTHON", sys.executable)
REPO = os.environ.get("HYPERCORE_REPO", "/opt/blueblocx-hyperliquid")
WALLETS = os.environ.get("HYPERCORE_WALLETS_FILE", f"{REPO}/wallets.txt")


def _run(context: AssetExecutionContext, args: list[str]) -> None:
    cmd = [PY, "-m", *args]
    context.log.info("running: %s", " ".join(cmd))
    proc = subprocess.run(cmd, cwd=REPO, capture_output=True, text=True)
    # Python logging goes to stderr: keep the tail on success as well.
    context.log.info("stdout: %s", proc.stdout[-4000:])
    context.log.info("stderr: %s", proc.stderr[-4000:])
    if proc.returncode != 0:
        raise RuntimeError(f"{args[0]} exited {proc.returncode}")


@asset(group_name="hypercore", compute_kind="python")
def hypercore_l1_daily(context: AssetExecutionContext) -> None:
    """l1_wallet_ledger / l1_token_metadata / l4x snapshot / gates for the wallet
    list — incremental, HIP-3 aware, S3 backfill for 10k-capped wallets, lifted
    into nessie.hypercore.* (NESSIE_URI from the environment). Preflight first so a
    bad key / unreachable catalog fails before any API weight is spent; the daily
    runner writes _run_summary.json (exit 1 on problems)."""
    _run(context, ["blueblocx_hyperliquid.preflight", "--iceberg", "--s3", "--wallets-file", WALLETS])
    _run(context, ["blueblocx_hyperliquid.daily", "--wallets-file", WALLETS, "--iceberg", "--s3-backfill"])


@asset(group_name="hypercore", compute_kind="python", deps=[hypercore_l1_daily])
def hypercore_l3_prices(context: AssetExecutionContext) -> None:
    """Shared per-token l3_token_price_usd_1m for the held tokens (price_job)."""
    spot = os.environ.get("HYPERCORE_PRICE_SPOT", "HYPE,UBTC,UETH")
    perp = os.environ.get("HYPERCORE_PRICE_PERP", "BTC,ETH,HYPE")
    _run(context, ["blueblocx_hyperliquid.price_job", "--spot", spot, "--perp", perp, "--days", "3"])
