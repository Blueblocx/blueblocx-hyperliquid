"""
Operational plumbing around the scheduled run: atomic state writes, a
single-flight lock, the run summary (hard problems vs warnings) and the live
progress file. No business logic lives here — daily.py wraps extract.run with these.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
import socket
import time
from pathlib import Path

from . import __version__, config

log = logging.getLogger("blueblocx_hyperliquid.ops")

SUMMARY_FILE = "_run_summary.json"      # latest run, full detail
HISTORY_FILE = "_run_history.jsonl"     # one compact line per run, append-only
PROGRESS_FILE = "_run_progress.json"    # live state of the current run (phase, wallet i/N)


def utcnow() -> dt.datetime:
    return dt.datetime.now(tz=dt.timezone.utc)


def atomic_write_text(path: Path, text: str) -> None:
    """Sibling temp file + os.replace: a crash mid-write never leaves a half-written
    state file (staged CSV, watermarks, summaries) behind."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


# --- filesystem introspection --------------------------------------------------------

# Filesystem types where the STATE dir should not live: staged CSVs are rewritten
# whole on every upsert, the dev catalog is SQLite, and every run does thousands of
# small file operations - all slow or unsafe over a network filesystem. (The Iceberg
# WAREHOUSE on NFS is fine: large sequential files, written once.)
_NETWORK_FS = {"nfs", "nfs3", "nfs4", "cifs", "smb3", "smbfs", "9p", "fuse.sshfs", "fuse.rclone",
               "fuse.s3fs", "fuse.juicefs", "glusterfs", "ceph", "lustre", "afs", "davfs", "fuse.davfs2"}


def mount_fstype(target: str, mounts_text: str | None = None) -> tuple[str, str] | None:
    """(mount_point, fstype) of the longest-prefix mount containing `target`, read from
    /proc/mounts (Linux). None when the table is unavailable (Windows, macOS)."""
    if mounts_text is None:
        try:
            mounts_text = Path("/proc/mounts").read_text(encoding="utf-8")
        except OSError:
            return None
    target = target.rstrip("/") or "/"
    best: tuple[str, str] | None = None
    for line in mounts_text.splitlines():
        parts = line.split()
        if len(parts) < 3:
            continue
        mp = parts[1].replace("\\040", " ")
        if mp == "/" or target == mp or target.startswith(mp.rstrip("/") + "/"):
            if best is None or len(mp) > len(best[0]):
                best = (mp, parts[2])
    return best


def mount_entries(mounts_text: str | None = None) -> list[tuple[str, str, str]] | None:
    """(source, mount_point, fstype) per /proc/mounts line, in table order. None when
    the table is unavailable (Windows, macOS)."""
    if mounts_text is None:
        try:
            mounts_text = Path("/proc/mounts").read_text(encoding="utf-8")
        except OSError:
            return None
    out: list[tuple[str, str, str]] = []
    for line in mounts_text.splitlines():
        parts = line.split()
        if len(parts) >= 3:
            out.append((parts[0], parts[1].replace("\\040", " "), parts[2]))
    return out


def warehouse_mount_check(target: str, expected: str, mounts_text: str | None = None) -> str | None:
    """None when `target` lives on the filesystem mounted at `expected`, else the reason.

    Written for the mount table as seen from inside a systemd unit with
    ProtectSystem=strict + ReadWritePaths: every ReadWritePaths entry is its own bind
    mount, so the warehouse's longest-prefix mount is e.g. /mnt/blueblocx/hypercore
    (same source and fstype as /mnt/blueblocx), not the NAS mount point itself. Rules:
      1. something must be mounted at `expected`;
      2. that mount must be a separate filesystem, not a bind mount of the root disk
         (same source as `/`, non-network fstype) - the NAS-unmounted case;
      3. the warehouse's own mount is `expected`, or nested below it with the same
         fstype (a ReadWritePaths bind mount, or a nested export of the same NAS).
    Returns None without a mount table (Windows: nothing to check)."""
    entries = mount_entries(mounts_text)
    if entries is None:
        return None
    expected = expected.rstrip("/") or "/"
    target = target.rstrip("/") or "/"
    root = next((e for e in entries if e[1] == "/"), None)
    at_expected = [e for e in entries if (e[1].rstrip("/") or "/") == expected]
    if not at_expected:
        return f"nothing is mounted at {expected} - is the NAS mounted?"
    exp_src, _, exp_fs = at_expected[-1]            # the latest mount at that point wins
    if exp_fs not in _NETWORK_FS and root is not None and exp_src == root[0]:
        return (f"{expected} is a bind mount of the root disk ({exp_fs}, {exp_src}), not a separate "
                f"filesystem - is the NAS mounted?")
    best: tuple[str, str, str] | None = None
    for src, mp, fs in entries:
        mp_n = mp.rstrip("/") or "/"
        if mp_n == "/" or target == mp_n or target.startswith(mp_n + "/"):
            if best is None or len(mp_n) >= len(best[1].rstrip("/") or "/"):
                best = (src, mp_n, fs)
    if best is None:
        return f"no mount contains {target}"
    best_src, best_mp, best_fs = best
    if best_mp == expected:
        return None
    if best_mp.startswith(expected + "/") and best_fs == exp_fs:
        return None
    return (f"warehouse {target} is on mount {best_mp} ({best_fs}, {best_src}), not on {expected} "
            f"({exp_fs}) - is the NAS mounted there?")


def pid_alive(pid: int) -> bool | None:
    """True/False on Linux (via /proc), None where it cannot be told. Never uses
    os.kill(pid, 0): on Windows that terminates the target."""
    if not isinstance(pid, int) or pid <= 0:
        return None
    if Path("/proc").is_dir():
        return Path(f"/proc/{pid}").exists()
    return None


def pid_is_ours(pid: int) -> bool | None:
    """Is the live process `pid` one of this pipeline's entry points (python -m
    blueblocx_hyperliquid.<module>)? True/False from /proc/<pid>/cmdline on Linux;
    None where that cannot be read (Windows, permissions, process just exited).
    Guards the lock against a recycled PID: a lock whose PID now belongs to an
    unrelated process must not block the scheduler forever."""
    if not isinstance(pid, int) or pid <= 0:
        return None
    try:
        raw = Path(f"/proc/{pid}/cmdline").read_bytes()
    except OSError:
        return None
    if not raw:
        return None
    return b"blueblocx_hyperliquid" in raw


# --- single-flight lock ------------------------------------------------------------

class LockHeld(RuntimeError):
    """Another run holds the lock."""


class RunLock:
    """Single-flight guard shared by every writer of the state dir (daily, extract,
    iceberg_load, maintenance).

    Atomic O_EXCL create of a small JSON lock file holding pid/host/started_at. A
    lock is treated as abandoned - and replaced - when its recorded PID is no longer
    alive on this host (Linux, via /proc; a SIGKILL/OOM-killed run never reaches
    __exit__). A lock whose PID IS alive on this host AND is one of this pipeline's
    processes (/proc/<pid>/cmdline) is never discarded, however old: a hand-run S3
    backfill of a market-maker wallet legitimately runs for hours and stealing its
    lock would let the hourly tick interleave CSV writes, roll back watermarks and
    drop the backfill's rows from its own lift. A live PID that belongs to an
    unrelated process (PID recycled after a crash or reboot) is discarded. The age
    rule (HL_LOCK_STALE_HOURS) applies only when liveness or identity cannot be told
    (another host, no /proc, unreadable cmdline). Liveness is never probed with
    os.kill(pid, 0), which terminates the target on Windows. systemd timers never
    overlap a running service anyway; cron and hand runs do, hence the lock.
    """

    def __init__(self, path: Path, stale_hours: float | None = None):
        self.path = Path(path)
        hours = float(config.env("HL_LOCK_STALE_HOURS")) if stale_hours is None else stale_hours
        self.stale_s = hours * 3600.0

    def _holder(self) -> dict:
        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {}

    def _abandoned(self) -> str | None:
        """Reason the existing lock can be discarded, else None."""
        try:
            age = time.time() - self.path.stat().st_mtime
        except FileNotFoundError:
            return "vanished"
        holder = self._holder()
        if holder.get("host") in (None, socket.gethostname()):
            pid = holder.get("pid")
            alive = pid_alive(pid) if holder else None
            if alive is False:
                return f"pid {pid} is not running"
            if alive is True:
                ours = pid_is_ours(pid)
                if ours is False:
                    return f"pid {pid} was recycled by an unrelated process"
                if ours is True:
                    return None                  # a live pipeline run on this host is never stolen
                # cmdline unreadable: liveness known but identity not -> the age rule decides
        if age > self.stale_s:
            return f"{age / 3600:.1f} h old"
        return None

    def __enter__(self) -> "RunLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        for attempt in (1, 2):
            try:
                fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except FileExistsError:
                reason = self._abandoned()
                if reason == "vanished":
                    continue                         # released between our calls: retry once
                if attempt == 1 and reason:
                    log.warning("abandoned lock %s removed (%s)", self.path, reason)
                    self.path.unlink(missing_ok=True)
                    continue
                raise LockHeld(f"another run holds {self.path}: {json.dumps(self._holder())[:200]}")
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump({"pid": os.getpid(), "host": socket.gethostname(),
                           "started_at": utcnow().isoformat()}, fh)
            return self
        raise LockHeld(f"could not acquire {self.path}")

    def __exit__(self, *exc) -> bool:
        self.path.unlink(missing_ok=True)
        return False


# --- run summary --------------------------------------------------------------------

# HARD problems fail the run (exit 1; shown by the monitor):
#   natural_key        chain-wide (tx_hash, event_index, direction) collision — nothing written
#   skipped / missing  a wallet's core pulls failed, or it never produced a result
#   fidelity           mapped rows != raw API input (dropped / duplicated / sign-flipped rows)
#   perp_oracle        perp equity != HL portfolio.perpAllTime (or the oracle could not be read)
#   networth_oracle    computed net worth != HL portfolio.allTime
#   fills_truncated    the window is incomplete (API cap without a complete S3 backfill);
#                      for auto-tracked (leaderboard) wallets this is a WARNING instead -
#                      pinned wallets are what the product must have complete
#   fetch_errors       a non-core pull failed (staking / vaults / open orders / a HIP-3 dex)
#   tracked_unavailable  the leaderboard tracked set could not be refreshed and no cache
#                      was fresh enough: the run covered the pins only (added by daily.py)
#   error              the run raised
# WARNINGS are reported in the summary but do not fail the run:
#   pnl_oracle         HL perpDay.pnlHistory Δ24h != realized + Δunrealized (still being
#                      calibrated on live data; promote to hard once it is stable)
#   unmapped_ledger_types   a ledger delta.type the mapper does not know (rows skipped)
#   unpriced           a held non-stable symbol with no spot price (net worth understated)
#   deferred=N         the soft deadline left N wallets for the next run (rotating order)
#   networth_pending   net worth != HL, but the full-ledger lending lookup that could explain
#                      it was skipped by the soft deadline; the next run pulls it (then the
#                      wallet is either clean or a real networth_oracle problem)
#   tracked_stale=Xh   the leaderboard was unreachable; the cached tracked set was used


def build_summary(recon: dict | None, wallets: list[str], started_at: dt.datetime,
                  finished_at: dt.datetime, *, error: str | None = None,
                  options: dict | None = None, deferred: list[str] | None = None,
                  soft_wallets: set[str] | None = None) -> dict:
    """Turn extract.run's reconciliation into the operator-facing verdict.

    deferred: wallets the soft deadline left for the next run (status 'deferred', one
    warning, never a problem; their stale by_wallet entry from an earlier run is not
    re-judged). soft_wallets: auto-tracked wallets for which an API-capped window is
    a warning rather than a hard problem."""
    by = (recon or {}).get("by_wallet", {}) or {}
    deferred_set = set(deferred or [])
    soft = set(soft_wallets or [])
    problems: list[str] = []
    warnings: list[str] = []
    per_wallet: dict[str, dict] = {}
    if error:
        problems.append(f"error:{error}")
    if recon is not None and not recon.get("natural_key_ok", True):
        problems.append("natural_key")

    for w in wallets:
        short = w[:10]
        if w in deferred_set:
            per_wallet[w] = {"status": "deferred"}
            continue
        r = by.get(w) or {}
        if not r:
            per_wallet[w] = {"status": "missing"}
            if recon is not None:
                problems.append(f"{short}:missing")
            continue
        if r.get("skipped"):
            per_wallet[w] = {"status": "skipped", "errors": r.get("errors")}
            problems.append(f"{short}:skipped")
            continue
        oracle = r.get("oracle") or {}
        pnl = oracle.get("pnl") or {}
        entry = {
            "status": "ok",
            "fidelity_ok": bool(r.get("fidelity_ok")),
            "perp_oracle": oracle.get("account_value_matches"),
            "networth_oracle": oracle.get("networth_matches"),
            "pnl_oracle": (pnl.get("pnl_matches") if pnl.get("checked") else None),
            "fills_truncated": bool(r.get("fills_truncated_at_10k_cap")),
            "fetch_errors": sorted((r.get("fetch_errors") or {}).keys()),
            "l1_rows": r.get("l1_ledger_rows"),
            "raw_fills": r.get("raw_fills"),
            "open_positions": r.get("open_positions"),
            "lending_lazy_pull": bool(r.get("lending_lazy_pull")),
            "lending_deferred": bool(r.get("lending_deferred")),
            "unmapped_ledger_types": r.get("unmapped_ledger_types") or {},
            "unpriced_held_symbols": r.get("unpriced_held_symbols") or [],
        }
        hard: list[str] = []
        if not entry["fidelity_ok"]:
            hard.append("fidelity")
        if entry["perp_oracle"] is not True:
            hard.append("perp_oracle")
        if entry["networth_oracle"] is False:
            if entry["lending_deferred"]:
                # the one component that could explain the gap (borrow/lend supply) was not
                # pulled because the soft deadline had passed; the next run pulls it
                warnings.append(f"{short}:networth_pending")
            else:
                hard.append("networth_oracle")
        if entry["fills_truncated"]:
            if w in soft:
                warnings.append(f"{short}:fills_truncated")
            else:
                hard.append("fills_truncated")
        if entry["fetch_errors"]:
            hard.append("fetch_errors=" + ",".join(entry["fetch_errors"]))
        if entry["pnl_oracle"] is False:
            warnings.append(f"{short}:pnl_oracle")
        if entry["unmapped_ledger_types"]:
            warnings.append(f"{short}:unmapped_ledger_types=" + ",".join(sorted(entry["unmapped_ledger_types"])))
        if entry["unpriced_held_symbols"]:
            warnings.append(f"{short}:unpriced=" + ",".join(entry["unpriced_held_symbols"]))
        if hard:
            entry["status"] = "problem"
            problems += [f"{short}:{p}" for p in hard]
        per_wallet[w] = entry

    if deferred_set:
        warnings.append(f"deferred={len(deferred_set)}")
    # the native price step (v1.7.0) never fails a run: what it could not do is a warning,
    # and prices.history_pending == 0 is the ETL's onboarding gate
    prices = (recon or {}).get("prices")
    if prices:
        if prices.get("error"):
            warnings.append("prices:skipped")
        if prices.get("deferred"):
            warnings.append(f"prices_deferred={prices['deferred']}")
        if prices.get("history_pending"):
            warnings.append(f"prices_history_pending={prices['history_pending']}")
        if prices.get("errors"):
            warnings.append(f"prices_errors={len(prices['errors'])}")
        if (prices.get("early_lift") or {}).get("error"):
            warnings.append("prices:early_lift_failed")
    statuses = [e["status"] for e in per_wallet.values()]
    ok = recon is not None and not problems
    return {
        "version": __version__,
        "host": socket.gethostname(),
        "run_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_s": round((finished_at - started_at).total_seconds(), 1),
        "ok": ok,
        "wallets_total": len(wallets),
        "wallets_ok": statuses.count("ok"),
        "wallets_problem": statuses.count("problem"),
        "wallets_skipped": statuses.count("skipped") + statuses.count("missing"),
        "wallets_deferred": statuses.count("deferred"),
        "problems": problems,
        "warnings": warnings,
        "error": error,
        "natural_key_ok": (recon or {}).get("natural_key_ok"),
        "iceberg": (recon or {}).get("iceberg"),
        "retention": (recon or {}).get("retention"),
        "prices": prices or None,
        "hip3_dexes": (recon or {}).get("hip3_dexes_checked"),
        "unmapped_ledger_types": (recon or {}).get("unmapped_ledger_types") or {},
        "options": options or {},
        "per_wallet": per_wallet,
    }


def compact_summary(summary: dict) -> dict:
    """The history line: everything but the per-wallet detail (statuses only)."""
    out = {k: v for k, v in summary.items() if k not in ("per_wallet", "iceberg", "options")}
    out["per_wallet_status"] = {w: e.get("status") for w, e in (summary.get("per_wallet") or {}).items()}
    return out


def write_summary(ns_dir: Path, summary: dict) -> Path:
    ns_dir = Path(ns_dir)
    latest = ns_dir / SUMMARY_FILE
    atomic_write_text(latest, json.dumps(summary, indent=2, default=str))
    with (ns_dir / HISTORY_FILE).open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(compact_summary(summary), default=str) + "\n")
    return latest


def read_json(path: Path) -> dict | None:
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else None
    except (OSError, ValueError):
        return None


# --- live progress --------------------------------------------------------------------

class Progress:
    """Live state of the current run for `python -m blueblocx_hyperliquid.status`: one
    small JSON file (HL_OUT_DIR/hypercore/_run_progress.json) rewritten atomically at
    every phase change and after every wallet. The pull phase is otherwise quiet in
    the logs for minutes at a time, which looks exactly like a hang."""

    def __init__(self, ns_dir: Path, planned: int = 0):
        self.path = Path(ns_dir) / PROGRESS_FILE
        self.state: dict = {
            "state": "running", "pid": os.getpid(), "host": socket.gethostname(),
            "started_at": utcnow().isoformat(), "phase": "starting",
            "wallets_total": planned, "wallets_done": 0, "wallets_deferred": 0, "current": None,
        }
        self._write()

    def update(self, **fields) -> None:
        self.state.update(fields)
        self._write()

    def finish(self, **fields) -> None:
        self.update(state="finished", phase="done", current=None, finished_at=utcnow().isoformat(), **fields)

    def _write(self) -> None:
        self.state["updated_at"] = utcnow().isoformat()
        try:
            atomic_write_text(self.path, json.dumps(self.state, indent=2, default=str))
        except OSError as e:          # progress is observability, never a reason to fail the run
            log.warning("progress file %s not written: %s", self.path, e)


def read_progress(ns_dir: Path) -> dict | None:
    return read_json(Path(ns_dir) / PROGRESS_FILE)
