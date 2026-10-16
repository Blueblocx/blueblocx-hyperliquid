"""Read-only SQL over the lake (blueblocx_hyperliquid.query): a local catalog built with the
loader itself, then DuckDB queries through the lazily loading Lake. No network. duckdb is a
pinned dependency (requirements.txt), so these tests import it unconditionally."""
from __future__ import annotations

import datetime as dt
import decimal
import io
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock

from blueblocx_hyperliquid import iceberg_load as il
from blueblocx_hyperliquid import preflight
from blueblocx_hyperliquid import query as q

WALLETS = ["0xaaaa000000000000000000000000000000000001", "0xbbbb000000000000000000000000000000000002"]
DATES = ["2026-09-01", "2026-10-12", "2026-10-13"]          # one old day + two recent ones
TYPES = ["fee", "perp_realized_pnl", "transfer"]
NOW = "2026-10-14T00:00:00+00:00"


def _env(root: Path, **extra):
    base = {"NESSIE_URI": "", "HL_CATALOG_URI": "", "WAREHOUSE": "", "HL_WAREHOUSE_MOUNT": "", "HL_ENV_FILE": "",
            "HL_OUT_DIR": str(root)}
    base.update(extra)
    return mock.patch.dict(os.environ, base, clear=False)


def _ledger_rows() -> list[dict]:
    rows, n = [], 0
    for w in WALLETS:
        for d in DATES:
            for i, et in enumerate(TYPES):
                n += 1
                direction = "out" if et == "fee" else "in"
                amt = float(n)
                rows.append({"block_number": None, "block_timestamp": f"{d}T0{i}:30:00+00:00", "block_date": d,
                             "tx_hash": f"hlcore:{w}:tid:{n}", "tx_index": None, "event_index": i, "event_type": et,
                             "wallet_address": w, "wallet_group": 1, "counterparty_address": "BTC",
                             "token_address": "hlcore:usdc", "token_group": 0, "direction": direction,
                             "amount_raw": amt * 1e6, "decimals": 6, "standard": "ERC20", "amount": amt,
                             "amount_signed": -amt if direction == "out" else amt, "created_at": NOW, "updated_at": NOW})
    return rows


def build_lake(root: Path) -> None:
    """l1_wallet_ledger (18 rows, 3 days), l4x (2 rows, one day), token_whitelist (1 row),
    control.table_watermarks (1 row) in a local SQLite catalog under root/iceberg."""
    with _env(root):
        catalog, mode = il.load_catalog_from_env()
        assert mode == "local"
        try:
            ddl = il.parse_ddl()
            spec = ddl["l1_wallet_ledger"]
            t = il.ensure_table(catalog, "hypercore", "l1_wallet_ledger", spec)
            il.load_table(t, il.rows_to_arrow(_ledger_rows(), spec["columns"]), *il.TABLE_PLAN["l1_wallet_ledger"])
            spec = ddl["l4x_perp_position_snapshot"]
            t = il.ensure_table(catalog, "hypercore", "l4x_perp_position_snapshot", spec)
            snap_rows = [{"as_of_ts": "2026-10-13T12:00:00+00:00", "as_of_date": "2026-10-13", "wallet_address": w,
                          "wallet_group": 1, "coin": "BTC", "dex": "main", "szi": 1.0, "entry_px": 100.0,
                          "position_value": 100.0, "mark_px": 101.0, "unrealized_pnl_usd": 1.0, "return_on_equity": 0.01,
                          "liquidation_px": 50.0, "margin_used_usd": 10.0, "leverage_type": "cross", "leverage_value": 10.0,
                          "cum_funding_all_time": 0.0, "cum_funding_since_open": 0.0, "account_value_usd": 1000.0,
                          "withdrawable_usd": 900.0, "created_at": NOW} for w in WALLETS]
            il.load_table(t, il.rows_to_arrow(snap_rows, spec["columns"]), *il.TABLE_PLAN["l4x_perp_position_snapshot"])
            spec = ddl["token_whitelist"]
            t = il.ensure_table(catalog, "hypercore", "token_whitelist", spec)
            il.load_table(t, il.rows_to_arrow([{"token_address": "hlcore:usdc", "token_group": 0, "is_active": True,
                                                "whitelist_reason": "hypercore_token", "created_at": NOW,
                                                "updated_at": NOW}], spec["columns"]), *il.TABLE_PLAN["token_whitelist"])
            il.write_watermarks(catalog, "hypercore",
                                [("l1_wallet_ledger", "block_timestamp",
                                  dt.datetime(2026, 10, 13, 2, 30, tzinfo=dt.timezone.utc), 18, None)], "test")
        finally:
            q.close_catalog(catalog)


class ReadOnlyCatalogTests(unittest.TestCase):
    """The query tool must never create catalog tables, directories or database files."""

    def test_sql_props_read_only_never_inits_catalog_tables(self):
        with tempfile.TemporaryDirectory() as d, _env(Path(d)):
            self.assertEqual(il._sql_props("sqlite:///x.db", read_only=True)["init_catalog_tables"], "false")
            # without WAREHOUSE the lift creates the local dir; the reader must not
            self.assertFalse((Path(d) / "iceberg").exists())
            self.assertNotIn("init_catalog_tables", il._sql_props("sqlite:///x.db"))
            self.assertTrue((Path(d) / "iceberg").exists())

    def test_missing_local_catalog_is_an_error_not_a_new_catalog(self):
        with tempfile.TemporaryDirectory() as d, _env(Path(d)):
            with self.assertRaises(FileNotFoundError):
                il.load_catalog_from_env(read_only=True)
            self.assertFalse((Path(d) / "iceberg").exists())

    def test_missing_sqlite_catalog_file_in_sql_mode_is_an_error_for_every_spelling(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            for uri in (f"sqlite:///{(root / 'nope.db').as_posix()}", f"sqlite+pysqlite:///{(root / 'nope2.db').as_posix()}"):
                with _env(root, HL_CATALOG_URI=uri, WAREHOUSE=f"file://{(root / 'wh').as_posix()}"):
                    with self.assertRaises(FileNotFoundError):
                        il.load_catalog_from_env(read_only=True)
            self.assertEqual(sorted(p.name for p in root.iterdir()), [])

    def test_sqlite_file_detection(self):
        self.assertEqual(il._sqlite_file("sqlite:////var/lib/x/catalog.db"), Path("/var/lib/x/catalog.db"))
        self.assertEqual(il._sqlite_file("sqlite+pysqlite:////tmp/x.db"), Path("/tmp/x.db"))
        self.assertIsNone(il._sqlite_file("sqlite://"))
        self.assertIsNone(il._sqlite_file("sqlite:///:memory:"))
        self.assertIsNone(il._sqlite_file("postgresql+psycopg2://hl:pw@localhost:5432/hl_catalog"))
        self.assertIsNone(il._sqlite_file("not a url"))


class SqlTextTests(unittest.TestCase):
    def test_split_statements(self):
        text = "select 'a;b' as x; -- a comment; with semicolon\nselect \"q;q\" from t;\n\nselect 3"
        self.assertEqual(q.split_statements(text), ["select 'a;b' as x", 'select "q;q" from t', "select 3"])
        self.assertEqual(q.split_statements("  ;; "), [])
        self.assertEqual(q.split_statements("select 'it''s'; select 2"), ["select 'it''s'", "select 2"])
        # block comments: a ';' inside one is not a separator, and the comment is dropped
        self.assertEqual(q.split_statements("/* header; v1 */\nselect 1 /* note; */ as x; select 2"),
                         ["select 1  as x", "select 2"])
        self.assertEqual(q.split_statements("select '/* not a comment */' as s"), ["select '/* not a comment */' as s"])

    def test_complete(self):
        self.assertTrue(q.complete("select 1;"))
        self.assertFalse(q.complete("select 1"))
        self.assertFalse(q.complete("select 'a;"))
        self.assertFalse(q.complete("select 1; select"))
        self.assertFalse(q.complete("select 1 /* open ;"))
        self.assertTrue(q.complete("select 1; -- trailing comment"))
        self.assertTrue(q.complete("select 1;\n  -- more\n"))
        self.assertTrue(q.complete("select 1; /* done */"))

    def test_bound(self):
        today = dt.date(2026, 10, 14)
        self.assertEqual(q.Bound.from_args(30, None, None, today=today).since, dt.date(2026, 9, 14))
        self.assertEqual(q.Bound.from_args(0, None, None, today=today), q.Bound())
        b = q.Bound.from_args(30, "2026-10-01", "2026-10-13", today=today)
        self.assertEqual(b.row_filter("block_date"), "block_date >= '2026-10-01' and block_date <= '2026-10-13'")
        # --until alone: the N-day window ends there instead of colliding with today's window
        b = q.Bound.from_args(7, None, "2026-09-10", today=today)
        self.assertEqual((b.since, b.until), (dt.date(2026, 9, 3), dt.date(2026, 9, 10)))
        self.assertEqual(q.Bound.from_args(0, None, "2026-09-10", today=today), q.Bound(None, dt.date(2026, 9, 10)))
        self.assertEqual(q.Bound().row_filter("block_date"), None)
        self.assertEqual(q.Bound().describe(), "all rows")
        with self.assertRaises(ValueError):
            q.Bound.from_args(0, "2026-10-13", "2026-10-01")
        with self.assertRaises(ValueError):
            q.Bound.from_args(0, "yesterday", None)

    def test_formats(self):
        cols = ["d", "ts", "n", "s"]
        rows = [[dt.date(2026, 10, 12), dt.datetime(2026, 10, 12, 0, 30, tzinfo=dt.timezone.utc), 1.5, None],
                [dt.date(2026, 10, 13), None, 2, "x"]]
        table = q.format_rows(cols, rows, "table")
        self.assertIn("2026-10-12 00:30:00+00:00", table)
        self.assertIn("NULL", table)
        self.assertTrue(table.endswith("(2 rows)"))
        self.assertIn("1 more not shown", q.format_rows(cols, rows, "table", max_rows=1))
        csv_text = q.format_rows(cols, rows, "csv")
        self.assertEqual(csv_text.splitlines()[0], "d,ts,n,s")
        self.assertEqual(csv_text.splitlines()[1], "2026-10-12,2026-10-12T00:30:00+00:00,1.5,")
        lines = q.format_rows(cols, rows, "json").splitlines()
        self.assertEqual(len(lines), 2)
        self.assertIn('"s": "x"', lines[1])
        self.assertEqual(q.format_rows([], [], "table"), "(ok)")

    def test_formats_hugeint_decimal_bytes_nan(self):
        cols = ["total", "dec", "b", "nan"]
        rows = [[decimal.Decimal("1099511627777"), decimal.Decimal("1.500"), b"\x01\xff", float("nan")]]
        self.assertEqual(q.format_rows(cols, rows, "json"), '{"total": 1099511627777, "dec": 1.5, "b": "01ff", "nan": null}')
        self.assertEqual(q.format_rows(cols, rows, "csv").splitlines()[1], "1099511627777,1.500,01ff,nan")
        table = q.format_rows(cols, rows, "table").splitlines()
        self.assertEqual(table[2].split(" | ")[0], "1099511627777")          # numeric, right-aligned to its width
        self.assertIn("01ff", table[2])

    def test_scrub_hides_secret_values(self):
        with mock.patch.dict(os.environ, {"HL_CATALOG_URI": "postgresql+psycopg2://hl:s3cr3t@db.local:5432/hl_catalog",
                                          "S3_SECRET_KEY": "topsecret"}):
            out = q.scrub("failed: postgresql+psycopg2://hl:s3cr3t@db.local:5432/hl_catalog and s3cr3t and topsecret")
            self.assertNotIn("s3cr3t", out)
            self.assertNotIn("topsecret", out)
            self.assertIn("<HL_CATALOG_URI>", out)


class LakeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.root = Path(cls.tmp.name)
        build_lake(cls.root)

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def _lake(self, bound=q.Bound()) -> q.Lake:
        with _env(self.root):
            catalog, mode = il.load_catalog_from_env(read_only=True)
        try:
            lake = q.Lake(catalog, bound=bound, mode=mode)
        except Exception:
            q.close_catalog(catalog)
            raise
        self.addCleanup(lake.close)
        return lake

    def test_tables_and_lazy_loading(self):
        lake = self._lake()
        self.assertEqual(sorted(lake.idents()), ["l1_wallet_ledger", "l4x_perp_position_snapshot", "table_watermarks",
                                                 "token_whitelist"])
        self.assertEqual(lake.idents()["table_watermarks"], "control.table_watermarks")
        cols, rows = lake.run("select count(*) n from l1_wallet_ledger")
        self.assertEqual((cols, rows), (["n"], [[18]]))
        self.assertEqual(list(lake.loaded), ["l1_wallet_ledger"])           # only what the SQL named
        self.assertEqual(lake.loaded["l1_wallet_ledger"]["table_rows"], 18)  # metadata count, not a scan
        # namespace-qualified names and case-insensitive references work; joins load both tables
        cols, rows = lake.run("SELECT count(*) FROM hypercore.L1_WALLET_LEDGER l JOIN token_whitelist t USING (token_address)")
        self.assertEqual(rows, [[18]])
        self.assertIn("token_whitelist", lake.loaded)
        cols, rows = lake.run("select table_name, row_count from control.table_watermarks")
        self.assertEqual(rows, [["l1_wallet_ledger", 18]])

    def test_references_ignore_string_literals_and_comments(self):
        lake = self._lake()
        sql = "select table_name from table_watermarks where table_name = 'l1_wallet_ledger' -- l4x_perp_position_snapshot"
        self.assertEqual(lake.referenced(sql), ["table_watermarks"])
        self.assertEqual(lake.referenced('select * from "L1_WALLET_LEDGER" /* token_whitelist */'), ["l1_wallet_ledger"])
        lake.run(sql)
        self.assertEqual(list(lake.loaded), ["table_watermarks"])
        self.assertEqual(lake.resolve("hypercore.l1_wallet_ledger"), "l1_wallet_ledger")
        self.assertEqual(lake.resolve('"Table_Watermarks"'), "table_watermarks")
        self.assertIsNone(lake.resolve("nope"))

    def test_bound_prunes_old_days_and_leaves_dimensions_whole(self):
        lake = self._lake(q.Bound(since=dt.date(2026, 10, 1)))
        self.assertEqual(lake.run("select count(*) from l1_wallet_ledger")[1], [[12]])
        self.assertEqual(lake.loaded["l1_wallet_ledger"]["filter"], "block_date >= '2026-10-01'")
        self.assertEqual(lake.run("select count(*) from l4x_perp_position_snapshot")[1], [[2]])
        self.assertEqual(lake.run("select count(*) from token_whitelist")[1], [[1]])
        lake2 = self._lake(q.Bound(since=dt.date(2026, 10, 13), until=dt.date(2026, 10, 13)))
        self.assertEqual(lake2.run("select count(*) from l1_wallet_ledger")[1], [[6]])

    def test_timestamps_come_back_in_utc_without_pytz(self):
        lake = self._lake()
        cols, rows = lake.run("select min(block_timestamp) ts, min(block_date) d from l1_wallet_ledger")
        self.assertEqual(rows[0][0], dt.datetime(2026, 9, 1, 0, 30, tzinfo=dt.timezone.utc))
        self.assertEqual(rows[0][1], dt.date(2026, 9, 1))

    def test_duckdb_cannot_touch_files_or_change_its_configuration(self):
        import duckdb
        lake = self._lake()
        with tempfile.TemporaryDirectory() as d:
            target = Path(d) / "x.csv"
            for sql in (f"COPY (select 1) TO '{target.as_posix()}'", f"select * from read_csv('{target.as_posix()}')",
                        f"ATTACH '{(Path(d) / 'x.db').as_posix()}'", "INSTALL iceberg",
                        "SET enable_external_access = true", "SET TimeZone = 'Asia/Bangkok'"):
                with self.assertRaises(duckdb.Error, msg=sql):
                    lake.run(sql)
            self.assertFalse(target.exists())
        self.assertEqual(lake.run("select current_setting('TimeZone')")[1], [["UTC"]])
        self.assertTrue(lake.run("select current_setting('temp_directory')")[1][0][0].startswith(tempfile.gettempdir()))

    def test_describe(self):
        lake = self._lake(q.Bound(since=dt.date(2026, 10, 1)))
        cols, rows = lake.describe()
        by_name = {r[0]: r for r in rows}
        self.assertEqual(by_name["hypercore.l1_wallet_ledger"][1], 18)
        self.assertEqual(by_name["hypercore.l1_wallet_ledger"][2], "block_date,event_type")
        self.assertEqual(by_name["hypercore.l1_wallet_ledger"][4], "block_date >= '2026-10-01'")
        self.assertEqual(by_name["hypercore.token_whitelist"][4], "all rows")
        self.assertEqual(by_name["control.table_watermarks"][1], 1)

    def test_unknown_table_hint(self):
        lake = self._lake()
        self.assertEqual(lake.unknown("select * from l3_token_price_usd_1m"), ["l3_token_price_usd_1m"])
        err = io.StringIO()
        rc = q.execute(lake, "select * from l3_token_price_usd_1m", "table", 0, out=io.StringIO(), err=err)
        self.assertEqual(rc, 1)
        self.assertIn("not in the lake yet: l3_token_price_usd_1m", err.getvalue())

    def test_repl(self):
        lake = self._lake(q.Bound(since=dt.date(2026, 10, 1)))
        lines = [".tables", ".schema hypercore.l1_wallet_ledger", ".schema nope", ".rows 1",
                 "select event_type, count(*) n from l1_wallet_ledger", "group by 1 order by 1;",
                 ".load L4X_PERP_POSITION_SNAPSHOT", ".bogus", "select 'unterminated;", "x' as s; select 2 as two;",
                 ".quit", "select 'never runs';"]
        out, err = io.StringIO(), io.StringIO()
        with mock.patch("builtins.input", side_effect=lines + [EOFError()]), \
                redirect_stdout(out), redirect_stderr(err):
            rc = q.repl(lake, "table", 200)
        self.assertEqual(rc, 0)
        o, e = out.getvalue(), err.getvalue()
        self.assertIn("hypercore.l1_wallet_ledger", o)                         # .tables
        self.assertIn("block_timestamp", o)                                    # .schema (qualified name resolved)
        self.assertIn("unknown table nope", e)
        self.assertIn("showing 1 rows", e)
        self.assertIn("2 more not shown", o)                                    # .rows 1 applied to the 3-group result
        self.assertIn("loaded hypercore.l4x_perp_position_snapshot: 2 rows", e)
        self.assertIn("commands:", e)                                          # .bogus -> help
        self.assertIn("unterminated;\nx", o)                                   # a quote spanning lines, then 2nd stmt
        self.assertIn("two", o)
        self.assertNotIn("never runs", o)

    def test_main_end_to_end(self):
        with _env(self.root):
            out, err = io.StringIO(), io.StringIO()
            with redirect_stdout(out), redirect_stderr(err):
                rc = q.main(["--days", "0", "--format", "json",
                             "select event_type, count(*) n from l1_wallet_ledger group by 1 order by 1"])
            self.assertEqual(rc, 0, err.getvalue())
            self.assertEqual(out.getvalue().splitlines()[0], '{"event_type": "fee", "n": 6}')

            out, err = io.StringIO(), io.StringIO()
            with redirect_stdout(out), redirect_stderr(err):
                rc = q.main(["--list"])
            self.assertEqual(rc, 0, err.getvalue())
            self.assertIn("hypercore.l1_wallet_ledger", out.getvalue())
            self.assertIn("block_date,event_type", out.getvalue())

            out, err = io.StringIO(), io.StringIO()
            with redirect_stdout(out), redirect_stderr(err):
                rc = q.main(["select * from nope"])
            self.assertEqual(rc, 1)
            self.assertIn("error:", err.getvalue())

            # two statements, csv, an explicit lower bound that excludes the September rows
            out, err = io.StringIO(), io.StringIO()
            with redirect_stdout(out), redirect_stderr(err):
                rc = q.main(["--format", "csv", "--since", "2026-10-12",
                             "select count(*) n from l1_wallet_ledger; select count(*) n from l4x_perp_position_snapshot"])
            self.assertEqual(rc, 0, err.getvalue())
            self.assertEqual(out.getvalue().splitlines(), ["n", "12", "n", "2"])

            # --until alone bounds a window ending there (no error about a --since never given)
            out, err = io.StringIO(), io.StringIO()
            with redirect_stdout(out), redirect_stderr(err):
                rc = q.main(["--format", "csv", "--until", "2026-09-30", "--days", "60",
                             "select count(*) n from l1_wallet_ledger"])
            self.assertEqual((rc, out.getvalue().splitlines()), (0, ["n", "6"]), err.getvalue())

            # SQL from a file and from stdin
            sql_file = self.root / "q.sql"
            sql_file.write_text("/* report header; */\nselect count(*) n from token_whitelist;\n", encoding="utf-8")
            out, err = io.StringIO(), io.StringIO()
            with redirect_stdout(out), redirect_stderr(err):
                rc = q.main(["-f", str(sql_file), "--format", "csv"])
            self.assertEqual((rc, out.getvalue().splitlines()), (0, ["n", "1"]), err.getvalue())
            out, err = io.StringIO(), io.StringIO()
            with redirect_stdout(out), redirect_stderr(err), \
                    mock.patch.object(q.sys, "stdin", io.StringIO("select 1 as one;")):
                rc = q.main(["--format", "csv"])
            self.assertEqual((rc, out.getvalue().splitlines()), (0, ["one", "1"]))

            # argument conflicts are usage errors
            with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                q.main(["--list", "select 1"])
            with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                q.main(["-f", "x.sql", "select 1"])

    def test_main_interrupt_and_missing_duckdb_exit_cleanly(self):
        with _env(self.root):
            err = io.StringIO()
            with redirect_stdout(io.StringIO()), redirect_stderr(err), \
                    mock.patch.object(q.Lake, "run", side_effect=KeyboardInterrupt):
                rc = q.main(["select 1"])
            self.assertEqual(rc, 130)
            self.assertIn("interrupted", err.getvalue())
            self.assertNotIn("Traceback", err.getvalue())
            err = io.StringIO()
            with redirect_stdout(io.StringIO()), redirect_stderr(err), mock.patch.dict(sys.modules, {"duckdb": None}):
                rc = q.main(["--list"])
            self.assertEqual(rc, 2)
            self.assertIn("duckdb is not installed", err.getvalue())

    def test_main_without_a_catalog_exits_2_and_creates_nothing(self):
        with tempfile.TemporaryDirectory() as d, _env(Path(d)):
            err = io.StringIO()
            with redirect_stdout(io.StringIO()), redirect_stderr(err):
                rc = q.main(["--list"])
            self.assertEqual(rc, 2)
            self.assertIn("cannot open the Iceberg catalog", err.getvalue())
            self.assertFalse((Path(d) / "iceberg").exists())

    def test_main_unreadable_env_file_is_a_one_line_error(self):
        with tempfile.TemporaryDirectory() as d:
            env_file = Path(d) / "hypercore.env"
            env_file.write_text("HL_OUT_DIR=/nowhere\n", encoding="utf-8")
            err = io.StringIO()
            with _env(Path(d), HL_ENV_FILE=str(env_file)), redirect_stdout(io.StringIO()), redirect_stderr(err), \
                    mock.patch.object(Path, "read_text", side_effect=PermissionError(13, "Permission denied", str(env_file))):
                rc = q.main(["--list"])
            self.assertEqual(rc, 2)
            self.assertIn("cannot read the env file", err.getvalue())
            self.assertNotIn("Traceback", err.getvalue())


class PreflightDepsTests(unittest.TestCase):
    def test_missing_duckdb_is_a_warning_not_a_failure(self):
        with mock.patch.dict(sys.modules, {"duckdb": None}):
            check = preflight.check_imports()
        self.assertEqual(check.status, "WARN")
        self.assertIn("duckdb", check.detail)
        self.assertIn("hypercore-sql", check.detail)
        self.assertEqual(preflight.check_imports().status, "OK")


if __name__ == "__main__":
    unittest.main()
