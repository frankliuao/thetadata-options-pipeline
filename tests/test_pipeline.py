"""Offline checks for the options data pipeline. No ThetaData credentials."""

from __future__ import annotations

import datetime
import tempfile
import unittest
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl

from options_data.cli import main as download_main
from options_data.downloader import (
    DownloadConfig,
    load_merged_options_year,
    nyse_today,
    to_option_root,
    validate_symbol,
)
from options_data.gdrive_sync import get_gdrive_base_cmd
from options_data.schema import date_expr, merge_underlying_frames, normalize_options_df
from options_data.universe import load_universe_symbols
from options_data.updater import IncrementalOptionsUpdater, find_latest_downloaded_date


class SchemaTests(unittest.TestCase):
    def test_compact_and_integer_expiration_drive_dte(self) -> None:
        trade_date = datetime.date(2024, 11, 15)
        frame = pl.DataFrame(
            {
                "expiration": ["20241220", "2025-01-17"],
                "strike": [100.0, 110.0],
                "right": ["PUT", "CALL"],
                "bid": [1.0, 2.0],
                "ask": [1.4, 2.2],
                "theta": ["-0.215", "0.010"],
            }
        )
        out = normalize_options_df(frame, trade_date=trade_date)
        self.assertEqual(out["expiration"].to_list(), [datetime.date(2024, 12, 20), datetime.date(2025, 1, 17)])
        self.assertEqual(out["dte"].to_list(), [35, 63])
        self.assertEqual(out["right"].to_list(), ["put", "call"])
        self.assertAlmostEqual(out["mid"][0], 1.2)
        self.assertAlmostEqual(out["theta"][0], -0.215)

        yyyymmdd = pl.DataFrame(
            {
                "expiration": [20241220],
                "strike": [100.0],
                "right": ["put"],
                "bid": [1.0],
                "ask": [1.2],
            }
        )
        parsed = normalize_options_df(yyyymmdd, trade_date=trade_date)
        self.assertEqual(parsed["expiration"][0], datetime.date(2024, 12, 20))
        self.assertEqual(parsed["dte"][0], 35)

    def test_integer_calendar_date_is_not_an_epoch_day(self) -> None:
        frame = pl.DataFrame({"date": [20241225]})
        parsed = frame.with_columns(date_expr("date", frame.schema["date"]).alias("date"))
        self.assertEqual(parsed["date"][0], datetime.date(2024, 12, 25))

    def test_partial_frame_without_contract_keys_does_not_crash(self) -> None:
        out = normalize_options_df(
            pl.DataFrame({"bid": [1.0], "ask": [1.2]}),
            trade_date=datetime.date(2024, 1, 2),
        )
        self.assertAlmostEqual(out["mid"][0], 1.1)
        self.assertEqual(out["trade_date"][0], datetime.date(2024, 1, 2))

    def test_scaled_greeks_and_oi_alias(self) -> None:
        trade_date = datetime.date(2024, 11, 15)
        frame = pl.DataFrame(
            {
                "expiration": ["2024-12-20", "2024-12-20"],
                "strike": [100.0, 100.0],
                "right": ["C", "P"],
                "vega": [2.635, 1.50],
                "rho": [0.4387, -0.210],
                "open_interest": [500, 1200],
            }
        )
        out = normalize_options_df(frame, trade_date=trade_date)
        self.assertEqual(out["right"].to_list(), ["call", "put"])
        self.assertAlmostEqual(out["vega_per_pct"][0], 0.02635)
        self.assertAlmostEqual(out["rho_per_pct"][0], 0.004387)
        self.assertEqual(out["open_interest_prior_close"].to_list(), [500, 1200])


class MergeTests(unittest.TestCase):
    def test_year_merge_keeps_prior_days_and_open_interest(self) -> None:
        day1 = datetime.date(2024, 1, 2)
        day2 = datetime.date(2024, 1, 3)
        day3 = datetime.date(2024, 1, 4)
        expiration = datetime.date(2024, 2, 16)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            processed = root / "processed" / "options_eod" / "symbol=SPY"
            raw = root / "raw" / "options_eod" / "symbol=SPY" / "year=2024"
            processed.mkdir(parents=True)
            raw.mkdir(parents=True)
            pl.DataFrame(
                {
                    "trade_date": [day1, day2],
                    "expiration": [expiration, expiration],
                    "strike": [100.0, 100.0],
                    "right": ["put", "put"],
                    "bid": [1.0, 1.1],
                    "open_interest": [50, 60],
                }
            ).write_parquet(processed / "SPY_2024.parquet")
            pl.DataFrame(
                {
                    "trade_date": [day2, day3],
                    "expiration": [expiration, expiration],
                    "strike": [100.0, 100.0],
                    "right": ["put", "put"],
                    "bid": [1.4, 1.5],
                    "open_interest": [None, 70],
                }
            ).write_parquet(raw / "SPY_2024-01-04.parquet")

            merged = load_merged_options_year(root, "SPY", 2024)

        self.assertIsNotNone(merged)
        assert merged is not None
        self.assertEqual(merged["trade_date"].to_list(), [day1, day2, day3])
        day2_row = merged.filter(pl.col("trade_date") == day2)
        self.assertAlmostEqual(day2_row["bid"][0], 1.4)
        self.assertEqual(day2_row["open_interest"][0], 60)

    def test_underlying_merge_keeps_sessions_outside_the_new_window(self) -> None:
        first = datetime.datetime(2024, 1, 2, 17, 15, tzinfo=ZoneInfo("America/New_York"))
        second = datetime.datetime(2024, 1, 3, 17, 15, tzinfo=ZoneInfo("America/New_York"))
        existing = pl.DataFrame({"created": [first, second], "close": [10.0, 11.0]})
        fresh = pl.DataFrame({"created": [second], "close": [12.5]})
        merged = merge_underlying_frames(existing, fresh)
        self.assertEqual(merged.height, 2)
        self.assertAlmostEqual(merged.sort("created")["close"][0], 10.0)
        self.assertAlmostEqual(merged.sort("created")["close"][1], 12.5)


class SymbolAndCalendarTests(unittest.TestCase):
    def test_path_traversal_ticker_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            validate_symbol("../../../../../outside")
        with self.assertRaises(ValueError):
            validate_symbol("SPY/../X")
        self.assertEqual(validate_symbol("brk.b"), "BRK.B")

    def test_checked_in_universe_matches_documented_counts(self) -> None:
        current = load_universe_symbols(mode="current")
        removed = load_universe_symbols(mode="removed_active")
        active = load_universe_symbols(mode="active")
        everything = load_universe_symbols(mode="all", include_historical_tickers=True)
        self.assertEqual(len(current), 503)
        self.assertEqual(len(removed), 72)
        self.assertEqual(len(active), 575)
        self.assertEqual(len(everything), 580 + 51)
        self.assertIn("BRK.B", current)
        self.assertIn("BF.B", current)
        self.assertIn("GPS", everything)
        self.assertIn("GAP", everything)
        for symbol in everything:
            validate_symbol(symbol)

    def test_nyse_today_is_the_new_york_session_date(self) -> None:
        session = nyse_today()
        local = datetime.date.today()
        self.assertIsInstance(session, datetime.date)
        self.assertLessEqual(abs((session - local).days), 1)
        self.assertEqual(
            session,
            datetime.datetime.now(ZoneInfo("America/New_York")).date(),
        )

    def test_gdrive_command_uses_the_current_home(self) -> None:
        command, env = get_gdrive_base_cmd()
        self.assertIn(str(Path.home() / ".gemini" / "antigravity" / "bin"), env["PATH"])

    def test_to_option_root_converts_class_shares(self) -> None:
        self.assertEqual(to_option_root("BRK.B"), "BRKB")
        self.assertEqual(to_option_root("BF.B"), "BFB")
        self.assertEqual(to_option_root("AAPL"), "AAPL")

    def test_gdrive_sync_has_thread_lock(self) -> None:
        from options_data.gdrive_sync import GoogleDriveSync
        sync = GoogleDriveSync()
        self.assertTrue(hasattr(sync, "_lock"))
        with sync._lock:
            self.assertEqual(sync.root_folder_id, sync.root_folder_id)


class CatchUpTests(unittest.TestCase):
    def test_latest_date_considers_processed_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            raw = root / "raw" / "options_eod" / "symbol=SPY" / "year=2024"
            processed = root / "processed" / "options_eod" / "symbol=SPY"
            raw.mkdir(parents=True)
            processed.mkdir(parents=True)
            pl.DataFrame({"trade_date": [datetime.date(2024, 11, 15)]}).write_parquet(
                raw / "SPY_2024-11-15.parquet"
            )
            pl.DataFrame({"trade_date": [datetime.date(2024, 12, 20)]}).write_parquet(
                processed / "SPY_2024.parquet"
            )
            self.assertEqual(
                find_latest_downloaded_date(root, "SPY"),
                datetime.date(2024, 12, 20),
            )

    def test_one_failed_day_stops_that_symbol_and_continues_the_next(self) -> None:
        days = [datetime.date(2024, 1, 2), datetime.date(2024, 1, 3)]

        class FakeDownloader:
            def __init__(self) -> None:
                self.gdrive = None
                self.consolidated: list[tuple[str, int]] = []
                self.seen: list[tuple[str, datetime.date]] = []

            def get_trading_days(self, start: datetime.date, end: datetime.date) -> list[datetime.date]:
                return [day for day in days if start <= day <= end]

            def download_day(self, symbol: str, trade_date: datetime.date) -> tuple[datetime.date, int, str]:
                self.seen.append((symbol, trade_date))
                if trade_date == days[0]:
                    raise RuntimeError("transient failure")
                return (trade_date, 3, "saved")

            def consolidate_year(self, symbol: str, year: int) -> None:
                self.consolidated.append((symbol, year))

        fake = FakeDownloader()
        updater = IncrementalOptionsUpdater(
            config=DownloadConfig(
                symbols=["SPY", "AAPL"],
                start_date=days[0],
                end_date=days[-1],
                sync_to_gdrive=False,
                data_dir=Path("/tmp/options-review-unused"),
            ),
            downloader=fake,  # type: ignore[arg-type]
        )
        failed = updater.run_update(target_end_date=days[-1], forced_since=days[0])
        self.assertEqual(failed, 2)
        self.assertEqual(fake.seen, [("SPY", days[0]), ("AAPL", days[0])])
        self.assertEqual(fake.consolidated, [])

    def test_mid_range_failure_is_retried_on_the_next_run(self) -> None:
        days = [
            datetime.date(2024, 1, 2),
            datetime.date(2024, 1, 3),
            datetime.date(2024, 1, 4),
        ]

        class FakeDownloader:
            def __init__(self, root: Path) -> None:
                self.root = root
                self.gdrive = None
                self.seen: list[datetime.date] = []
                self.failures_left = 1

            def get_trading_days(self, start: datetime.date, end: datetime.date) -> list[datetime.date]:
                return [day for day in days if start <= day <= end]

            def download_day(self, symbol: str, trade_date: datetime.date) -> tuple[datetime.date, int, str]:
                self.seen.append(trade_date)
                if trade_date == days[1] and self.failures_left:
                    self.failures_left -= 1
                    raise RuntimeError("mid-range failure")
                out = (
                    self.root
                    / "raw"
                    / "options_eod"
                    / f"symbol={symbol}"
                    / f"year={trade_date.year}"
                    / f"{symbol}_{trade_date.isoformat()}.parquet"
                )
                out.parent.mkdir(parents=True, exist_ok=True)
                pl.DataFrame({"trade_date": [trade_date]}).write_parquet(out)
                return (trade_date, 1, "saved")

            def consolidate_year(self, symbol: str, year: int) -> None:
                raw = self.root / "raw" / "options_eod" / f"symbol={symbol}" / f"year={year}"
                frames = [pl.read_parquet(path) for path in sorted(raw.glob("*.parquet"))]
                if not frames:
                    return
                out = (
                    self.root
                    / "processed"
                    / "options_eod"
                    / f"symbol={symbol}"
                    / f"{symbol}_{year}.parquet"
                )
                out.parent.mkdir(parents=True, exist_ok=True)
                pl.concat(frames).write_parquet(out)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fake = FakeDownloader(root)
            updater = IncrementalOptionsUpdater(
                config=DownloadConfig(
                    symbols=["SPY"],
                    start_date=days[0],
                    end_date=days[-1],
                    sync_to_gdrive=False,
                    data_dir=root,
                    consolidate_yearly=True,
                ),
                downloader=fake,  # type: ignore[arg-type]
            )
            failed = updater.run_update(target_end_date=days[-1], forced_since=days[0])
            self.assertEqual(failed, 1)
            self.assertEqual(fake.seen, [days[0], days[1]])
            self.assertEqual(find_latest_downloaded_date(root, "SPY"), days[0])
            self.assertFalse(
                (
                    root
                    / "raw"
                    / "options_eod"
                    / "symbol=SPY"
                    / "year=2024"
                    / f"SPY_{days[2].isoformat()}.parquet"
                ).exists()
            )

            fake.seen.clear()
            failed_again = updater.run_update(target_end_date=days[-1], forced_since=None)
            self.assertEqual(failed_again, 0)
            self.assertEqual(fake.seen, [days[1], days[2]])
            self.assertEqual(find_latest_downloaded_date(root, "SPY"), days[2])

    def test_multithreaded_updater_runs_concurrently(self) -> None:
        days = [datetime.date(2024, 1, 2), datetime.date(2024, 1, 3)]

        class FakeDownloader:
            def __init__(self) -> None:
                self.gdrive = None
                self.seen: list[tuple[str, datetime.date]] = []

            def get_trading_days(self, start: datetime.date, end: datetime.date) -> list[datetime.date]:
                return [day for day in days if start <= day <= end]

            def download_day(self, symbol: str, trade_date: datetime.date) -> tuple[datetime.date, int, str]:
                self.seen.append((symbol, trade_date))
                return (trade_date, 10, "saved")

            def consolidate_year(self, symbol: str, year: int) -> None:
                pass

        fake = FakeDownloader()
        updater = IncrementalOptionsUpdater(
            config=DownloadConfig(
                symbols=["SPY", "AAPL"],
                start_date=days[0],
                end_date=days[-1],
                sync_to_gdrive=False,
                max_workers=2,
                consolidate_yearly=False,
            ),
            downloader=fake,  # type: ignore[arg-type]
        )
        failed = updater.run_update(target_end_date=days[-1], forced_since=days[0])
        self.assertEqual(failed, 0)
        self.assertEqual(len(fake.seen), 4)
        symbols_seen = {s for s, _ in fake.seen}
        self.assertEqual(symbols_seen, {"SPY", "AAPL"})


class MockCliTests(unittest.TestCase):
    def test_mock_pipeline_exits_zero(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(download_main(["--mock-test", "--data-dir", tmp]), 0)


if __name__ == "__main__":
    unittest.main()
