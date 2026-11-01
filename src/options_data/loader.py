"""High-performance Polars LazyFrame loader for backtesting on downloaded Parquet options data."""

from __future__ import annotations

import datetime
from pathlib import Path
from typing import Literal

import polars as pl


class OptionsDataLoader:
    """Lazy Parquet scanner for options strategy backtesting."""

    def __init__(self, data_dir: str | Path = "data") -> None:
        self.data_dir = Path(data_dir)

    def scan_symbol(
        self,
        symbol: str,
        *,
        use_consolidated: bool = True,
    ) -> pl.LazyFrame:
        """Return a Polars LazyFrame scanning all Parquet files for `symbol`."""
        sym = symbol.upper()
        processed_dir = self.data_dir / "processed" / "options_eod" / f"symbol={sym}"
        raw_dir = self.data_dir / "raw" / "options_eod" / f"symbol={sym}"

        processed_files: list[Path] = []
        if use_consolidated and processed_dir.exists():
            processed_files = sorted(processed_dir.glob("*.parquet"))

        raw_files: list[Path] = []
        if raw_dir.exists():
            raw_files = sorted(raw_dir.glob("year=*/*.parquet"))

        if not processed_files and not raw_files:
            raise FileNotFoundError(
                f"No Parquet files found for symbol '{sym}' in {processed_dir} or {raw_dir}."
            )

        if not processed_files:
            return pl.scan_parquet([str(p) for p in raw_files])

        if not raw_files:
            return pl.scan_parquet([str(p) for p in processed_files])

        # Both processed and raw files exist.
        # Find raw files not yet covered by consolidated yearly files.
        consolidated_max_dates: dict[int, datetime.date] = {}
        for pf in processed_files:
            try:
                max_d = (
                    pl.scan_parquet(str(pf))
                    .select(pl.col("trade_date").max())
                    .collect()
                    .item()
                )
                if isinstance(max_d, datetime.date):
                    consolidated_max_dates[max_d.year] = max_d
            except Exception:
                continue

        unconsolidated_raw: list[Path] = []
        for rf in raw_files:
            try:
                date_str = rf.stem.replace(f"{sym}_", "")
                rf_date = datetime.date.fromisoformat(date_str)
                rf_year = rf_date.year
                if rf_year not in consolidated_max_dates:
                    unconsolidated_raw.append(rf)
                elif rf_date > consolidated_max_dates[rf_year]:
                    unconsolidated_raw.append(rf)
            except Exception:
                unconsolidated_raw.append(rf)

        sources = [pl.scan_parquet([str(p) for p in processed_files])]
        if unconsolidated_raw:
            sources.append(pl.scan_parquet([str(p) for p in unconsolidated_raw]))

        if len(sources) == 1:
            return sources[0]
        return pl.concat(sources, how="vertical_relaxed")

    def query_chain(
        self,
        symbol: str,
        *,
        start_date: datetime.date | str | None = None,
        end_date: datetime.date | str | None = None,
        right: Literal["call", "put", "both"] = "both",
        min_dte: int | None = None,
        max_dte: int | None = None,
        min_abs_delta: float | None = None,
        max_abs_delta: float | None = None,
        min_bid: float | None = None,
    ) -> pl.DataFrame:
        """Query historical option chains with predicate pushdown into Parquet."""
        lf = self.scan_symbol(symbol)

        if isinstance(start_date, str):
            start_date = datetime.date.fromisoformat(start_date)
        if isinstance(end_date, str):
            end_date = datetime.date.fromisoformat(end_date)

        if start_date is not None:
            lf = lf.filter(pl.col("trade_date") >= start_date)
        if end_date is not None:
            lf = lf.filter(pl.col("trade_date") <= end_date)
        if right in ("call", "put"):
            lf = lf.filter(pl.col("right") == right)
        if min_dte is not None:
            lf = lf.filter(pl.col("dte") >= min_dte)
        if max_dte is not None:
            lf = lf.filter(pl.col("dte") <= max_dte)
        if min_abs_delta is not None:
            lf = lf.filter(pl.col("delta").abs() >= min_abs_delta)
        if max_abs_delta is not None:
            lf = lf.filter(pl.col("delta").abs() <= max_abs_delta)
        if min_bid is not None:
            lf = lf.filter(pl.col("bid") >= min_bid)

        return lf.collect()
