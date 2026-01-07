#!/usr/bin/env python3
"""
xantaken_diff.py

Compare two Torn faction Xanax-taken snapshot CSVs and produce a per-user report.

Input CSVs are expected to look like:
    user_id,name,position,level,xantaken,export_date

Output includes:
- xantaken_a (from file1)
- xantaken_b (from file2)
- diff_xantaken = xantaken_b - xantaken_a
- avg_xantaken_per_day = (max(xantaken_a, xantaken_b) - min(...)) / days_between_exports

Usage:
  python xantaken_diff.py path/to/snap1.csv path/to/snap2.csv
  python xantaken_diff.py snap1.csv snap2.csv --output report.csv

Notes:
- days_between_exports is computed from export_date (preferred) or a YYYY-MM-DD in the filename.
- If days_between_exports cannot be determined (or is 0), avg_xantaken_per_day is left blank.
"""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Optional, Tuple

import numpy as np
import pandas as pd


DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")


def _parse_date_from_df_or_filename(df: pd.DataFrame, path: Path) -> Optional[date]:
    """Try export_date column first, then fall back to parsing YYYY-MM-DD from filename."""
    if "export_date" in df.columns:
        # Use the first non-null value; files are usually single-day exports.
        series = df["export_date"].dropna()
        if not series.empty:
            try:
                return pd.to_datetime(series.iloc[0]).date()
            except Exception:
                pass

    m = DATE_RE.search(path.name)
    if m:
        try:
            return date.fromisoformat(m.group(1))
        except Exception:
            pass

    return None


def _load_snapshot(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)

    required = {"user_id", "xantaken"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"{path} is missing required column(s): {', '.join(sorted(missing))}. "
            f"Found columns: {', '.join(df.columns)}"
        )

    # Normalize types
    df = df.copy()
    df["user_id"] = df["user_id"].astype(str)
    df["xantaken"] = pd.to_numeric(df["xantaken"], errors="coerce")

    return df


def build_report(path_a: Path, path_b: Path) -> Tuple[pd.DataFrame, Optional[date], Optional[date], Optional[int]]:
    df_a = _load_snapshot(path_a)
    df_b = _load_snapshot(path_b)

    date_a = _parse_date_from_df_or_filename(df_a, path_a)
    date_b = _parse_date_from_df_or_filename(df_b, path_b)

    days_between: Optional[int]
    start_date: Optional[date]
    end_date: Optional[date]

    if date_a and date_b:
        # Always treat "start" as the earlier date
        start_date, end_date = (date_a, date_b) if date_a <= date_b else (date_b, date_a)
        days_between = abs((date_b - date_a).days)
    else:
        start_date, end_date, days_between = date_a, date_b, None

    # Keep common metadata if present
    meta_cols = [c for c in ["name", "position", "level"] if (c in df_a.columns or c in df_b.columns)]

    keep_a = ["user_id", "xantaken"] + [c for c in meta_cols if c in df_a.columns]
    keep_b = ["user_id", "xantaken"] + [c for c in meta_cols if c in df_b.columns]

    a = df_a[keep_a].copy()
    b = df_b[keep_b].copy()

    a = a.rename(columns={"xantaken": "xantaken_a", **{c: f"{c}_a" for c in meta_cols if c in a.columns}})
    b = b.rename(columns={"xantaken": "xantaken_b", **{c: f"{c}_b" for c in meta_cols if c in b.columns}})

    merged = a.merge(b, on="user_id", how="outer")

    # Prefer metadata from file2 when available; fall back to file1
    for c in meta_cols:
        ca, cb = f"{c}_a", f"{c}_b"
        if cb in merged.columns and ca in merged.columns:
            merged[c] = merged[cb].combine_first(merged[ca])
        elif cb in merged.columns:
            merged[c] = merged[cb]
        elif ca in merged.columns:
            merged[c] = merged[ca]

    merged["diff_xantaken"] = merged["xantaken_b"] - merged["xantaken_a"]
    merged["xantaken_min"] = merged[["xantaken_a", "xantaken_b"]].min(axis=1)
    merged["xantaken_max"] = merged[["xantaken_a", "xantaken_b"]].max(axis=1)

    if days_between is None or days_between == 0:
        merged["avg_xantaken_per_day"] = np.nan
    else:
        merged["avg_xantaken_per_day"] = (merged["xantaken_max"] - merged["xantaken_min"]) / (days_between)
        # We subtract 1 from the given days since there is a 1 day delay in the API

    def _status(row: pd.Series) -> str:
        a_missing = pd.isna(row["xantaken_a"])
        b_missing = pd.isna(row["xantaken_b"])
        avg_xan_taken = row["avg_xantaken_per_day"]
        if a_missing and b_missing:
            return "Never Taken Xan!!!"
        if a_missing:
            return "New Recruit"
        if b_missing:
            return "Not in Faction and Time of Second Snapshot"
        if avg_xan_taken < 1:
            return "Fail"
        if avg_xan_taken >= 2:
            return "Exceeds"
        return "Pass"

    merged["status"] = merged.apply(_status, axis=1)
    #merged["start_date"] = str(start_date) if start_date else ""
    #merged["end_date"] = str(end_date) if end_date else ""
    #merged["days_between"] = days_between if days_between is not None else ""

    # Column order
    out_cols = (
        ["user_id"]
        + meta_cols
        + ["xantaken_a", "xantaken_b", "diff_xantaken", "avg_xantaken_per_day", "status"]#, "start_date", "end_date", "days_between"]
    )
    out_cols = [c for c in out_cols if c in merged.columns]

    report = merged[out_cols].sort_values(
        by=["position", "diff_xantaken", "user_id"],
        ascending=[True, False, True],
        na_position="last",
    )

    return report, start_date, end_date, days_between


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare two xantaken snapshot CSVs and write a diff report.")
    parser.add_argument("file1", type=Path, help="Path to the earlier (or first) snapshot CSV")
    parser.add_argument("file2", type=Path, help="Path to the later (or second) snapshot CSV")
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="Output CSV path (default: xantaken_diff_report_<start>_to_<end>.csv or xantaken_diff_report.csv)",
    )
    args = parser.parse_args()

    report, start_date, end_date, _days = build_report(args.file1, args.file2)

    if args.output is None:
        base = "xantaken_diff_report"
        if start_date and end_date:
            base += f"_{start_date}_to_{end_date}"
        args.output = Path.cwd() / f"{base}.csv"

    report.to_csv(args.output, index=False)
    print(f"Wrote report: {args.output} ({len(report)} rows)")


if __name__ == "__main__":
    main()
