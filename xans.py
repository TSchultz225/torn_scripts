#!/usr/bin/env python3
"""
Pull faction members (API v2) and each member's Xanax taken (xantaken),
then write results to a dated CSV.

Endpoints used:
- GET https://api.torn.com/v2/faction/{faction_id}/members
- GET https://api.torn.com/v2/user/{user_id}/personalstats?stat=xantaken
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import requests


BASE_URL = "https://api.torn.com/v2"


def torn_get(
    session: requests.Session,
    url: str,
    *,
    api_key: str,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 30,
    max_retries: int = 5,
) -> Dict[str, Any]:
    """
    GET wrapper with basic retry/backoff.
    Torn can return errors in-body (e.g. error.code == 5 for rate limit).
    """
    headers = {
        "Authorization": f"ApiKey xOZ42wvjt01rhnl9",
        "Accept": "application/json",
        "User-Agent": "faction-xantaken-export/1.0",
    }

    backoff = 2.0
    last_err: Optional[str] = None

    for attempt in range(1, max_retries + 1):
        try:
            r = session.get(url, headers=headers, params=params, timeout=timeout)
        except requests.RequestException as e:
            last_err = f"Request failed: {e}"
            time.sleep(backoff)
            backoff *= 1.7
            continue

        # HTTP-level rate limiting
        if r.status_code == 429:
            last_err = "HTTP 429 rate limited"
            time.sleep(backoff)
            backoff *= 1.7
            continue

        # Non-OK HTTP
        if not (200 <= r.status_code < 300):
            last_err = f"HTTP {r.status_code}: {r.text[:200]}"
            time.sleep(backoff)
            backoff *= 1.7
            continue

        try:
            data = r.json()
        except ValueError:
            last_err = f"Non-JSON response: {r.text[:200]}"
            time.sleep(backoff)
            backoff *= 1.7
            continue

        # Torn-style error in JSON body
        if isinstance(data, dict) and "error" in data:
            err = data.get("error") or {}
            code = err.get("code")
            msg = err.get("error") or err.get("message") or str(err)

            # Code 5 is "Too many requests" (100/min rolling limit)
            if code == 5:
                last_err = f"Torn error code 5 (rate limit): {msg}"
                time.sleep(backoff)
                backoff *= 1.7
                continue

            raise RuntimeError(f"Torn API error {code}: {msg}")

        return data

    raise RuntimeError(last_err or "Failed after retries")


def get_faction_members(session: requests.Session, api_key: str, faction_id: int) -> List[Dict[str, Any]]:
    url = f"{BASE_URL}/faction/{faction_id}/members"
    data = torn_get(session, url, api_key=api_key, params={"striptags": "true"})
    members = data.get("members")
    if not isinstance(members, list):
        raise RuntimeError(f"Unexpected members payload shape: {type(members)}")
    return members


def get_member_xantaken(session: requests.Session, api_key: str, user_id: int) -> Optional[int]:
    url = f"{BASE_URL}/user/{user_id}/personalstats"
    data = torn_get(session, url, api_key=api_key, params={"stat": "xantaken"})
    ps = data.get("personalstats", {})
    per_stats = ps.pop()
    val = per_stats.get("value")
    return int(val) if isinstance(val, (int, float)) else None


def main() -> int:
    parser = argparse.ArgumentParser(description="Export faction members + xantaken to dated CSV.")
    parser.add_argument("--key", default=os.getenv("TORN_API_KEY"), help="Torn API key (or set TORN_API_KEY env var)")
    parser.add_argument("--faction-id", type=int, default=22631, help="Faction ID (default: 22631)")
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.75,
        help="Seconds to sleep between member personalstats calls (default: 0.75)",
    )
    parser.add_argument(
        "--outdir",
        default=".",
        help="Output directory (default: current directory)",
    )
    args = parser.parse_args()

    if not args.key:
        print("ERROR: Missing API key. Pass --key or set TORN_API_KEY.", file=sys.stderr)
        return 2

    today = dt.date.today().isoformat()
    out_path = os.path.join(args.outdir, f"faction_{args.faction_id}_xantaken_{today}.csv")

    with requests.Session() as session:
        members = get_faction_members(session, args.key, args.faction_id)

        rows: List[Dict[str, Any]] = []
        for i, m in enumerate(members, start=1):
            user_id = int(m.get("id"))
            name = m.get("name")
            position = m.get("position")
            level = m.get("level")

            xantaken = None
            err = ""
            try:
                xantaken = get_member_xantaken(session, args.key, user_id)
            except Exception as e:
                err = str(e)

            rows.append(
                {
                    "user_id": user_id,
                    "name": name,
                    "position": position,
                    "level": level,
                    "xantaken": xantaken,
                    "export_date": today
                }
            )

            # Throttle (helps stay under the 100/min rolling limit)
            if args.sleep > 0:
                time.sleep(args.sleep)

    # Write CSV
    os.makedirs(args.outdir, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["user_id", "name", "position", "level", "xantaken", "export_date"],
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} members to: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
