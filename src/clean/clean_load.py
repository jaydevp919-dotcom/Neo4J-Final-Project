from __future__ import annotations

import os
import sys
from datetime import date
from typing import Any, Dict, Mapping, Optional, Union, Iterable

import pandas as pd
from neo4j import GraphDatabase


def build_flight_id(
    fl_date: Union[str, date, Mapping[str, Any]],
    carrier: Optional[str] = None,
    origin: Optional[str] = None,
    dest: Optional[str] = None,
    crs_dep_time: Optional[int] = None,
) -> str:
    """
    Supports BOTH call styles:

    1) build_flight_id(row_dict)
    2) build_flight_id("2020-01-01", "AA", "EWR", "JFK", 5)

    Output format:
    YYYY-MM-DD|CARRIER|ORIGIN|DEST-XXXX
    """

    if isinstance(fl_date, Mapping):
        row = fl_date
        fl_date_val = row.get("fl_date") or row.get("FL_DATE") or row.get("date")
        carrier_val = row.get("carrier") or row.get("op_carrier") or row.get("OP_CARRIER")
        origin_val = row.get("origin") or row.get("ORIGIN")
        dest_val = row.get("dest") or row.get("DEST")
        crs_val = row.get("crs_dep_time") or row.get("CRS_DEP_TIME")
    else:
        fl_date_val = fl_date
        carrier_val = carrier
        origin_val = origin
        dest_val = dest
        crs_val = crs_dep_time

    if fl_date_val is None or carrier_val is None or origin_val is None or dest_val is None or crs_val is None:
        raise ValueError("Missing fields for build_flight_id")

    fl_date_str = fl_date_val.isoformat() if isinstance(fl_date_val, date) else str(fl_date_val).strip()

    carrier_str = str(carrier_val).strip().upper()
    origin_str = str(origin_val).strip().upper()
    dest_str = str(dest_val).strip().upper()
    crs_int = int(float(crs_val))

    return f"{fl_date_str}|{carrier_str}|{origin_str}|{dest_str}-{crs_int:04d}"


def norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df


def pick(row: Mapping[str, Any], *keys: str) -> Optional[Any]:
    for k in keys:
        if k in row and pd.notna(row[k]) and str(row[k]).strip() != "":
            return row[k]
    return None


def to_int(x: Any) -> Optional[int]:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    try:
        return int(float(x))
    except Exception:
        return None


def to_float(x: Any) -> Optional[float]:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    try:
        return float(x)
    except Exception:
        return None


def main(csv_path: str) -> None:
    uri: str = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user: str = os.getenv("NEO4J_USER", "neo4j")
    password: str = os.getenv("NEO4J_PASSWORD", "neo4jpassword")
    database: str = os.getenv("NEO4J_DATABASE", "neo4j")

    driver = GraphDatabase.driver(uri, auth=(user, password))

    with driver.session(database=database) as s:
        s.run(
            "CREATE CONSTRAINT cleanflight_fid IF NOT EXISTS "
            "FOR (f:CleanFlight) REQUIRE f.flight_id IS UNIQUE"
        )

    chunk_size: int = int(os.getenv("CLEAN_CHUNK", "100000"))
    batch_size: int = int(os.getenv("CLEAN_BATCH", "2000"))

    total_inserted: int = 0

    cypher: str = """
    UNWIND $rows AS row
    MERGE (f:CleanFlight {flight_id: row.flight_id})
    SET f += row
    SET f:Flight
    """

    with driver.session(database=database) as session:
        for chunk in pd.read_csv(csv_path, chunksize=chunk_size, low_memory=False):
            chunk = norm_cols(chunk)

            rows: list[Dict[str, Any]] = []

            for _, r in chunk.iterrows():
                rec: Dict[str, Any] = r.to_dict()

                fl_date = pick(rec, "fl_date", "flight_date", "flightdate")
                op_carrier = pick(rec, "op_carrier", "carrier", "unique_carrier")
                op_carrier_fl_num = pick(rec, "op_carrier_fl_num", "flight_num", "fl_num")
                origin = pick(rec, "origin", "origin_airport", "orig")
                dest = pick(rec, "dest", "destination", "dest_airport")

                if origin is None or dest is None:
                    continue

                row: Dict[str, Any] = {
                    "flight_id": f"{fl_date}_{op_carrier}_{op_carrier_fl_num}_{origin}_{dest}",
                    "fl_date": str(fl_date),
                    "op_carrier": str(op_carrier),
                    "op_carrier_fl_num": str(op_carrier_fl_num),
                    "origin": str(origin).strip(),
                    "dest": str(dest).strip(),
                    "crs_dep_time": to_int(pick(rec, "crs_dep_time")),
                    "dep_time": to_int(pick(rec, "dep_time")),
                    "dep_delay": to_float(pick(rec, "dep_delay")),
                    "arr_delay": to_float(pick(rec, "arr_delay")),
                    "cancelled": to_int(pick(rec, "cancelled")),
                    "diverted": to_int(pick(rec, "diverted")),
                }

                rows.append(row)

                if len(rows) >= batch_size:
                    session.run(cypher, rows=rows).consume()
                    total_inserted += len(rows)
                    rows = []

            if rows:
                session.run(cypher, rows=rows).consume()
                total_inserted += len(rows)

    driver.close()
    print(f"✅ Clean load completed. Total rows inserted: {total_inserted}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python -m src.clean.clean_load <path_to_csv>")
    main(sys.argv[1])
