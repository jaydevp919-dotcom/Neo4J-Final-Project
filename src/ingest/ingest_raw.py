import sys
import pandas as pd
from db.neo4j_conn import get_driver

# Columns that exist in YOUR data (from your header output)
USECOLS = [
    "FL_DATE",
    "OP_CARRIER",
    "OP_CARRIER_FL_NUM",
    "ORIGIN",
    "DEST",
    "CRS_DEP_TIME",
    "DEP_DELAY",
    "ARR_DELAY",
    "CANCELLED",
    "DIVERTED",
    "DISTANCE",
]

def main(csv_path: str, chunksize: int = 100_000) -> None:
    driver = get_driver()
    total = 0

    try:
        with driver.session() as session:
            for chunk in pd.read_csv(
                csv_path,
                usecols=USECOLS,
                chunksize=chunksize,
                low_memory=False,
            ):
                # --- Standardize / basic cleaning for RAW layer ---
                chunk["FL_DATE"] = pd.to_datetime(chunk["FL_DATE"], errors="coerce").dt.date

                chunk["OP_CARRIER"] = chunk["OP_CARRIER"].astype(str).str.strip().str.upper()
                chunk["ORIGIN"] = chunk["ORIGIN"].astype(str).str.strip().str.upper()
                chunk["DEST"] = chunk["DEST"].astype(str).str.strip().str.upper()

                # Drop missing required fields (RAW still needs minimally valid records)
                chunk = chunk.dropna(subset=["FL_DATE", "OP_CARRIER", "ORIGIN", "DEST", "CRS_DEP_TIME"])

                # Convert chunk to list of dicts for UNWIND
                rows = chunk.to_dict(orient="records")
                if not rows:
                    continue

                # Insert into Neo4j
                session.run(
                    """
                    UNWIND $rows AS r
                    CREATE (:RawFlight {
                      fl_date: r.FL_DATE,
                      carrier: r.OP_CARRIER,
                      flight_num: toInteger(r.OP_CARRIER_FL_NUM),
                      origin: r.ORIGIN,
                      dest: r.DEST,
                      crs_dep_time: toInteger(r.CRS_DEP_TIME),
                      dep_delay: r.DEP_DELAY,
                      arr_delay: r.ARR_DELAY,
                      cancelled: toInteger(r.CANCELLED),
                      diverted: toInteger(r.DIVERTED),
                      distance: r.DISTANCE
                    })
                    """,
                    rows=rows,
                )

                total += len(rows)
                print(f"Inserted chunk: {len(rows)} | Total inserted: {total}")

        print(f"\n✅ RAW ingestion complete. Total rows inserted: {total}")

    finally:
        driver.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python -m ingest.ingest_raw <path_to_csv>")
    main(sys.argv[1])
