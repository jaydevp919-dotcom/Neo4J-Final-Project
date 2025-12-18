from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional

from neo4j import Driver, GraphDatabase
from neo4j.work.simple import Session  # type: ignore[attr-defined]


def env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v and v.strip() else default


NEO4J_URI: str = env("NEO4J_URI", "bolt://127.0.0.1:7687")  # force IPv4 default
NEO4J_USER: str = env("NEO4J_USER", "neo4j")
NEO4J_PASSWORD: str = env("NEO4J_PASSWORD", "neo4jpassword")
NEO4J_DATABASE: str = env("NEO4J_DATABASE", "neo4j")


def get_driver() -> Driver:
    return GraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USER, NEO4J_PASSWORD),
    )


def wait_for_neo4j(driver: Driver, attempts: int = 15, sleep_s: float = 2.0) -> None:
    last_err: Optional[Exception] = None
    for _ in range(attempts):
        try:
            driver.verify_connectivity()
            print("✅ Neo4j connectivity OK")
            return
        except Exception as e:
            last_err = e
            time.sleep(sleep_s)

    # mypy-safe: ensure last_err is not None before raising "from"
    if last_err is None:
        raise RuntimeError("Neo4j not reachable after retries (no exception captured).")
    raise RuntimeError(f"Neo4j not reachable after retries. Last error: {last_err}") from last_err


def run_cypher(session: Session, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    result = session.run(query, params or {})
    return [r.data() for r in result]


def main() -> None:
    driver = get_driver()

    try:
        wait_for_neo4j(driver)

        with driver.session(database=NEO4J_DATABASE) as session:
            # ---------- Verify source data ----------
            cnt = run_cypher(session, "MATCH (f:CleanFlight) RETURN count(f) AS n;")
            clean_n = int(cnt[0]["n"]) if cnt else 0
            print(f"✅ CleanFlight rows: {clean_n}")

            # ---------- Create constraints ----------
            session.run(
                """
                CREATE CONSTRAINT route_summary_unique IF NOT EXISTS
                FOR (r:RouteSummary)
                REQUIRE (r.origin, r.dest) IS UNIQUE;
                """
            ).consume()
            print("✅ Constraint ensured: (RouteSummary.origin, RouteSummary.dest) unique")

            session.run(
                """
                CREATE CONSTRAINT daily_carrier_unique IF NOT EXISTS
                FOR (d:DailyCarrierSummary)
                REQUIRE (d.carrier, d.date) IS UNIQUE;
                """
            ).consume()
            print("✅ Constraint ensured: (DailyCarrierSummary.carrier, DailyCarrierSummary.date) unique")

            session.run(
                """
                CREATE CONSTRAINT monthly_airport_unique IF NOT EXISTS
                FOR (m:MonthlyAirportSummary)
                REQUIRE (m.airport, m.month) IS UNIQUE;
                """
            ).consume()
            print("✅ Constraint ensured: (MonthlyAirportSummary.airport, MonthlyAirportSummary.month) unique")

            # ---------- Gold aggregates ----------

            # RouteSummary
            route_res = run_cypher(
                session,
                """
                MATCH (f:Flight)
                WHERE f.origin IS NOT NULL AND f.dest IS NOT NULL
                WITH f.origin AS o, f.dest AS d,
                     count(*) AS flights,
                     avg(coalesce(f.arr_delay, 0.0)) AS avg_arr_delay,
                     avg(coalesce(f.dep_delay, 0.0)) AS avg_dep_delay
                MERGE (r:RouteSummary {origin:o, dest:d})
                SET r.flights = flights,
                    r.avg_arr_delay = avg_arr_delay,
                    r.avg_dep_delay = avg_dep_delay
                RETURN count(r) AS created_or_updated;
                """,
            )
            route_n = int(route_res[0]["created_or_updated"]) if route_res else 0
            print(f"✅ Gold build: RouteSummary created/updated = {route_n}")

            # DailyCarrierSummary
            daily_res = run_cypher(
                session,
                """
                MATCH (f:Flight)
                WHERE f.op_carrier IS NOT NULL AND f.fl_date IS NOT NULL
                WITH f.op_carrier AS carrier,
                     date(f.fl_date) AS dt,
                     avg(coalesce(f.dep_delay, 0.0)) AS avg_dep_delay,
                     count(*) AS flights
                MERGE (d:DailyCarrierSummary {carrier: carrier, date: dt})
                SET d.avg_dep_delay = avg_dep_delay,
                    d.flights = flights
                RETURN count(d) AS created_or_updated;
                """,
            )
            daily_n = int(daily_res[0]["created_or_updated"]) if daily_res else 0
            print(f"✅ Gold build: DailyCarrierSummary created/updated = {daily_n}")

            # MonthlyAirportSummary
            monthly_res = run_cypher(
                session,
                """
                MATCH (f:Flight)
                WHERE f.origin IS NOT NULL AND f.fl_date IS NOT NULL
                WITH f.origin AS airport,
                     substring(toString(date(f.fl_date)), 0, 7) AS month,
                     avg(coalesce(f.dep_delay, 0.0)) AS avg_dep_delay,
                     avg(CASE WHEN coalesce(f.cancelled, 0) = 1 THEN 1.0 ELSE 0.0 END) AS cancel_rate,
                     count(*) AS flights
                MERGE (m:MonthlyAirportSummary {airport: airport, month: month})
                SET m.avg_dep_delay = avg_dep_delay,
                    m.cancel_rate = cancel_rate,
                    m.flights = flights
                RETURN count(m) AS created_or_updated;
                """,
            )
            monthly_n = int(monthly_res[0]["created_or_updated"]) if monthly_res else 0
            print(f"✅ Gold build: MonthlyAirportSummary created/updated = {monthly_n}")

            print("✅ Gold layer build complete.")

    finally:
        driver.close()


if __name__ == "__main__":
    main()
