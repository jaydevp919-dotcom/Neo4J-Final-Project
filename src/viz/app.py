from __future__ import annotations

import os
import time
from typing import Any, Optional

import pandas as pd
import streamlit as st
from neo4j import Driver, GraphDatabase
from neo4j.exceptions import ServiceUnavailable

# --- Connection (env vars first, else defaults) ---
NEO4J_URI: str = os.getenv("NEO4J_URI", "neo4j://127.0.0.1:7687")
NEO4J_USER: str = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD: str = os.getenv("NEO4J_PASSWORD", "neo4jpassword")
NEO4J_DB: str = os.getenv("NEO4J_DATABASE", "neo4j")

st.set_page_config(page_title="Neo4j Airline Gold Dashboard", layout="wide")
st.title("Neo4j Airline Dashboard (Gold Layer)")


@st.cache_resource
def get_driver() -> Driver:
    return GraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USER, NEO4J_PASSWORD),
        encrypted=False,
        connection_timeout=10,
        max_connection_lifetime=300,
    )


def run_query(
    q: str,
    params: Optional[dict[str, Any]] = None,
    retries: int = 3,
    sleep_s: float = 1.0,
) -> pd.DataFrame:
    driver: Driver = get_driver()

    last_err: Optional[Exception] = None
    for _ in range(retries):
        try:
            with driver.session(database=NEO4J_DB) as session:
                safe_params: Optional[dict[str, Any]] = dict(params) if params is not None else None
                res = session.run(q, safe_params)
                return pd.DataFrame([r.data() for r in res])
        except ServiceUnavailable as e:
            last_err = e
            time.sleep(sleep_s)

    raise RuntimeError("Neo4j query failed after retries") from last_err


# --- Quick connection test ---
with st.expander("Connection status"):
    try:
        run_query("RETURN 1 AS ok;")
        st.success(f"Connected ✅  URI={NEO4J_URI}  DB={NEO4J_DB}")
    except Exception as e:
        st.error("Neo4j connection failed.")
        st.code(str(e))
        st.stop()

# --- Debug keys ---
with st.expander("Debug: Show node property keys (Gold layer)"):
    try:
        k1 = run_query("MATCH (r:RouteSummary) RETURN keys(r) AS keys LIMIT 1;")
        k2 = run_query("MATCH (s:DailyCarrierSummary) RETURN keys(s) AS keys LIMIT 1;")
        k3 = run_query("MATCH (m:MonthlyAirportSummary) RETURN keys(m) AS keys LIMIT 1;")
        st.write("RouteSummary keys:", k1.to_dict("records"))
        st.write("DailyCarrierSummary keys:", k2.to_dict("records"))
        st.write("MonthlyAirportSummary keys:", k3.to_dict("records"))
    except Exception as e:
        st.error(f"Debug query failed: {e}")

col1, col2 = st.columns(2)

with col1:
    st.subheader("1) Daily Carrier Avg Departure Delay (Line)")
    carrier = st.selectbox(
        "Carrier code",
        options=["AA", "UA", "DL", "WN", "B6", "AS", "NK", "F9", "HA", "OO"],
        index=1,
    )

    df = run_query(
        """
        MATCH (s:DailyCarrierSummary {carrier: $carrier})
        RETURN toString(s.date) AS date,
               s.flights AS flights,
               s.avg_dep_delay AS avg_dep_delay
        ORDER BY date
        LIMIT 365
        """,
        {"carrier": carrier},
    )

    if not df.empty:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"])
        st.line_chart(df.set_index("date")[["avg_dep_delay"]])
    else:
        st.warning("No data found for that carrier.")

with col2:
    st.subheader("2) Top 10 Routes by Avg Arrival Delay (Bar)")
    min_flights = st.slider("Minimum flights for route", 1, 500, 50, step=10)

    df2 = run_query(
        """
        MATCH (r:RouteSummary)
        WHERE r.flights >= $min_flights
        RETURN r.origin AS origin, r.dest AS dest,
               r.avg_arr_delay AS avg_arr_delay,
               r.flights AS flights
        ORDER BY avg_arr_delay DESC
        LIMIT 10
        """,
        {"min_flights": int(min_flights)},
    )

    if not df2.empty:
        df2["route"] = df2["origin"] + "→" + df2["dest"]
        st.bar_chart(df2.set_index("route")[["avg_arr_delay"]])
    else:
        st.warning("No route data found (try lowering minimum flights).")

st.subheader("3) Monthly Airport Summary (Table)")
airport = st.selectbox(
    "Airport code",
    options=["EWR", "JFK", "LGA", "PHL", "BOS", "ORD", "ATL", "LAX", "SFO", "DFW", "DEN", "SEA", "MIA", "ANC"],
    index=0,
)

df3 = run_query(
    """
    MATCH (m:MonthlyAirportSummary {airport: $airport})
    RETURN m.month AS month,
           m.flights AS flights,
           m.avg_dep_delay AS avg_dep_delay,
           m.cancel_rate AS cancel_rate
    ORDER BY month
    """,
    {"airport": airport},
)

st.dataframe(df3, use_container_width=True, height=260)
if df3.empty:
    st.info("No monthly rows for this airport. Try another airport.")
