from datetime import date
import pytest
from src.models.clean_flight import CleanFlight

def test_clean_flight_valid() -> None:
    f = CleanFlight(
        flight_id="2020-01-01-AA-EWR-JFK-0900",
        fl_date=date(2020, 1, 1),
        carrier="AA",
        origin="EWR",
        dest="JFK",
        crs_dep_time=900,
        dep_delay=5.0,
        arr_delay=3.0,
        cancelled=0,
        diverted=0,
        distance=20.0,
    )
    assert f.origin == "EWR"

def test_clean_flight_invalid_time() -> None:
    with pytest.raises(Exception):
        CleanFlight(
            flight_id="x",
            fl_date=date(2020, 1, 1),
            carrier="AA",
            origin="EWR",
            dest="JFK",
            crs_dep_time=9999,  # invalid
            dep_delay=None,
            arr_delay=None,
            cancelled=0,
            diverted=0,
            distance=None,
        )
