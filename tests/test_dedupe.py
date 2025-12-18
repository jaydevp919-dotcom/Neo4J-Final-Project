from src.clean.clean_load import build_flight_id

def test_build_flight_id() -> None:
    fid = build_flight_id("2020-01-01", "AA", "EWR", "JFK", 5)
    assert fid.endswith("-0005")
