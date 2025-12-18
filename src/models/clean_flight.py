from __future__ import annotations

from datetime import date
from typing import Optional

# Works with BOTH Pydantic v2 and v1
try:
    # Pydantic v2
    from pydantic import BaseModel, ConfigDict, field_validator
    PYDANTIC_V2 = True
except ImportError:
    # Pydantic v1
    from pydantic import BaseModel, validator
    PYDANTIC_V2 = False


class CleanFlight(BaseModel):
    flight_id: str
    fl_date: date
    carrier: str
    origin: str
    dest: str
    crs_dep_time: int
    dep_delay: Optional[float] = None
    arr_delay: Optional[float] = None
    cancelled: int
    diverted: int
    distance: Optional[float] = None

    if PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore")

        @field_validator("crs_dep_time")
        @classmethod
        def validate_crs_dep_time(cls, v: int) -> int:
            """
            Valid HHMM time in 24h format:
              - integer
              - 0..2359
              - minutes < 60
            """
            if not isinstance(v, int):
                raise ValueError("crs_dep_time must be an integer HHMM (e.g., 930, 1430)")
            if v < 0 or v > 2359:
                raise ValueError("crs_dep_time must be between 0 and 2359")
            if v % 100 >= 60:
                raise ValueError("crs_dep_time minutes must be < 60")
            return v

        @field_validator("carrier", "origin", "dest")
        @classmethod
        def normalize_codes(cls, v: str) -> str:
            if v is None:
                raise ValueError("code cannot be null")
            v = v.strip().upper()
            if len(v) == 0:
                raise ValueError("code cannot be empty")
            return v

    else:
        class Config:
            extra = "ignore"

        @validator("crs_dep_time")
        def validate_crs_dep_time(cls, v: int) -> int:
            if not isinstance(v, int):
                raise ValueError("crs_dep_time must be an integer HHMM (e.g., 930, 1430)")
            if v < 0 or v > 2359:
                raise ValueError("crs_dep_time must be between 0 and 2359")
            if v % 100 >= 60:
                raise ValueError("crs_dep_time minutes must be < 60")
            return v

        @validator("carrier", "origin", "dest", pre=True)
        def normalize_codes(cls, v: str) -> str:
            if v is None:
                raise ValueError("code cannot be null")
            v = str(v).strip().upper()
            if len(v) == 0:
                raise ValueError("code cannot be empty")
            return v
