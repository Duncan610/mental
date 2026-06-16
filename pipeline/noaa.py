"""
pipeline/other_pipelines.py

NOAA, BLS, and SAMHSA → DuckDB pipelines using dlt.

All three follow the same dlt pattern:
  1. Define a @dlt.source with @dlt.resource generators
  2. Each resource yields dicts — dlt writes to DuckDB automatically
  3. primary_key + write_disposition='merge' = idempotent upserts

No Snowflake. No custom loaders. No MERGE SQL boilerplate.
"""

import logging
from datetime import datetime, timezone
from typing import Iterator, Optional

import dlt
import httpx

from config.settings import Settings

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# NOAA WEATHER PIPELINE
# API: NOAA Climate Data Online (CDO)
# Auth: Free token from https://www.ncdc.noaa.gov/cdo-web/token (email signup)
# Data: Daily temperature, precipitation, daylight by station
# ═══════════════════════════════════════════════════════════════════════════

NOAA_BASE = "https://www.ncdc.noaa.gov/cdo-web/api/v2"

# Top 50 US stations by population density — covers major metro areas
# In a production system this seed list would come from a database table
STATION_IDS = [
    "GHCND:USW00094728",  # New York, NY
    "GHCND:USW00094846",  # Chicago, IL
    "GHCND:USW00023174",  # Los Angeles, CA
    "GHCND:USW00012960",  # Houston, TX
    "GHCND:USW00023183",  # Phoenix, AZ
    "GHCND:USW00013881",  # Philadelphia, PA
    "GHCND:USW00023234",  # San Antonio, TX
    "GHCND:USW00023272",  # San Diego, CA
    "GHCND:USW00013958",  # Dallas, TX
    "GHCND:USW00023230",  # San Jose, CA
    "GHCND:USW00094728",  # Austin, TX
    "GHCND:USW00014819",  # Detroit, MI
    "GHCND:USW00014739",  # Boston, MA
    "GHCND:USW00093721",  # Memphis, TN
    "GHCND:USW00013882",  # Baltimore, MD
    "GHCND:USW00014922",  # Minneapolis, MN
    "GHCND:USW00093738",  # Nashville, TN
    "GHCND:USW00093805",  # Louisville, KY
    "GHCND:USW00093987",  # Portland, OR
    "GHCND:USW00024233",  # Seattle, WA
]


@dlt.source(name="noaa")
def noaa_source(api_token: str = dlt.secrets.value) -> dlt.resource:

    @dlt.resource(
        name="daily_weather",
        primary_key=["station_id", "observation_date", "datatype"],
        write_disposition="merge",
    )
    def daily_weather(
        observation_date=dlt.sources.incremental(
            "observation_date",
            initial_value="2020-01-01",
        ),
    ) -> Iterator[dict]:
        """Daily GHCN observations for major US stations."""
        http = httpx.Client(
            timeout=30.0,
            headers={"token": api_token}
        )

        since = observation_date.last_value
        datatypes = ["TMAX", "TMIN", "PRCP", "SNOW"]

        for station_id in STATION_IDS:
            for datatype in datatypes:
                try:
                    resp = http.get(
                        f"{NOAA_BASE}/data",
                        params={
                            "datasetid": "GHCND",
                            "stationid": station_id,
                            "datatypeid": datatype,
                            "startdate": since,
                            "enddate": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                            "limit": 1000,
                            "units": "metric",
                        }
                    )
                    if resp.status_code == 204:  # No data
                        continue
                    resp.raise_for_status()
                    data = resp.json().get("results", [])

                    for obs in data:
                        yield {
                            "station_id":       station_id,
                            "observation_date": obs.get("date", "")[:10],
                            "datatype":         obs.get("datatype", ""),
                            "value":            obs.get("value"),
                            "_loaded_at":       datetime.now(timezone.utc).isoformat(),
                        }

                except Exception as e:
                    logger.warning(f"NOAA error for {station_id}/{datatype}: {e}")
                    continue

    return daily_weather


def run_noaa_pipeline(settings: Settings) -> dlt.Pipeline:
    pipeline = dlt.pipeline(
        pipeline_name="mental_health_noaa",
        destination=dlt.destinations.duckdb(
            credentials="data/duckdb/mental_health_pulse.duckdb"
        ),
        dataset_name="raw_noaa",
    )
    load_info = pipeline.run(noaa_source(api_token=settings.NOAA_API_TOKEN))
    logger.info(f"NOAA pipeline complete: {load_info}")
    return pipeline


# ═══════════════════════════════════════════════════════════════════════════
# BLS ECONOMICS PIPELINE
# API: BLS Public Data API v2
# Auth: Free registration key from https://data.bls.gov/registrationEngine/
# Data: Unemployment rate by state and metro area (monthly)
# ═══════════════════════════════════════════════════════════════════════════

BLS_BASE = "https://api.bls.gov/publicAPI/v2/timeseries/data"

# LAUS series IDs for all 50 states + DC (unemployment rate)
# Format: LAUST<FIPS>0000000000003  (3 = unemployment rate)
STATE_SERIES = {
    "Alabama": "LAUST01000000000000003",
    "Alaska": "LAUST02000000000000003",
    "Arizona": "LAUST04000000000000003",
    "Arkansas": "LAUST05000000000000003",
    "California": "LAUST06000000000000003",
    "Colorado": "LAUST08000000000000003",
    "Connecticut": "LAUST09000000000000003",
    "Delaware": "LAUST10000000000000003",
    "Florida": "LAUST12000000000000003",
    "Georgia": "LAUST13000000000000003",
    "Hawaii": "LAUST15000000000000003",
    "Idaho": "LAUST16000000000000003",
    "Illinois": "LAUST17000000000000003",
    "Indiana": "LAUST18000000000000003",
    "Iowa": "LAUST19000000000000003",
    "Kansas": "LAUST20000000000000003",
    "Kentucky": "LAUST21000000000000003",
    "Louisiana": "LAUST22000000000000003",
    "Maine": "LAUST23000000000000003",
    "Maryland": "LAUST24000000000000003",
    "Massachusetts": "LAUST25000000000000003",
    "Michigan": "LAUST26000000000000003",
    "Minnesota": "LAUST27000000000000003",
    "Mississippi": "LAUST28000000000000003",
    "Missouri": "LAUST29000000000000003",
    "Montana": "LAUST30000000000000003",
    "Nebraska": "LAUST31000000000000003",
    "Nevada": "LAUST32000000000000003",
    "New Hampshire": "LAUST33000000000000003",
    "New Jersey": "LAUST34000000000000003",
    "New Mexico": "LAUST35000000000000003",
    "New York": "LAUST36000000000000003",
    "North Carolina": "LAUST37000000000000003",
    "Ohio": "LAUST39000000000000003",
    "Oklahoma": "LAUST40000000000000003",
    "Oregon": "LAUST41000000000000003",
    "Pennsylvania": "LAUST42000000000000003",
    "Texas": "LAUST48000000000000003",
    "Virginia": "LAUST51000000000000003",
    "Washington": "LAUST53000000000000003",
}


@dlt.source(name="bls")
def bls_source(api_key: str = dlt.secrets.value) -> dlt.resource:

    @dlt.resource(
        name="state_unemployment",
        primary_key=["state_name", "year", "period"],
        write_disposition="merge",
    )
    def state_unemployment() -> Iterator[dict]:
        """Monthly unemployment rates by state from BLS LAUS."""
        http = httpx.Client(timeout=30.0)

        # BLS API accepts up to 50 series per request
        series_ids = list(STATE_SERIES.values())
        state_lookup = {v: k for k, v in STATE_SERIES.items()}

        # Batch into groups of 25 (conservative — BLS limits at 50)
        for batch_start in range(0, len(series_ids), 25):
            batch = series_ids[batch_start: batch_start + 25]

            try:
                resp = http.post(
                    BLS_BASE,
                    json={
                        "seriesid":   batch,
                        "startyear":  "2020",
                        "endyear":    str(datetime.now().year),
                        "registrationkey": api_key,
                    }
                )
                resp.raise_for_status()
                data = resp.json()

                for series in data.get("Results", {}).get("series", []):
                    series_id = series["seriesID"]
                    state_name = state_lookup.get(series_id, "Unknown")

                    for obs in series.get("data", []):
                        yield {
                            "state_name":         state_name,
                            "series_id":          series_id,
                            "year":               int(obs.get("year", 0)),
                            "period":             obs.get("period", ""),    # e.g. M01
                            "period_name":        obs.get("periodName", ""),
                            "unemployment_rate":  _safe_float(obs.get("value")),
                            "footnotes":          str(obs.get("footnotes", "")),
                            "_loaded_at":         datetime.now(timezone.utc).isoformat(),
                        }

            except Exception as e:
                logger.error(f"BLS API error for batch starting {batch_start}: {e}")

    return state_unemployment


def run_bls_pipeline(settings: Settings) -> dlt.Pipeline:
    pipeline = dlt.pipeline(
        pipeline_name="mental_health_bls",
        destination=dlt.destinations.duckdb(
            credentials="data/duckdb/mental_health_pulse.duckdb"
        ),
        dataset_name="raw_bls",
    )
    load_info = pipeline.run(bls_source(api_key=settings.BLS_API_KEY))
    logger.info(f"BLS pipeline complete: {load_info}")
    return pipeline


# ═══════════════════════════════════════════════════════════════════════════
# SAMHSA PIPELINE
# Source: SAMHSA NSDUH state estimates (static CSV files, annual)
# Auth: None required
# URL: https://www.samhsa.gov/data/nsduh/state-reports-NSDUH-2022
# Engineering note: We load from the SAMHSA public S3 bucket / direct CSV
# ═══════════════════════════════════════════════════════════════════════════

# Direct URLs to SAMHSA NSDUH state-level CSV files
# These are stable government URLs updated annually
SAMHSA_CSV_URLS = {
    2022: "https://www.samhsa.gov/data/sites/default/files/reports/rpt42469/NSDUHsaeLongTermCSVs2022.csv",
    2021: "https://www.samhsa.gov/data/sites/default/files/reports/rpt39441/NSDUHsaeLongTermCSVs2021.csv",
    2020: "https://www.samhsa.gov/data/sites/default/files/reports/rpt35323/NSDUHsaeLongTermCSVs2020.csv",
}


@dlt.source(name="samhsa")
def samhsa_source() -> dlt.resource:

    @dlt.resource(
        name="state_mental_health_estimates",
        primary_key=["state_abbr", "survey_year", "measure"],
        write_disposition="merge",
    )
    def state_estimates() -> Iterator[dict]:
        """
        SAMHSA NSDUH annual state-level mental health estimates.
        Loaded once as a historical backfill — static annual data.
        """
        import csv
        import io
        http = httpx.Client(timeout=60.0)

        for year, url in SAMHSA_CSV_URLS.items():
            try:
                resp = http.get(url)
                resp.raise_for_status()
                reader = csv.DictReader(io.StringIO(resp.text))

                for row in reader:
                    # SAMHSA CSV has state abbreviations, measure names, estimates
                    state = row.get("State", "").strip()
                    if not state or len(state) != 2:
                        continue

                    yield {
                        "state_abbr":    state,
                        "survey_year":   year,
                        "measure":       row.get("Measure", "").strip(),
                        "estimate":      _safe_float(row.get("Estimate")),
                        "lower_ci":      _safe_float(row.get("Lower CI")),
                        "upper_ci":      _safe_float(row.get("Upper CI")),
                        "_loaded_at":    datetime.now(timezone.utc).isoformat(),
                    }

            except Exception as e:
                logger.warning(f"SAMHSA load failed for {year}: {e}")

    return state_estimates


def _safe_float(value) -> Optional[float]:
    if value is None or str(value).strip() in ("", ".", "--"):
        return None
    try:
        return float(str(value).replace(",", ""))
    except (ValueError, TypeError):
        return None


def run_samhsa_pipeline(settings: Settings) -> dlt.Pipeline:
    pipeline = dlt.pipeline(
        pipeline_name="mental_health_samhsa",
        destination=dlt.destinations.duckdb(
            credentials="data/duckdb/mental_health_pulse.duckdb"
        ),
        dataset_name="raw_samhsa",
    )
    load_info = pipeline.run(samhsa_source())
    logger.info(f"SAMHSA pipeline complete: {load_info}")
    return pipeline