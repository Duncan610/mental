"""
pipeline/cdc_pipeline.py

CDC WONDER → DuckDB ingestion pipeline using dlt.

DATA SOURCE: CDC WONDER API
  URL: https://wonder.cdc.gov
  Data: Drug overdose and suicide mortality by county and month
  Auth: None required (public API)
  Format: Tab-delimited text (quirky but free)

ENGINEERING NOTE — CDC WONDER API QUIRKS:
  CDC WONDER doesn't have a clean REST API. It uses a form-submission
  interface that returns tab-delimited text.


  We use: https://data.cdc.gov/resource/
  Specifically: Drug overdose death rates dataset (VSRR Provisional)

  This is actually what public health analysts use in practice.
  The data.cdc.gov API has proper pagination, filtering, and JSON output.
"""

import logging
from datetime import datetime, timezone
from typing import Iterator

import dlt
import httpx

from config.settings import Settings

logger = logging.getLogger(__name__)

# CDC Socrata API — no auth required, generous rate limits
# Dataset: VSRR Provisional Drug Overdose Death Counts
CDC_OVERDOSE_URL = "https://data.cdc.gov/resource/xkb8-kh2a.json"
# Dataset: Monthly provisional counts of deaths by select causes
CDC_MORTALITY_URL = "https://data.cdc.gov/resource/9dzk-mvmi.json"


@dlt.source(name="cdc")
def cdc_source() -> dlt.resource:
    """
    dlt source for CDC drug overdose and mortality data.
    No authentication required.
    """

    @dlt.resource(
        name="drug_overdose_monthly",
        primary_key=["state", "year_month", "indicator"],
        write_disposition="merge",
    )
    def drug_overdose_monthly(
        # Incremental on data_as_of — CDC updates this field when data refreshes
        data_as_of=dlt.sources.incremental("data_as_of"),
    ) -> Iterator[dict]:
        """
        Monthly provisional drug overdose death counts by state.
        Grain: one row per state per month per indicator (drug category).
        """
        http = httpx.Client(timeout=30.0)
        offset = 0
        limit = 1000  # Socrata page size

        while True:
            resp = http.get(
                CDC_OVERDOSE_URL,
                params={
                    "$limit": limit,
                    "$offset": offset,
                    "$order": "data_as_of ASC",
                }
            )
            resp.raise_for_status()
            rows = resp.json()

            if not rows:
                break

            for row in rows:
                # Normalize the Socrata response fields
                yield {
                    "state":          row.get("state_name", ""),
                    "state_abbr":     row.get("state", ""),
                    "year_month":     row.get("year_month", ""),
                    "indicator":      row.get("indicator", ""),
                    "data_value":     _safe_float(row.get("data_value")),
                    "predicted_value":_safe_float(row.get("predicted_value")),
                    "period":         row.get("period", ""),
                    "data_as_of":     row.get("data_as_of", ""),
                    "_loaded_at":     datetime.now(timezone.utc).isoformat(),
                }

            offset += limit
            if len(rows) < limit:
                break  # Last page

            logger.info(f"CDC overdose: loaded {offset} rows...")

    return drug_overdose_monthly


def _safe_float(value) -> float | None:
    """Convert string to float, returning None for missing/suppressed values."""
    if value is None or str(value).strip() in ("", ".", "Missing", "Suppressed"):
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def run_cdc_pipeline(settings: Settings) -> dlt.Pipeline:
    """Run CDC → DuckDB ingestion pipeline."""
    pipeline = dlt.pipeline(
        pipeline_name="mental_health_cdc",
        destination=dlt.destinations.duckdb(
            credentials="../data/duckdb/mental_health_pulse.duckdb"
        ),
        dataset_name="raw_cdc",
    )

    load_info = pipeline.run(cdc_source())
    logger.info(f"CDC pipeline complete: {load_info}")
    return pipeline


if __name__ == "__main__":
    from config.settings import Settings
    run_cdc_pipeline(Settings())