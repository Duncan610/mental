import logging
from datetime import datetime, timezone
from typing import Iterator

import dlt
import httpx

from config.settings import Settings

logger = logging.getLogger(__name__)

# Correct CDC endpoint - VSRR Provisional Drug Overdose Death Counts
CDC_OVERDOSE_URL = "https://data.cdc.gov/resource/xkb8-kh2a.json"


@dlt.source(name="cdc")
def cdc_source():

    @dlt.resource(
        name="drug_overdose_monthly",
        primary_key=["state_name", "year", "month", "indicator"],
        write_disposition="merge",
    )
    def drug_overdose_monthly() -> Iterator[dict]:
        http = httpx.Client(timeout=30.0)
        offset = 0
        limit = 1000

        while True:
            try:
                resp = http.get(
                    CDC_OVERDOSE_URL,
                    params={
                        "$limit": limit,
                        "$offset": offset,
                    }
                )
                resp.raise_for_status()
                rows = resp.json()

                if not rows:
                    break

                for row in rows:
                    yield {
                        "state_name":    row.get("state_name", ""),
                        "state_abbr":    row.get("state", ""),
                        "year":          row.get("year", ""),
                        "month":         row.get("month", ""),
                        "period":        row.get("period", ""),
                        "indicator":     row.get("indicator", ""),
                        "data_value":    _safe_float(row.get("data_value")),
                        "data_as_of":    row.get("data_as_of", ""),
                        "_loaded_at":    datetime.now(timezone.utc).isoformat(),
                    }

                offset += limit
                logger.info(f"CDC: loaded {offset} rows...")

                if len(rows) < limit:
                    break

            except Exception as e:
                logger.error(f"CDC API error at offset {offset}: {e}")
                break

    return drug_overdose_monthly


def _safe_float(value):
    if value is None or str(value).strip() in ("", ".", "Missing", "Suppressed"):
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def run_cdc_pipeline(settings: Settings):
    pipeline = dlt.pipeline(
        pipeline_name="mental_health_cdc",
        destination=dlt.destinations.duckdb(
            credentials="data/duckdb/mental_health_pulse.duckdb"
        ),
        dataset_name="raw_cdc",
    )
    load_info = pipeline.run(cdc_source())
    logger.info(f"CDC pipeline complete: {load_info}")
    return pipeline


if __name__ == "__main__":
    run_cdc_pipeline(Settings())


if __name__ == "__main__":
    run_cdc_pipeline(Settings())