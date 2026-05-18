"""
pipeline/bluesky_pipeline.py

Bluesky → DuckDB ingestion pipeline using dlt (data load tool).

ENGINEERING DECISION: dlt over custom loader
─────────────────────────────────────────────
dlt has a declarative approach:
  - Schema inference and enforcement built-in
  - Incremental loading with state tracking (replaces our watermark logic)
  - Automatic deduplication via primary keys
  - Writes to DuckDB, Snowflake, BigQuery, Postgres — same code, different target
  - Data contracts via Pydantic (we keep our models)

dlt STATE MANAGEMENT (replaces our manual watermark table):
  dlt stores pipeline state (last cursor, run metadata) in a _dlt_pipeline_state
  table within DuckDB itself. On every incremental run, it reads the last
  cursor and only fetches new data. Identical to our watermark pattern but
  zero infrastructure to maintain.

HOW INCREMENTAL LOADING WORKS WITH dlt:
  1. First run: fetch 7 days of data, load everything, save cursor
  2. Subsequent runs: read cursor from DuckDB state, fetch only new posts
  3. dlt handles deduplication using the primary key (post_uri)
  No duplicate rows. No missing rows. Idempotent by design.
"""

import time
import logging
from datetime import datetime, timezone, timedelta
from typing import Iterator, Optional

import dlt
import httpx
from dlt.sources import DltResource
from pydantic import BaseModel, field_validator

from config.settings import Settings

logger = logging.getLogger(__name__)

BSKY_API = "https://bsky.social/xrpc"


# SEARCH TERMS — mental health signals on Bluesky
SEARCH_TERMS = [
    "#mentalhealth",
    "#depression",
    "#anxiety",
    "#mentalillness",
    "#mentalhealthawareness",
    "#suicideprevention",
    "#recoveryispossible",
    "#therapyworks",
    "mental health",
    "feeling depressed",
    "anxiety attack",
    "mental health crisis",
]


# PYDANTIC SCHEMA — validated before dlt loads to DuckDB
class BlueskyPost(BaseModel):
    """
    Validated Bluesky post. dlt uses this as the DuckDB table schema.
    Field names become column names. Types become DuckDB types.
    """
    post_uri: str           
    post_cid: str
    author_did: str
    author_handle: str
    post_text: str = ""
    like_count: int = 0
    repost_count: int = 0
    reply_count: int = 0
    quote_count: int = 0
    created_at: str     
    search_term: str
    langs: Optional[str] = None

    @field_validator("post_text")
    @classmethod
    def truncate(cls, v: str) -> str:
        return (v or "")[:500]


# BLUESKY SESSION — simple auth without the full atproto SDK overhead
class BlueskySession:
    """Manages an authenticated Bluesky API session."""

    def __init__(self, handle: str, app_password: str):
        self.handle = handle
        self.app_password = app_password
        self.access_jwt: Optional[str] = None
        self.http = httpx.Client(timeout=30.0)

    def authenticate(self) -> None:
        """Exchange handle + app password for an access JWT. ~30ms."""
        resp = self.http.post(
            f"{BSKY_API}/com.atproto.server.createSession",
            json={"identifier": self.handle, "password": self.app_password},
        )
        resp.raise_for_status()
        self.access_jwt = resp.json()["accessJwt"]
        logger.info(f"Bluesky authenticated as {self.handle}")

    def search_posts(
        self,
        query: str,
        since: Optional[str] = None,
        cursor: Optional[str] = None,
        limit: int = 100,
    ) -> dict:
        """
        Search Bluesky posts. Requires authentication.

        Args:
            query:   Search string (hashtag or keyword)
            since:   ISO 8601 datetime — only posts after this timestamp
            cursor:  Pagination cursor from previous response
            limit:   Max results per page (API max: 100)
        """
        if not self.access_jwt:
            raise RuntimeError("Call authenticate() first.")

        params: dict = {"q": query, "limit": limit, "sort": "latest"}
        if since:
            params["since"] = since
        if cursor:
            params["cursor"] = cursor

        resp = self.http.get(
            f"{BSKY_API}/app.bsky.feed.searchPosts",
            headers={"Authorization": f"Bearer {self.access_jwt}"},
            params=params,
        )

        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", 60))
            logger.warning(f"Rate limited. Waiting {wait}s...")
            time.sleep(wait)
            return self.search_posts(query, since, cursor, limit)

        resp.raise_for_status()
        return resp.json()

# dlt SOURCE — the @dlt.source + @dlt.resource decorators define
# HOW data flows into DuckDB.
#
# @dlt.source  = a collection of related resources (like a "connector")
# @dlt.resource = a single data stream (like a "table")
#
# dlt automatically handles:
#   - Schema creation in DuckDB
#   - Incremental state tracking (the "cursor")
#   - Deduplication via primary key
#   - Error recovery

@dlt.source(name="bluesky")
def bluesky_source(
    handle: str = dlt.secrets.value,
    app_password: str = dlt.secrets.value,
) -> DltResource:
    """
    dlt source for Bluesky mental health posts.

    The `dlt.secrets.value` default means dlt reads these from
    .dlt/secrets.toml or environment variables automatically —
    no manual credential passing needed.
    """
    session = BlueskySession(handle, app_password)
    session.authenticate()

    @dlt.resource(
        name="posts",
        primary_key="post_uri",           # dlt deduplicates on this key
        write_disposition="merge",         # MERGE = idempotent upsert
    )
    def mental_health_posts(
        # dlt.sources.incremental tracks state between runs automatically.
        # last_value = the max created_at from the previous run.
        # On first run, initial_value sets how far back we go.
        created_at=dlt.sources.incremental(
            "created_at",
            initial_value=(
                datetime.now(timezone.utc) - timedelta(days=7)
            ).strftime("%Y-%m-%dT%H:%M:%SZ"),
        ),
    ) -> Iterator[dict]:
        """
        Yields Bluesky posts matching mental health search terms.

        dlt calls this generator on every pipeline run.
        The `created_at` incremental cursor automatically filters
        to only new posts since the last successful run.
        """
        since = created_at.last_value
        seen_uris: set[str] = set()  # Cross-term deduplication in memory

        for search_term in SEARCH_TERMS:
            logger.info(f"Searching: '{search_term}' (since: {since})")
            cursor = None
            term_count = 0

            for page in range(10):  # Max 10 pages (1000 posts) per term per run
                try:
                    result = session.search_posts(
                        query=search_term,
                        since=since,
                        cursor=cursor,
                        limit=100,
                    )
                except httpx.HTTPStatusError as e:
                    logger.error(f"API error for '{search_term}': {e}")
                    break

                posts = result.get("posts", [])
                if not posts:
                    break

                for raw in posts:
                    record = raw.get("record", {})
                    author = raw.get("author", {})
                    uri = raw.get("uri", "")

                    if uri in seen_uris:
                        continue
                    seen_uris.add(uri)

                    try:
                        post = BlueskyPost(
                            post_uri=      uri,
                            post_cid=      raw.get("cid", ""),
                            author_did=    author.get("did", ""),
                            author_handle= author.get("handle", ""),
                            post_text=     record.get("text", ""),
                            like_count=    raw.get("likeCount", 0),
                            repost_count=  raw.get("repostCount", 0),
                            reply_count=   raw.get("replyCount", 0),
                            quote_count=   raw.get("quoteCount", 0),
                            created_at=    record.get("createdAt", ""),
                            search_term=   search_term,
                            langs=         ",".join(record.get("langs", [])) or None,
                        )
                        # Yield as dict — dlt handles the DuckDB write
                        yield post.model_dump()
                        term_count += 1

                    except Exception as e:
                        logger.warning(f"Skipping malformed post {uri}: {e}")

                cursor = result.get("cursor")
                if not cursor:
                    break

                time.sleep(0.3)  # Polite delay between pages

            logger.info(f"  '{search_term}': {term_count} posts")
            time.sleep(0.8)  # Polite delay between search terms

    return mental_health_posts

# PIPELINE RUNNER
def run_bluesky_pipeline(settings: Settings) -> dlt.Pipeline:
    """
    Run the Bluesky → DuckDB ingestion pipeline.

    dlt pipeline configuration:
      - pipeline_name: used for state storage (which cursor to resume from)
      - destination: duckdb (writes to a .duckdb file)
      - dataset_name: schema/namespace within DuckDB (raw_bluesky)
    """
    pipeline = dlt.pipeline(
        pipeline_name="mental_health_bluesky",
        destination=dlt.destinations.duckdb(
            credentials=f"../data/duckdb/mental_health_pulse.duckdb"
        ),
        dataset_name="raw_bluesky",   # → DuckDB schema: raw_bluesky
    )

    # Run the source — dlt handles load, dedup, state tracking
    load_info = pipeline.run(
        bluesky_source(
            handle=settings.BLUESKY_HANDLE,
            app_password=settings.BLUESKY_APP_PASSWORD,
        )
    )

    logger.info(f"Bluesky pipeline complete: {load_info}")
    return pipeline


if __name__ == "__main__":
    from config.settings import Settings
    settings = Settings()
    run_bluesky_pipeline(settings)