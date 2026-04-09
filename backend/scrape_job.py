"""
Standalone scraper job for GitHub Actions cron.
Syncs current season data and backfills any new matches.
"""

import os
import sys
import logging
import asyncio

# Ensure we can import sibling modules
sys.path.insert(0, os.path.dirname(__file__))

from database import init_db, get_total_match_count, get_total_try_count
from scraper import scrape_all, sync_current_season

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    init_db()
    count = get_total_match_count()
    logger.info(f"DB has {count} matches.")

    if count == 0:
        # Fresh database — run full historical scrape
        logger.info("Empty database detected. Running full historical scrape...")
        asyncio.run(scrape_all())
    else:
        # Existing data — just sync recent rounds
        logger.info("Running current season sync...")
        asyncio.run(sync_current_season())

    final = get_total_match_count()
    tries = get_total_try_count()
    logger.info(f"Done. DB now has {final} matches, {tries} tries.")


if __name__ == "__main__":
    main()
