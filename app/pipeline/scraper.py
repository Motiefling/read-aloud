"""
Web scraper for extracting Chinese light novel text from ttkan.co.

Fetches chapter pages, extracts story content using CSS selectors,
and follows next-chapter links to chain chapters together.
"""

import asyncio
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from app.config import settings


class ScrapingError(Exception):
    """Raised when content extraction fails."""
    pass


async def scrape_chapter(client: httpx.AsyncClient, url: str, config=None) -> dict:
    """
    Scrape a single chapter page and return its content.

    Returns:
        dict with keys: title, chinese_text, source_url, next_url
    """
    if config is None:
        config = settings.scraper

    # TODO: Implement chapter scraping
    # - Fetch page with httpx
    # - Parse with BeautifulSoup(response.text, "lxml")
    # - Extract content using config.content_selector
    # - Extract title using config.title_selector
    # - Extract next chapter URL using config.next_page_selector
    raise NotImplementedError


async def scrape_novel(start_url: str, novel_id: str, config=None) -> list[dict]:
    """
    Scrape all chapters of a novel starting from the given URL.
    Follows next-page links until no more chapters are found.

    Returns:
        List of chapter dicts with keys: title, chinese_text, source_url, chapter_number
    """
    if config is None:
        config = settings.scraper

    # TODO: Implement novel scraping loop
    # - Create httpx.AsyncClient with user_agent header
    # - Loop: scrape_chapter -> follow next_url -> sleep(request_delay_seconds)
    # - Store each chapter in DB
    # - Return list of all chapters
    raise NotImplementedError


async def scrape_table_of_contents(toc_url: str, config=None) -> list[str]:
    """
    Parse a table of contents page to get all chapter URLs at once.

    Returns:
        List of chapter URLs in order.
    """
    if config is None:
        config = settings.scraper

    # TODO: Implement TOC parsing
    raise NotImplementedError
