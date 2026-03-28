"""
Web scraper for extracting Chinese light novel text from supported sites.

Supports multiple sites via per-site configuration profiles.
Fetches chapter pages, extracts story content using CSS selectors,
and follows next-chapter links to chain chapters together.
"""

import asyncio
import logging
import re
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup, NavigableString

from app.config import settings, SiteProfile

logger = logging.getLogger(__name__)


class ScrapingError(Exception):
    """Raised when content extraction fails."""
    pass


class UnsupportedSiteError(Exception):
    """Raised when a URL is from a site with no configured profile."""
    pass


def get_site_profile(url: str) -> tuple[str, SiteProfile]:
    """
    Look up the site profile for a URL based on its domain.
    Returns (domain, profile).
    Raises UnsupportedSiteError if the site is not configured.
    """
    domain = urlparse(url).netloc.removeprefix("www.")
    try:
        return domain, settings.scraper.get_site_profile(domain)
    except KeyError as e:
        raise UnsupportedSiteError(str(e)) from e


async def _fetch_page(client: httpx.AsyncClient, url: str) -> BeautifulSoup:
    """Fetch a page and return a parsed BeautifulSoup object."""
    response = await client.get(url)
    response.raise_for_status()
    return BeautifulSoup(response.text, "lxml")


# ---------------------------------------------------------------------------
# Content extraction helpers — site-specific logic lives here.
# When a profile has a CSS selector, we use it directly.
# When it's None, we fall back to site-specific heuristics.
# ---------------------------------------------------------------------------

def _extract_content(soup: BeautifulSoup, profile: SiteProfile, url: str) -> str:
    """Extract the story text from a chapter page."""
    if not profile.content_selector:
        raise ScrapingError(f"No content_selector configured for this site ({url})")

    element = soup.select_one(profile.content_selector)
    if not element:
        raise ScrapingError(
            f"Content element '{profile.content_selector}' not found at {url}"
        )
    return element.get_text(separator="\n", strip=True)


def _extract_title(soup: BeautifulSoup, domain: str, profile: SiteProfile) -> str:
    """Extract the chapter title from a chapter page."""
    # If the profile has a CSS selector, use it
    if profile.title_selector:
        el = soup.select_one(profile.title_selector)
        if el:
            return el.get_text(strip=True)

    # --- Site-specific fallbacks ---

    if domain == "funs.me":
        # On funs.me, the title is a bare text node between the book-title <a>
        # and the font-size toggle (#enlarge_font_size). Look for text matching
        # the chapter heading pattern (e.g. "第86章 ...").
        # Exclude matches inside <title>, <script>, and #ChSize.
        exclude_tags = {"title", "script", "style"}
        for text_node in soup.find_all(string=re.compile(r"第\d+章")):
            parent = text_node.parent
            if parent is None:
                continue
            tag_name = getattr(parent, "name", None)
            if tag_name in exclude_tags:
                continue
            # Skip if inside #ChSize (the content div)
            if parent.get("id") == "ChSize" or parent.find_parent(id="ChSize"):
                continue
            return text_node.strip()

    return "Untitled"


def _extract_next_url(soup: BeautifulSoup, domain: str, profile: SiteProfile, current_url: str) -> str | None:
    """Extract the next-chapter URL from a chapter page. Returns None if there is no next chapter."""
    # If the profile has a CSS selector, use it
    if profile.next_page_selector:
        el = soup.select_one(profile.next_page_selector)
        if el and el.get("href"):
            return urljoin(current_url, el["href"])
        return None

    # --- Site-specific fallbacks ---

    if domain == "funs.me":
        # Navigation links are bare <a> tags. The "next" link contains "下一頁".
        for a in soup.find_all("a", href=True):
            if "下一頁" in a.get_text():
                href = a["href"]
                next_url = urljoin(current_url, href)
                # Sanity check: the next URL should be a chapter page on the same site,
                # not a link to a different section. Chapter hrefs are typically
                # relative like "15620087.html" or absolute like "/mtext/2058/15620087.html".
                if re.search(r"/m?text/\d+/\d+\.html", next_url) or re.match(r"\d+\.html$", href):
                    return next_url
        return None

    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def scrape_chapter(client: httpx.AsyncClient, url: str, domain: str, profile: SiteProfile) -> dict:
    """
    Scrape a single chapter page and return its content.

    Returns:
        dict with keys: title, chinese_text, source_url, next_url
    """
    soup = await _fetch_page(client, url)

    chinese_text = _extract_content(soup, profile, url)
    title = _extract_title(soup, domain, profile)
    next_url = _extract_next_url(soup, domain, profile, url)

    return {
        "title": title,
        "chinese_text": chinese_text,
        "source_url": url,
        "next_url": next_url,
    }


async def resolve_start_url(url: str) -> str:
    """
    If the URL is a TOC/book page, parse it to find the first chapter URL.
    If it's already a chapter URL, return it as-is.

    Supports:
        - funs.me book pages:    /book/{id}.html  -> first chapter from TOC
        - funs.me chapter pages: /mtext/{id}/{id}.html  -> returned as-is
    """
    domain, profile = get_site_profile(url)

    if domain == "funs.me":
        # Chapter URLs match /mtext/{book_id}/{chapter_id}.html
        if re.search(r"/mtext/\d+/\d+\.html", url):
            return url
        # /text/ links also point to chapters — just convert to /mtext/
        if re.search(r"/text/\d+/\d+\.html", url):
            return url.replace("/text/", "/mtext/")
        # TOC/book URLs match /book/{id}.html or similar non-chapter paths
        # Try parsing as TOC to get chapter list
        chapter_urls = await scrape_table_of_contents(url)
        if not chapter_urls:
            raise ScrapingError(f"No chapters found on TOC page: {url}")
        logger.info("Resolved TOC URL to %d chapters, starting from first.", len(chapter_urls))
        return chapter_urls[0]

    # For unknown patterns, assume it's a chapter URL
    return url


async def scrape_novel(
    start_url: str,
    novel_id: str,
    max_chapters: int | None = None,
    on_chapter: callable = None,
) -> list[dict]:
    """
    Scrape all chapters of a novel starting from the given URL.

    Accepts either a chapter URL or a TOC/book URL — if a TOC URL is given,
    it resolves to the first chapter automatically.

    Follows next-page links until no more chapters are found.

    Args:
        start_url: URL of the first chapter or a TOC/book page.
        novel_id: Identifier for this novel (used in returned dicts).
        max_chapters: Optional cap on how many chapters to scrape.
        on_chapter: Optional async callback called after each chapter is scraped,
                    receives (chapter_number, chapter_dict).

    Returns:
        List of chapter dicts.
    """
    start_url = await resolve_start_url(start_url)
    domain, profile = get_site_profile(start_url)
    config = settings.scraper

    chapters = []
    current_url = start_url
    chapter_number = 1

    async with httpx.AsyncClient(
        headers={"User-Agent": config.user_agent},
        follow_redirects=True,
        timeout=30.0,
    ) as client:
        while current_url:
            if max_chapters and chapter_number > max_chapters:
                logger.info("Reached max_chapters cap (%d), stopping.", max_chapters)
                break

            logger.info("Scraping chapter %d: %s", chapter_number, current_url)

            result = None
            for attempt in range(1, config.max_retries + 1):
                try:
                    result = await scrape_chapter(client, current_url, domain, profile)
                    break
                except httpx.HTTPStatusError as e:
                    logger.warning(
                        "HTTP %d on attempt %d/%d for %s",
                        e.response.status_code, attempt, config.max_retries, current_url,
                    )
                    if attempt == config.max_retries:
                        # If we already have chapters, stop gracefully instead of crashing
                        if chapters:
                            logger.info(
                                "Stopping scrape after %d chapters — next URL failed: %s",
                                len(chapters), current_url,
                            )
                        else:
                            raise ScrapingError(
                                f"Failed to fetch {current_url} after {config.max_retries} attempts: {e}"
                            ) from e
                    else:
                        await asyncio.sleep(config.request_delay_seconds * attempt)
                except httpx.RequestError as e:
                    logger.warning(
                        "Request error on attempt %d/%d for %s: %s",
                        attempt, config.max_retries, current_url, e,
                    )
                    if attempt == config.max_retries:
                        if chapters:
                            logger.info(
                                "Stopping scrape after %d chapters — next URL failed: %s",
                                len(chapters), current_url,
                            )
                        else:
                            raise ScrapingError(
                                f"Failed to fetch {current_url} after {config.max_retries} attempts: {e}"
                            ) from e
                    else:
                        await asyncio.sleep(config.request_delay_seconds * attempt)

            # If fetch failed after retries and we have previous chapters, stop here
            if result is None:
                break

            chapter = {
                "novel_id": novel_id,
                "chapter_number": chapter_number,
                **result,
            }
            chapters.append(chapter)

            if on_chapter:
                await on_chapter(chapter_number, chapter)

            current_url = result["next_url"]
            chapter_number += 1

            if current_url:
                await asyncio.sleep(config.request_delay_seconds)

    logger.info("Finished scraping %d chapters for novel %s.", len(chapters), novel_id)
    return chapters


async def scrape_table_of_contents(toc_url: str) -> list[str]:
    """
    Parse a table of contents page to get all chapter URLs at once.

    Returns:
        List of chapter URLs in reading order.
    """
    domain, profile = get_site_profile(toc_url)
    config = settings.scraper

    async with httpx.AsyncClient(
        headers={"User-Agent": config.user_agent},
        follow_redirects=True,
        timeout=30.0,
    ) as client:
        soup = await _fetch_page(client, toc_url)

    # --- Site-specific TOC parsing ---

    if domain == "funs.me":
        # TOC page lists all chapters as <a> tags with href like /text/{book_id}/{chapter_id}.html
        # We need to convert /text/ URLs to /mtext/ since that's what chapter pages actually use.
        chapter_links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if re.search(r"/text/\d+/\d+\.html$", href):
                # Convert /text/ to /mtext/ for actual chapter page URLs
                chapter_url = urljoin(toc_url, href.replace("/text/", "/mtext/"))
                chapter_links.append(chapter_url)
        return chapter_links

    raise ScrapingError(f"TOC parsing not implemented for {domain}")
