"""
Web scraper for extracting Chinese light novel text from supported sites.

Supports multiple sites via per-site configuration profiles.
Fetches chapter pages, extracts story content using CSS selectors,
and follows next-chapter links to chain chapters together.

Uses httpx for simple sites and Playwright (headless Chromium) for sites
with anti-bot protection (Cloudflare, JS challenges, etc.).
"""

import asyncio
import logging
import re
from contextlib import asynccontextmanager
from typing import Callable, Awaitable
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup, NavigableString

from app.config import settings, SiteProfile, ScraperSettings, BASE_DIR, get_data_dir

logger = logging.getLogger(__name__)

# Type alias: an async callable that fetches a URL and returns parsed HTML
FetchFn = Callable[[str], Awaitable[BeautifulSoup]]


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


# ---------------------------------------------------------------------------
# Fetcher abstraction — httpx for simple sites, Playwright for protected ones
# ---------------------------------------------------------------------------

async def _fetch_page(client: httpx.AsyncClient, url: str) -> BeautifulSoup:
    """Fetch a page via httpx and return a parsed BeautifulSoup object."""
    response = await client.get(url)
    response.raise_for_status()
    return BeautifulSoup(response.text, "lxml")


@asynccontextmanager
async def _create_fetcher(profile: SiteProfile, config: ScraperSettings):
    """
    Create a fetch function appropriate for the site.

    Yields an async callable: fetch(url) -> BeautifulSoup

    For sites with use_browser=True, launches a headless Chromium browser
    via Playwright. Otherwise uses httpx.
    """
    if profile.use_browser:
        from playwright.async_api import async_playwright
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(user_agent=config.user_agent)
            page = await context.new_page()
            wait_ms = profile.browser_wait_time or 5000

            async def fetch_browser(url: str) -> BeautifulSoup:
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(wait_ms)
                # Wait for any JS redirects (e.g. anti-bot challenges) to settle
                await page.wait_for_load_state("domcontentloaded")
                html = await page.content()
                return BeautifulSoup(html, "lxml")

            try:
                yield fetch_browser
            finally:
                await browser.close()
    else:
        async with httpx.AsyncClient(
            headers={"User-Agent": config.user_agent},
            follow_redirects=True,
            timeout=30.0,
        ) as client:
            async def fetch_httpx(url: str) -> BeautifulSoup:
                response = await client.get(url)
                response.raise_for_status()
                return BeautifulSoup(response.text, "lxml")

            yield fetch_httpx


# ---------------------------------------------------------------------------
# Content extraction helpers — site-specific logic lives here.
# When a profile has a CSS selector, we use it directly.
# When it's None, we fall back to site-specific heuristics.
# ---------------------------------------------------------------------------

def _extract_content(soup: BeautifulSoup, domain: str, profile: SiteProfile, url: str) -> str:
    """Extract the story text from a chapter page."""
    if not profile.content_selector:
        raise ScrapingError(f"No content_selector configured for this site ({url})")

    element = soup.select_one(profile.content_selector)
    if not element:
        raise ScrapingError(
            f"Content element '{profile.content_selector}' not found at {url}"
        )

    # --- Site-specific content cleanup ---

    if domain == "piaotia.com":
        # #content contains mixed nav elements (h1, div.toplink, table, <a> tags)
        # alongside bare text nodes that hold the actual chapter content.
        # Remove all structured elements to leave only the text.
        for tag in element.find_all(["h1", "table", "div", "a", "script"]):
            tag.decompose()

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
        # On funs.me, the title is a bare text node in a <font> tag between the
        # book-title <a> and the content div. It matches the chapter heading
        # pattern "第N章 ..." where N can be Arabic digits (第86章) or Chinese
        # numerals (第一章, 第二十三章).
        # Exclude matches inside <title>, <script>, and #ChSize.
        _ch_num = r"[\d一二三四五六七八九十百千零〇]+"
        exclude_tags = {"title", "script", "style"}
        for text_node in soup.find_all(string=re.compile(rf"第{_ch_num}章")):
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

    if domain == "dxmwx.org":
        # Navigation links are bare <a> tags with text "下一章".
        # Sentinel href ending in "_0.html" means no next chapter.
        for a in soup.find_all("a", href=True):
            if "下一章" in a.get_text():
                href = a["href"]
                if href.endswith("_0.html"):
                    return None
                return urljoin(current_url, href)
        return None

    if domain == "piaotia.com":
        # Navigation links are bare <a> tags with text "下一章".
        # Chapter hrefs are numeric like "3356132.html"; non-chapter = "index.html" or "./"
        for a in soup.find_all("a", href=True):
            if "下一章" in a.get_text():
                href = a["href"]
                if re.match(r"\d+\.html$", href):
                    return urljoin(current_url, href)
                return None
        return None

    if domain == "ixdzs8.com":
        # Navigation links are bare <a> tags with text "下一章".
        # Chapter hrefs match /read/{id}/p{num}.html; TOC link is /read/{id}/
        for a in soup.find_all("a", href=True):
            if "下一章" in a.get_text():
                href = a["href"]
                if re.search(r"/read/\d+/p\d+\.html", href):
                    return urljoin(current_url, href)
                return None
        return None

    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def scrape_chapter(fetch_fn: FetchFn, url: str, domain: str, profile: SiteProfile) -> dict:
    """
    Scrape a single chapter page and return its content.

    Args:
        fetch_fn: Async callable that fetches a URL and returns BeautifulSoup.

    Returns:
        dict with keys: title, chinese_text, source_url, next_url
    """
    soup = await fetch_fn(url)

    # Extract title and next_url before content, because content extraction
    # may decompose elements in the soup (e.g. piaotia strips nav from #content).
    title = _extract_title(soup, domain, profile)
    next_url = _extract_next_url(soup, domain, profile, url)
    chinese_text = _extract_content(soup, domain, profile, url)

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
        chapter_urls = await scrape_table_of_contents(url)
        if not chapter_urls:
            raise ScrapingError(f"No chapters found on TOC page: {url}")
        logger.info("Resolved TOC URL to %d chapters, starting from first.", len(chapter_urls))
        return chapter_urls[0]

    if domain == "dxmwx.org":
        # Chapter URLs match /read/{book_id}_{chapter_id}.html
        if re.search(r"/read/\d+_\d+\.html", url):
            return url
        # Book/TOC URLs match /book/{id}.html — resolve via TOC
        chapter_urls = await scrape_table_of_contents(url)
        if not chapter_urls:
            raise ScrapingError(f"No chapters found on TOC page: {url}")
        logger.info("Resolved TOC URL to %d chapters, starting from first.", len(chapter_urls))
        return chapter_urls[0]

    if domain == "ttkan.co":
        # Chapter URLs match /novel/pagea/{slug}_{number}.html
        if re.search(r"/novel/pagea/.+_\d+\.html", url):
            return url
        # TOC URLs match /novel/chapters/{slug} — resolve via TOC
        chapter_urls = await scrape_table_of_contents(url)
        if not chapter_urls:
            raise ScrapingError(f"No chapters found on TOC page: {url}")
        logger.info("Resolved TOC URL to %d chapters, starting from first.", len(chapter_urls))
        return chapter_urls[0]

    if domain == "piaotia.com":
        # Chapter URLs match /html/{group}/{book_id}/{chapter_id}.html
        if re.search(r"/html/\d+/\d+/\d+\.html", url):
            return url
        # Book info page /bookinfo/{group}/{book_id}.html -> chapter list at /html/{group}/{book_id}/
        m = re.search(r"/bookinfo/(\d+)/(\d+)\.html", url)
        if m:
            group, book_id = m.groups()
            toc_url = urljoin(url, f"/html/{group}/{book_id}/")
            chapter_urls = await scrape_table_of_contents(toc_url)
            if not chapter_urls:
                raise ScrapingError(f"No chapters found on TOC page: {toc_url}")
            logger.info("Resolved book info URL to %d chapters, starting from first.", len(chapter_urls))
            return chapter_urls[0]
        # Chapter list page /html/{group}/{book_id}/ — resolve via TOC
        chapter_urls = await scrape_table_of_contents(url)
        if not chapter_urls:
            raise ScrapingError(f"No chapters found on TOC page: {url}")
        logger.info("Resolved TOC URL to %d chapters, starting from first.", len(chapter_urls))
        return chapter_urls[0]

    if domain == "ixdzs8.com":
        # Chapter URLs match /read/{book_id}/p{number}.html
        if re.search(r"/read/\d+/p\d+\.html", url):
            return url
        # TOC URL is /read/{book_id}/ — AJAX chapter list is unreliable,
        # but chapters follow sequential p1, p2, ... pattern. Start from p1.
        m = re.search(r"/read/(\d+)/?$", url)
        if m:
            first_chapter = urljoin(url.rstrip("/") + "/", "p1.html")
            logger.info("Resolved TOC URL to first chapter: %s", first_chapter)
            return first_chapter
        return url

    # For unknown patterns, assume it's a chapter URL
    return url


async def scrape_novel(
    start_url: str,
    novel_id: str,
    max_chapters: int | None = None,
    on_chapter: callable = None,
    start_number: int = 1,
    cancel_check: callable = None,
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
        start_number: Chapter number to start counting from (for log messages).
        cancel_check: Optional callable that returns True if scraping should stop
                      (e.g. because the job was cancelled).

    Returns:
        List of chapter dicts.
    """
    start_url = await resolve_start_url(start_url)
    domain, profile = get_site_profile(start_url)
    config = settings.scraper

    chapters = []
    current_url = start_url
    chapter_number = start_number
    scraped_count = 0

    async with _create_fetcher(profile, config) as fetch:
        while current_url:
            if cancel_check and cancel_check():
                logger.info("Scraping cancelled after %d chapters.", scraped_count)
                break

            if max_chapters and scraped_count >= max_chapters:
                logger.info("Reached max_chapters cap (%d), stopping.", max_chapters)
                break

            logger.info("Scraping chapter %d: %s", chapter_number, current_url)

            result = None
            for attempt in range(1, config.max_retries + 1):
                try:
                    result = await scrape_chapter(fetch, current_url, domain, profile)
                    break
                except ScrapingError:
                    raise  # Don't retry parse errors
                except Exception as e:
                    logger.warning(
                        "Error on attempt %d/%d for %s: %s",
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
            scraped_count += 1

            if on_chapter:
                await on_chapter(chapter_number, chapter)

            current_url = result["next_url"]
            chapter_number += 1

            if current_url:
                await asyncio.sleep(config.request_delay_seconds)

    logger.info("Finished scraping %d chapters for novel %s.", len(chapters), novel_id)
    return chapters


async def check_for_updates(last_chapter_url: str) -> bool:
    """
    Lightweight check: fetch the last chapter's page and see if a next-chapter
    link exists.  Does NOT scrape content — just checks for the link.

    Returns True if a next chapter is available, False otherwise.
    """
    domain, profile = get_site_profile(last_chapter_url)
    config = settings.scraper

    async with _create_fetcher(profile, config) as fetch:
        soup = await fetch(last_chapter_url)
        next_url = _extract_next_url(soup, domain, profile, last_chapter_url)
        return next_url is not None


async def scrape_novel_title(source_url: str) -> str | None:
    """
    Scrape the novel title from a book/TOC page.

    Extracts from the <title> tag and strips site names and metadata.
    Returns the raw Chinese title, or None if not found.
    """
    domain, profile = get_site_profile(source_url)
    config = settings.scraper

    try:
        async with _create_fetcher(profile, config) as fetch:
            soup = await fetch(source_url)

            # Try og:title first
            og = soup.find("meta", property="og:title")
            if og and og.get("content"):
                title = og["content"].strip()
            elif soup.title and soup.title.string:
                title = soup.title.string.strip()
            else:
                return None

            # Clean up: strip site names and common suffixes.
            # Most sites use " - SiteName" or "《title》 metadata - site"
            # Strip 《》 brackets
            title = re.sub(r"[《》]", "", title)
            # Take everything before the first " - " or " – " separator
            title = re.split(r"\s*[-–|]\s*", title)[0].strip()
            # Strip common suffixes like "最新章節" etc.
            title = re.sub(r"\s*最新.*$", "", title).strip()

            if title:
                logger.info("Scraped novel title: %s", title)
                return title
            return None

    except Exception as e:
        logger.warning("Failed to scrape novel title: %s", e)
        return None


async def scrape_cover_image(source_url: str, novel_id: str) -> str | None:
    """
    Scrape the cover image from a novel's book/TOC page and save it locally.

    Tries og:image meta tag first, then site-specific fallbacks.
    Returns the relative path to the saved image, or None if not found.
    """
    domain, profile = get_site_profile(source_url)
    config = settings.scraper

    try:
        async with _create_fetcher(profile, config) as fetch:
            soup = await fetch(source_url)

            image_url = None

            # Site-specific cover selectors
            if domain == "funs.me":
                # Cover is at /bimg/{book_id}.jpg
                m = re.search(r"/book/(\d+)\.html", source_url)
                if m:
                    image_url = urljoin(source_url, f"/bimg/{m.group(1)}.jpg")

            # Generic fallback: og:image meta tag (works for ttkan.co and many others)
            if not image_url:
                og = soup.find("meta", property="og:image")
                if og and og.get("content"):
                    image_url = urljoin(source_url, og["content"])

            if not image_url:
                logger.info("No cover image found for %s", source_url)
                return None

            # Download the image
            logger.info("Downloading cover image: %s", image_url)
            async with httpx.AsyncClient(
                headers={"User-Agent": config.user_agent},
                follow_redirects=True,
                timeout=30.0,
            ) as client:
                resp = await client.get(image_url)
                resp.raise_for_status()

            # Determine file extension from content type or URL
            content_type = resp.headers.get("content-type", "")
            if "png" in content_type:
                ext = ".png"
            elif "webp" in content_type:
                ext = ".webp"
            else:
                ext = ".jpg"

            cover_dir = get_data_dir() / "novels" / novel_id
            cover_dir.mkdir(parents=True, exist_ok=True)
            cover_path = cover_dir / f"cover{ext}"
            cover_path.write_bytes(resp.content)

            relative_path = str(cover_path.relative_to(get_data_dir())).replace("\\", "/")
            logger.info("Saved cover image: %s", relative_path)
            return relative_path

    except Exception as e:
        logger.warning("Failed to scrape cover image: %s", e)
        return None


async def scrape_table_of_contents(toc_url: str) -> list[str]:
    """
    Parse a table of contents page to get all chapter URLs at once.

    Returns:
        List of chapter URLs in reading order.
    """
    domain, profile = get_site_profile(toc_url)
    config = settings.scraper

    async with _create_fetcher(profile, config) as fetch:
        soup = await fetch(toc_url)

        # --- Site-specific TOC parsing ---

        if domain == "funs.me":
            # TOC page lists all chapters as <a> tags with href like /text/{book_id}/{chapter_id}.html
            # We need to convert /text/ URLs to /mtext/ since that's what chapter pages actually use.
            chapter_links = []
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if re.search(r"/text/\d+/\d+\.html$", href):
                    chapter_url = urljoin(toc_url, href.replace("/text/", "/mtext/"))
                    chapter_links.append(chapter_url)
            return chapter_links

        if domain == "dxmwx.org":
            # Two-level TOC: book page lists chapter range pages, each range page lists chapters.
            range_links = []
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if re.search(r"/chapternum/\d+/\d+_\d+\.html$", href):
                    range_links.append(urljoin(toc_url, href))

            chapter_links = []
            for range_url in range_links:
                await asyncio.sleep(config.request_delay_seconds)
                range_soup = await fetch(range_url)
                for a in range_soup.find_all("a", href=True):
                    href = a["href"]
                    if re.search(r"/read/\d+_\d+\.html$", href):
                        chapter_links.append(urljoin(range_url, href))
            return chapter_links

        if domain == "ttkan.co":
            # All chapters listed on one page as links to /novel/pagea/{slug}_{number}.html
            chapter_links = []
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if re.search(r"/novel/pagea/.+_\d+\.html$", href):
                    chapter_links.append(urljoin(toc_url, href))
            return chapter_links

        if domain == "piaotia.com":
            # Chapter list at /html/{group}/{book_id}/ has all chapters as
            # bare relative links like "3356131.html" (numeric IDs).
            chapter_links = []
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if re.match(r"\d+\.html$", href):
                    chapter_links.append(urljoin(toc_url, href))
            return chapter_links

    raise ScrapingError(f"TOC parsing not implemented for {domain}")
