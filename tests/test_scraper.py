"""Tests for the web scraper module."""

import pytest


class TestScraper:
    """Tests for scraping functionality."""

    def test_scrape_chapter_extracts_content(self):
        """Test that a single chapter's content is extracted correctly."""
        # TODO: Implement with mock HTTP responses
        pass

    def test_scrape_chapter_extracts_title(self):
        """Test that the chapter title is extracted."""
        # TODO: Implement
        pass

    def test_scrape_chapter_finds_next_url(self):
        """Test that the next chapter URL is found."""
        # TODO: Implement
        pass

    def test_scrape_novel_chains_chapters(self):
        """Test that multiple chapters are chained via next links."""
        # TODO: Implement
        pass

    def test_scrape_handles_missing_content(self):
        """Test that ScrapingError is raised when content is not found."""
        # TODO: Implement
        pass

    def test_scrape_respects_delay(self):
        """Test that requests are spaced by the configured delay."""
        # TODO: Implement
        pass
