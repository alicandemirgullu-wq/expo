"""Utility to export exhibitor listings from Messe Düsseldorf JSON API.

This script fetches paginated exhibitor data from endpoints used by
https://www.interpack.com/ and similar Messe Düsseldorf fairs.  It consumes the
JSON search API that populates the "Exhibitor Index" pages, aggregates the
results and serialises them to CSV and Excel outputs.

The implementation honours best-effort retry logic, exposes a configurable
command line interface and leaves extension hooks for future Google Drive or
Microsoft Graph uploads.
"""
from __future__ import annotations

import argparse
import logging
import string
import sys
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence

import pandas as pd
import requests

logger = logging.getLogger(__name__)


DEFAULT_BASE_URL = "https://www.interpack.com/vis/v1/api/searchResult"
DEFAULT_TICKET = "g_u_e_s_t"
DEFAULT_LANG = "en"
DEFAULT_PAGE_SIZE = 50
DEFAULT_DELAY = 1.0
DEFAULT_RETRIES = 3
REQUEST_TIMEOUT = 30


@dataclass
class FetchConfig:
    """Configuration for API pagination."""

    base_url: str = DEFAULT_BASE_URL
    ticket: str = DEFAULT_TICKET
    lang: str = DEFAULT_LANG
    query: str = ""
    page_size: int = DEFAULT_PAGE_SIZE
    delay: float = DEFAULT_DELAY
    retries: int = DEFAULT_RETRIES
    first_letters: Optional[Sequence[str]] = None


def build_params(
    config: FetchConfig, letter: Optional[str], page: int
) -> Dict[str, str]:
    """Build query parameters for the API request."""

    params: Dict[str, str] = {
        "ticket": config.ticket,
        "lang": config.lang,
        "query": config.query,
        "page": str(page),
        "pageSize": str(config.page_size),
    }
    if letter:
        params["filter[firstLetter]"] = letter
    return params


def fetch_page(
    session: requests.Session,
    config: FetchConfig,
    letter: Optional[str],
    page: int,
) -> List[Dict[str, object]]:
    """Fetch a single page of results.

    Parameters
    ----------
    session:
        The ``requests.Session`` used for HTTP requests.
    config:
        Fetch configuration with credentials and behaviour.
    letter:
        Optional first-letter filter; ``None`` fetches all entries regardless of
        starting letter.
    page:
        The 1-indexed page number to retrieve.

    Returns
    -------
    list of dict
        Parsed result entries. Empty when no results or repeated request
        failures occur.
    """

    params = build_params(config, letter, page)
    attempt = 0

    while attempt < config.retries:
        try:
            logger.debug("Fetching %s page %s", letter or "all", page)
            response = session.get(config.base_url, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            payload = response.json()
            results = (
                payload.get("results")
                or payload.get("data", {}).get("results")
                or payload.get("data", {}).get("items")
                or payload.get("items")
            )
            if not isinstance(results, list):
                logger.warning(
                    "Unexpected response structure for letter=%s page=%s", letter, page
                )
                return []
            return results
        except (requests.RequestException, ValueError) as exc:
            attempt += 1
            logger.warning(
                "Request failed for letter=%s page=%s (attempt %s/%s): %s",
                letter,
                page,
                attempt,
                config.retries,
                exc,
            )
            time.sleep(config.delay)

    logger.error("Exceeded retry budget for letter=%s page=%s", letter, page)
    return []


def normalise_product_categories(entry: Dict[str, object]) -> str:
    """Convert product category information to a comma separated string."""

    categories = entry.get("productCategories")
    if not categories:
        return ""
    if isinstance(categories, str):
        return categories
    if isinstance(categories, Sequence):
        return ", ".join(str(item) for item in categories)
    return str(categories)


def extract_contact(entry: Dict[str, object]) -> Dict[str, str]:
    """Extract contact person information when available."""

    contact = entry.get("contact") or entry.get("contactPerson")
    if not isinstance(contact, dict):
        return {"contactName": "", "contactEmail": ""}

    name_parts: List[str] = []
    for key in ("title", "firstName", "lastName"):
        value = contact.get(key)
        if value:
            name_parts.append(str(value))
    email = contact.get("email") or contact.get("eMail") or ""

    return {"contactName": " ".join(name_parts).strip(), "contactEmail": str(email)}


def transform_entry(entry: Dict[str, object]) -> Dict[str, object]:
    """Map raw JSON entry to a flattened dictionary."""

    address = entry.get("address", {}) if isinstance(entry.get("address"), dict) else {}
    hall_information = entry.get("hall") or entry.get("hallStand") or {}
    if isinstance(hall_information, dict):
        hall = hall_information.get("hall") or hall_information.get("name")
        stand = hall_information.get("stand") or hall_information.get("number")
    else:
        hall = entry.get("hall")
        stand = entry.get("standNumber") or entry.get("stand")

    contact_info = extract_contact(entry)

    transformed = {
        "companyName": entry.get("companyName") or entry.get("name") or "",
        "country": entry.get("country") or address.get("country"),
        "website": entry.get("website") or entry.get("www") or "",
        "hall": hall or "",
        "standNumber": stand or "",
        "productCategories": normalise_product_categories(entry),
        "address": address.get("street") or address.get("addressLine") or "",
        "zipCode": address.get("zipCode") or address.get("zip") or "",
        "city": address.get("city") or "",
    }
    transformed.update(contact_info)
    return transformed


def iter_letters(first_letters: Optional[Sequence[str]]) -> Iterable[Optional[str]]:
    """Yield letters to fetch.

    ``None`` in the generator indicates fetching without a letter filter.
    """

    if first_letters:
        for letter in first_letters:
            yield letter
    else:
        for letter in string.ascii_uppercase:
            yield letter


def fetch_all(config: FetchConfig) -> pd.DataFrame:
    """Fetch all companies for configured letters and pages."""

    session = requests.Session()
    rows: List[Dict[str, object]] = []

    for letter in iter_letters(config.first_letters):
        page = 1
        while True:
            page_results = fetch_page(session, config, letter, page)
            if not page_results:
                break

            for entry in page_results:
                if isinstance(entry, dict):
                    rows.append(transform_entry(entry))
            page += 1
            time.sleep(config.delay)

    session.close()
    return pd.DataFrame(rows)


def save_to_csv(df: pd.DataFrame, filename: str) -> None:
    """Persist the DataFrame to a CSV file using UTF-8 encoding."""

    df.to_csv(filename, index=False, encoding="utf-8")


def save_to_excel(df: pd.DataFrame, filename: str) -> None:
    """Persist the DataFrame to an Excel workbook."""

    df.to_excel(filename, index=False, engine="openpyxl")


def upload_to_google_drive(_: str, __: str) -> None:
    """Placeholder for future Google Drive upload integration."""

    logger.info(
        "Google Drive upload skipped. Provide OAuth credentials and implementation to enable."
    )


def send_via_microsoft_graph(_: str, __: str) -> None:
    """Placeholder for future Microsoft Graph email integration."""

    logger.info(
        "Microsoft Graph email skipped. Provide OAuth credentials and implementation to enable."
    )


def parse_letters(value: Optional[str]) -> Optional[List[str]]:
    """Parse the --first-letter argument into a list of uppercase letters."""

    if not value:
        return None
    letters = [letter.strip().upper() for letter in value.split(",") if letter.strip()]
    return letters or None


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Set up CLI argument parsing."""

    parser = argparse.ArgumentParser(
        description=(
            "Download exhibitor data from Messe Düsseldorf trade fair APIs and export to CSV/Excel."
        )
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Search API base URL")
    parser.add_argument("--ticket", default=DEFAULT_TICKET, help="Access ticket or token")
    parser.add_argument("--lang", default=DEFAULT_LANG, help="Language code for results")
    parser.add_argument("--query", default="", help="Free text search query")
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE, help="Page size")
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY,
        help="Delay between requests in seconds",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=DEFAULT_RETRIES,
        help="Retry attempts per request",
    )
    parser.add_argument(
        "--first-letter",
        help=(
            "Optional comma separated list of initial letters to fetch. Defaults to the entire alphabet."
        ),
    )
    parser.add_argument(
        "--output-csv",
        default="exhibitors.csv",
        help="Destination CSV filename",
    )
    parser.add_argument(
        "--output-xlsx",
        default="exhibitors.xlsx",
        help="Destination Excel filename",
    )
    parser.add_argument(
        "--google-drive-folder",
        help="Optional Google Drive folder ID for future upload integration",
    )
    parser.add_argument(
        "--microsoft-graph-recipient",
        help="Optional email recipient for future Microsoft Graph integration",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Script entry point."""

    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    logger.info(
        "Ensure compliance with Messe Düsseldorf terms of use and robots.txt before scraping."
    )

    config = FetchConfig(
        base_url=args.base_url,
        ticket=args.ticket,
        lang=args.lang,
        query=args.query,
        page_size=args.page_size,
        delay=args.delay,
        retries=args.retries,
        first_letters=parse_letters(args.first_letter),
    )

    logger.info("Starting data collection from %s", config.base_url)
    df = fetch_all(config)

    if df.empty:
        logger.warning("No data retrieved. Check parameters or ticket token.")
    else:
        logger.info("Fetched %s exhibitor entries", len(df))

    save_to_csv(df, args.output_csv)
    logger.info("CSV export written to %s", args.output_csv)

    save_to_excel(df, args.output_xlsx)
    logger.info("Excel export written to %s", args.output_xlsx)

    if args.google_drive_folder:
        upload_to_google_drive(args.output_xlsx, args.google_drive_folder)

    if args.microsoft_graph_recipient:
        send_via_microsoft_graph(args.output_xlsx, args.microsoft_graph_recipient)

    return 0


if __name__ == "__main__":
    sys.exit(main())
