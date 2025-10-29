"""Scrape exhibitor information from the ceramitec exhibitor directory.

The directory at
https://exhibitors.ceramitec.com/en/exhibitors-details/exhibitors-brands-cross-references-search/exhibitorFulltextlist/
serves its data through a client-side application.  The page embeds a large JSON
blob inside a ``<script id="__NEXT_DATA__">`` tag (Next.js) which contains the
complete exhibitor listing.  This module downloads the page, extracts the JSON
payload and normalises the entries into a flat structure that can easily be
persisted as CSV.

The implementation only relies on the Python standard library so the script can
be dropped into any environment without additional dependencies.  It also
contains a best-effort fallback that scans other ``<script>`` tags for JSON and
lightweight heuristics for detecting exhibitor objects.  This keeps the scraper
robust even if the page structure changes slightly.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import sys
import time
from dataclasses import dataclass
from html import unescape
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Mapping, MutableMapping, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, build_opener


LOGGER = logging.getLogger(__name__)

DEFAULT_URL = (
    "https://exhibitors.ceramitec.com/en/"
    "exhibitors-details/exhibitors-brands-cross-references-search/"
    "exhibitorFulltextlist/"
)
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119 Safari/537.36"
)
DEFAULT_RETRIES = 3
DEFAULT_DELAY = 1.0

FIELD_MAP = {
    "company_name": (
        "companyName",
        "company",
        "company_name",
        "exhibitorName",
        "name",
        "title",
        "profileName",
        "organisation",
    ),
    "country": ("country", "nation", "land", "countryName"),
    "city": ("city", "town", "location"),
    "address": (
        "address",
        "street",
        "addressLine",
        "fullAddress",
        "postalAddress",
    ),
    "phone": ("phone", "telephone", "tel", "phoneNumber"),
    "email": ("email", "mail", "e_mail"),
    "website": ("website", "web", "url", "homepage"),
    "hall": ("hall", "hallName", "hallNumber"),
    "stand": ("stand", "booth", "standNumber", "boothNumber"),
    "brands": ("brands", "brand", "brandNames", "brandList"),
}

LIST_DETECTION_KEYWORDS = (
    "company",
    "exhibitor",
    "brand",
    "stand",
    "hall",
)

SCRIPT_JSON_PATTERNS = (
    re.compile(r"<script[^>]+id=\"__NEXT_DATA__\"[^>]*>(?P<json>{.*?})</script>", re.DOTALL),
    re.compile(r"<script[^>]*>\s*window\.__NUXT__\s*=\s*(?P<json>{.*?})</script>", re.DOTALL),
    re.compile(r"<script[^>]*>\s*window\.__INITIAL_STATE__\s*=\s*(?P<json>{.*?})</script>", re.DOTALL),
)


@dataclass
class Exhibitor:
    """Normalised exhibitor entry."""

    source: str
    company_name: str = ""
    country: str = ""
    city: str = ""
    address: str = ""
    phone: str = ""
    email: str = ""
    website: str = ""
    hall: str = ""
    stand: str = ""
    brands: str = ""

    def as_row(self) -> Dict[str, str]:
        return {
            "Source": self.source,
            "Company Name": self.company_name,
            "Country": self.country,
            "City": self.city,
            "Address": self.address,
            "Phone": self.phone,
            "Email": self.email,
            "Website": self.website,
            "Hall": self.hall,
            "Stand": self.stand,
            "Brands": self.brands,
        }


def fetch_html(url: str, *, retries: int, delay: float) -> str:
    """Download HTML content with a couple of retries."""

    last_error: Optional[Exception] = None
    opener = build_opener()  # honour environment proxy configuration
    for attempt in range(1, retries + 1):
        LOGGER.debug("Fetching %s (attempt %s/%s)", url, attempt, retries)
        request = Request(url, headers={"User-Agent": DEFAULT_USER_AGENT})
        try:
            with opener.open(request, timeout=45) as response:
                charset = response.headers.get_content_charset() or "utf-8"
                html = response.read().decode(charset, errors="replace")
                LOGGER.debug("Downloaded %s characters", len(html))
                return html
        except (HTTPError, URLError, TimeoutError) as exc:  # pragma: no cover - network
            last_error = exc
            LOGGER.warning("Network error: %s", exc)
            if attempt < retries:
                time.sleep(delay)
    raise RuntimeError(f"Unable to fetch {url!r}: {last_error}")


def iter_json_blobs(html: str) -> Iterator[str]:
    """Yield JSON blobs embedded in ``<script>`` tags."""

    for pattern in SCRIPT_JSON_PATTERNS:
        for match in pattern.finditer(html):
            blob = unescape(match.group("json"))
            LOGGER.debug("Found JSON blob via %s", pattern.pattern[:30])
            yield blob

    # Generic fallback: pick other script tags that look like JSON
    script_pattern = re.compile(r"<script[^>]*>(.*?)</script>", re.DOTALL)
    for match in script_pattern.finditer(html):
        body = match.group(1).strip()
        if not body.startswith("{"):
            continue
        LOGGER.debug("Found generic JSON script blob")
        yield unescape(body)


def parse_json(blob: str) -> Optional[object]:
    try:
        return json.loads(blob)
    except json.JSONDecodeError:
        return None


def walk_values(node: object) -> Iterator[object]:
    if isinstance(node, Mapping):
        for value in node.values():
            yield value
            yield from walk_values(value)
    elif isinstance(node, list):
        for item in node:
            yield item
            yield from walk_values(item)


def looks_like_exhibitor(candidate: object) -> bool:
    if not isinstance(candidate, Mapping):
        return False
    lowered = {str(key).lower() for key in candidate.keys()}
    return any(keyword in key for keyword in LIST_DETECTION_KEYWORDS for key in lowered)


def discover_exhibitor_lists(data: object) -> Iterator[List[Mapping[str, object]]]:
    if isinstance(data, list):
        if data and all(isinstance(item, Mapping) for item in data):
            score = sum(looks_like_exhibitor(item) for item in data)
            if score >= max(1, len(data) // 3):
                LOGGER.debug("Detected exhibitor list with %s entries", len(data))
                yield data  # type: ignore[return-value]
        for item in data:
            yield from discover_exhibitor_lists(item)
    elif isinstance(data, Mapping):
        for value in data.values():
            yield from discover_exhibitor_lists(value)


def extract_first(entry: Mapping[str, object], candidates: Iterable[str]) -> str:
    for key in candidates:
        if "\." in key:
            outer, inner = key.split(".", 1)
            value = entry.get(outer)
            if isinstance(value, Mapping):
                result = extract_first(value, [inner])
                if result:
                    return result
            continue
        if key in entry:
            value = entry[key]
            text = normalise_value(value)
            if text:
                return text
        for actual_key, value in entry.items():
            if actual_key.lower() == key.lower():
                text = normalise_value(value)
                if text:
                    return text
    return ""


def normalise_value(value: object) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, Mapping):
        # flatten simple nested dictionaries
        items = [f"{k}: {normalise_value(v)}" for k, v in value.items() if normalise_value(v)]
        return ", ".join(items)
    if isinstance(value, list):
        parts = [normalise_value(item) for item in value]
        parts = [part for part in parts if part]
        return "; ".join(parts)
    return ""


def normalise_entry(entry: Mapping[str, object], source: str) -> Exhibitor:
    data: MutableMapping[str, str] = {}
    for field, keys in FIELD_MAP.items():
        data[field] = extract_first(entry, keys)
    return Exhibitor(
        source=source,
        company_name=data["company_name"],
        country=data["country"],
        city=data["city"],
        address=data["address"],
        phone=data["phone"],
        email=data["email"],
        website=data["website"],
        hall=data["hall"],
        stand=data["stand"],
        brands=data["brands"],
    )


def scrape(url: str, retries: int, delay: float) -> List[Exhibitor]:
    html = fetch_html(url, retries=retries, delay=delay)
    exhibitors: List[Exhibitor] = []
    for blob in iter_json_blobs(html):
        data = parse_json(blob)
        if data is None:
            continue
        for entries in discover_exhibitor_lists(data):
            for entry in entries:
                exhibitor = normalise_entry(entry, url)
                if exhibitor.company_name:
                    exhibitors.append(exhibitor)
        if exhibitors:
            break
    if not exhibitors:
        LOGGER.warning("No exhibitor data detected in JSON payloads")
    return exhibitors


def ensure_rows(records: Iterable[Exhibitor]) -> List[Dict[str, str]]:
    rows = [record.as_row() for record in records]
    if not rows:
        LOGGER.warning("No data to serialise")
    return rows


def write_csv(path: Path, rows: List[Dict[str, str]]) -> None:
    if not rows:
        LOGGER.warning("No data to write to %s", path)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    LOGGER.info("Wrote %s records to %s", len(rows), path)


def write_excel(path: Path, rows: List[Dict[str, str]]) -> None:
    if not rows:
        LOGGER.warning("No data to write to %s", path)
        return
    try:
        import pandas as pd
    except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "Excel output requires the 'pandas' package. "
            "Install it with 'pip install pandas openpyxl'."
        ) from exc

    dataframe = pd.DataFrame(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_excel(path, index=False)
    LOGGER.info("Wrote %s records to %s", len(rows), path)


def write_output(path: Path, rows: List[Dict[str, str]], fmt: str) -> None:
    if fmt == "csv":
        write_csv(path, rows)
    elif fmt == "xlsx":
        write_excel(path, rows)
    else:  # pragma: no cover - defensive programming
        raise ValueError(f"Unsupported output format: {fmt}")


def detect_format(path: Path, explicit: Optional[str]) -> str:
    if explicit:
        return explicit
    suffix = path.suffix.lower()
    if suffix == ".xlsx":
        return "xlsx"
    return "csv"


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=DEFAULT_URL, help="Listing URL to scrape")
    parser.add_argument(
        "--output",
        default="ceramitec_exhibitors.csv",
        help="Destination file path (.csv or .xlsx)",
    )
    parser.add_argument(
        "--format",
        choices=("csv", "xlsx"),
        help="Override output format (otherwise inferred from file extension)",
    )
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY)
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    configure_logging(args.verbose)
    try:
        exhibitors = scrape(args.url, retries=args.retries, delay=args.delay)
    except RuntimeError as exc:
        LOGGER.error("%s", exc)
        return 1

    if not exhibitors:
        LOGGER.error("No exhibitor entries were extracted")
        return 2

    output_path = Path(args.output)
    rows = ensure_rows(exhibitors)
    if not rows:
        return 2

    output_format = detect_format(output_path, args.format)
    write_output(output_path, rows, output_format)
    return 0


if __name__ == "__main__":  # pragma: no cover - manual invocation
    sys.exit(main())
