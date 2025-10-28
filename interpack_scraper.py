"""Scraper for the Expomed Istanbul exhibitor directory.

This module targets https://expomedistanbul.com/katilimci-listesi and attempts to
discover exhibitor information with minimal third-party dependencies.  The
implementation is intentionally resilient: it prefers the site's WordPress REST
API when available, falls back to JSON blobs embedded in the HTML and, as a last
resort, inspects exhibitor cards rendered on the page.

All results are normalised into a flat structure and exported both as CSV and
Excel files.  The Excel writer is implemented from scratch using only the Python
standard library so the script remains completely self-contained.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import time
import unicodedata
from dataclasses import dataclass
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin
from urllib.request import Request, urlopen
import zipfile


logger = logging.getLogger(__name__)


DEFAULT_PAGE_URL = "https://expomedistanbul.com/katilimci-listesi"
DEFAULT_API_ROOT = "https://expomedistanbul.com/wp-json"
DEFAULT_CSV = "expomed_exhibitors.csv"
DEFAULT_XLSX = "expomed_exhibitors.xlsx"
EXPORT_HEADERS = [
    "Source",
    "Company Name",
    "Address",
    "City",
    "Country",
    "Phone",
    "Fax",
    "Email",
    "Website",
    "Hall",
    "Stand",
    "Other Fields",
]
REQUEST_DELAY = 0.2
REQUEST_RETRIES = 3
REQUEST_TIMEOUT = 45

NAME_PATTERNS = [
    "company_name",
    "firma_unvani",
    "firma_adi",
    "firma",
    "company",
    "title.rendered",
    "title",
    "name",
]
NAME_EXCLUDES = ["contact", "authorized", "responsible", "yetkili", "person"]

ADDRESS_PATTERNS = [
    "address",
    "adres",
    "street",
    "sokak",
    "cadde",
    "mahalle",
    "district",
    "posta",
    "zip",
    "post",
    "pobox",
]
ADDRESS_EXCLUDES = ["email", "mail", "e-mail", "web", "http"]

CITY_PATTERNS = ["city", "şehir", "sehir", "il", "town"]
COUNTRY_PATTERNS = ["country", "ülke", "ulke", "nation"]
PHONE_PATTERNS = ["phone", "telefon", "tel", "gsm", "mobile"]
FAX_PATTERNS = ["fax"]
EMAIL_PATTERNS = ["email", "mail"]
WEBSITE_PATTERNS = ["website", "web", "url", "site"]
HALL_PATTERNS = ["hall", "salon"]
STAND_PATTERNS = ["stand", "booth"]

JSON_MARKERS = [
    "window.__NUXT__=",
    "window.__INITIAL_STATE__=",
    "window.__DATA__=",
    "var __NUXT__ =",
    "var nuxtState =",
    "var appData =",
]

CARD_KEYWORDS = ["exhibitor", "participant", "company", "firma", "katılımc"]


@dataclass
class ScraperConfig:
    """Runtime configuration for the scraper."""

    page_url: str = DEFAULT_PAGE_URL
    api_root: str = DEFAULT_API_ROOT
    output_csv: str = DEFAULT_CSV
    output_xlsx: str = DEFAULT_XLSX
    delay: float = REQUEST_DELAY
    retries: int = REQUEST_RETRIES
    user_agent: str = (
        "ExpomedScraper/1.0 (+https://github.com/openai/)"
    )


@dataclass
class ExhibitorRecord:
    """Normalised exhibitor information."""

    source: str
    company_name: str = ""
    address: str = ""
    city: str = ""
    country: str = ""
    phone: str = ""
    fax: str = ""
    email: str = ""
    website: str = ""
    hall: str = ""
    stand: str = ""
    other_fields: str = ""

    def as_row(self) -> Dict[str, str]:
        values = {
            "Source": self.source,
            "Company Name": self.company_name,
            "Address": self.address,
            "City": self.city,
            "Country": self.country,
            "Phone": self.phone,
            "Fax": self.fax,
            "Email": self.email,
            "Website": self.website,
            "Hall": self.hall,
            "Stand": self.stand,
            "Other Fields": self.other_fields,
        }
        # Ensure all expected headers are present even if the dataclass changes
        # in the future.  Missing keys would break CSV/XLSX exports, so default to
        # empty strings.
        for header in EXPORT_HEADERS:
            values.setdefault(header, "")
        return values


class ExpomedScraper:
    """Scrape exhibitor data from the Expomed Istanbul website."""

    def __init__(self, config: ScraperConfig) -> None:
        self.config = config
        self._page_html: Optional[str] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def collect(self) -> List[ExhibitorRecord]:
        """Collect exhibitor records using all available strategies."""

        logger.info("Collecting exhibitor data from %s", self.config.page_url)

        records = self._collect_via_wp_rest()
        if records:
            logger.info("Collected %s entries via WordPress REST API", len(records))
            return records

        logger.info("Falling back to embedded JSON parsing")
        records = self._collect_from_embedded_json()
        if records:
            logger.info("Collected %s entries from embedded JSON", len(records))
            return records

        logger.info("Falling back to HTML card parsing")
        records = self._collect_from_html_cards()
        if records:
            logger.info("Collected %s entries from HTML cards", len(records))
        else:
            logger.warning("No exhibitor entries detected")
        return records

    # ------------------------------------------------------------------
    # WordPress REST API discovery
    # ------------------------------------------------------------------
    def _collect_via_wp_rest(self) -> List[ExhibitorRecord]:
        """Try to collect data using the WordPress REST API."""

        try:
            types_url = urljoin(self.config.api_root.rstrip("/") + "/", "wp/v2/types")
            types_payload = self._fetch_json(types_url)
        except Exception as exc:  # pragma: no cover - network/HTTP errors
            logger.debug("Failed to load post types: %s", exc, exc_info=True)
            return []

        if not isinstance(types_payload, dict):
            return []

        candidates = self._rank_post_types(types_payload)
        records: List[ExhibitorRecord] = []

        for slug in candidates:
            logger.debug("Trying WordPress post type '%s'", slug)
            posts = self._fetch_all_posts(slug)
            if not posts:
                continue
            for post in posts:
                record = self._normalise(post, f"wp_rest:{slug}")
                if record:
                    records.append(record)
            if records:
                break
        return records

    def _rank_post_types(self, types_payload: Dict[str, Any]) -> List[str]:
        """Score available post types and return candidates sorted by relevance."""

        preferred_order = ["exhibitor", "participant", "katilimci", "company", "firma"]
        keywords = ["exhibitor", "participant", "company", "firma", "supplier", "katılımc"]
        scored: List[Tuple[int, int, str]] = []

        for slug, data in types_payload.items():
            text_parts = [slug]
            if isinstance(data, dict):
                for key in ("name", "description", "labels"):
                    value = data.get(key)
                    if isinstance(value, dict):
                        text_parts.extend(str(v) for v in value.values())
                    elif value is not None:
                        text_parts.append(str(value))
            text = " ".join(text_parts).lower()
            score = sum(5 for keyword in keywords if keyword in text)
            if not score:
                continue
            try:
                order = preferred_order.index(slug.lower())
            except ValueError:
                order = len(preferred_order)
            scored.append((score, -order, slug))

        scored.sort(reverse=True)
        return [slug for _, _, slug in scored]

    def _fetch_all_posts(self, post_type: str) -> List[Dict[str, Any]]:
        """Fetch all items of a given WordPress post type."""

        per_page = 100
        page = 1
        items: List[Dict[str, Any]] = []

        while True:
            params = urlencode({"per_page": per_page, "page": page, "_embed": ""})
            url = urljoin(
                self.config.api_root.rstrip("/") + "/",
                f"wp/v2/{post_type}?{params}",
            )
            try:
                payload = self._fetch_json(url)
            except HTTPError as exc:  # pragma: no cover - network/HTTP errors
                if exc.code == 404:
                    break
                raise
            if not payload:
                break
            if isinstance(payload, list):
                items.extend(payload)
                if len(payload) < per_page:
                    break
            else:
                break
            page += 1
        return items

    # ------------------------------------------------------------------
    # Embedded JSON fallback
    # ------------------------------------------------------------------
    def _collect_from_embedded_json(self) -> List[ExhibitorRecord]:
        try:
            html = self._get_page_html()
        except Exception as exc:  # pragma: no cover - network/HTTP errors
            logger.debug("Failed to download page HTML: %s", exc, exc_info=True)
            return []
        if not html:
            return []

        records: List[ExhibitorRecord] = []
        json_objects = self._extract_json_objects(html)
        for obj in json_objects:
            candidate_list = self._find_candidate_records(obj)
            if not candidate_list:
                continue
            for item in candidate_list:
                record = self._normalise(item, "embedded_json")
                if record:
                    records.append(record)
            if records:
                break
        return records

    def _get_page_html(self) -> str:
        if self._page_html is None:
            logger.debug("Downloading exhibitor list page")
            self._page_html = self._fetch_text(self.config.page_url)
        return self._page_html

    def _extract_json_objects(self, html: str) -> List[Any]:
        objects: List[Any] = []

        # Extract from <script type="application/json"> blocks
        for match in re.finditer(
            r"<script[^>]+type=['\"]application/json['\"][^>]*>(.*?)</script>",
            html,
            re.DOTALL | re.IGNORECASE,
        ):
            snippet = unescape(match.group(1).strip())
            if not snippet:
                continue
            try:
                objects.append(json.loads(snippet))
            except json.JSONDecodeError:
                logger.debug("Failed to decode application/json script block")

        # Extract from assignment markers such as window.__NUXT__
        for marker in JSON_MARKERS:
            start = 0
            while True:
                idx = html.find(marker, start)
                if idx == -1:
                    break
                snippet = self._extract_balanced_json(html[idx + len(marker) :])
                if snippet:
                    try:
                        objects.append(json.loads(snippet))
                    except json.JSONDecodeError:
                        logger.debug("Failed to decode JSON for marker %s", marker)
                    start = idx + len(marker) + len(snippet)
                else:
                    start = idx + len(marker)
        return objects

    def _extract_balanced_json(self, text: str) -> Optional[str]:
        idx = 0
        length = len(text)
        while idx < length and text[idx] in "\n\r \t":
            idx += 1
        if idx >= length or text[idx] not in "[{":
            return None

        open_char = text[idx]
        close_char = "}" if open_char == "{" else "]"
        depth = 0
        in_string = False
        escape_next = False

        for pos in range(idx, length):
            char = text[pos]
            if in_string:
                if escape_next:
                    escape_next = False
                elif char == "\\":
                    escape_next = True
                elif char == "\"":
                    in_string = False
            else:
                if char == "\"":
                    in_string = True
                elif char == open_char:
                    depth += 1
                elif char == close_char:
                    depth -= 1
                    if depth == 0:
                        return text[idx : pos + 1]
        return None

    def _find_candidate_records(self, obj: Any) -> List[Dict[str, Any]]:
        best: List[Dict[str, Any]] = []
        best_score = 0

        def evaluate(record: Dict[str, Any]) -> int:
            score = 0
            for key in record.keys():
                key_lower = str(key).lower()
                if any(term in key_lower for term in NAME_PATTERNS):
                    score += 3
                if any(term in key_lower for term in ADDRESS_PATTERNS):
                    score += 3
                if any(term in key_lower for term in ("city", "country", "email", "phone", "stand")):
                    score += 1
            return score

        def walk(value: Any) -> None:
            nonlocal best, best_score
            if isinstance(value, list):
                if value and all(isinstance(item, dict) for item in value):
                    score = sum(evaluate(item) for item in value[: min(5, len(value))])
                    if score > best_score:
                        best = value
                        best_score = score
                for item in value:
                    walk(item)
            elif isinstance(value, dict):
                for nested in value.values():
                    walk(nested)

        walk(obj)
        return best

    # ------------------------------------------------------------------
    # HTML fallback
    # ------------------------------------------------------------------
    def _collect_from_html_cards(self) -> List[ExhibitorRecord]:
        try:
            html = self._get_page_html()
        except Exception as exc:  # pragma: no cover - network/HTTP errors
            logger.debug("Failed to download page HTML: %s", exc, exc_info=True)
            return []
        if not html:
            return []

        parser = _CardParser(CARD_KEYWORDS)
        parser.feed(html)

        records: List[ExhibitorRecord] = []
        for card_text in parser.cards:
            cleaned = [line.strip() for line in card_text.splitlines() if line.strip()]
            if not cleaned:
                continue
            name = cleaned[0]
            address = " | ".join(cleaned[1:]) if len(cleaned) > 1 else ""
            records.append(
                ExhibitorRecord(
                    source="html_card",
                    company_name=name,
                    address=address,
                    other_fields="",
                )
            )
        return records

    # ------------------------------------------------------------------
    # Normalisation helpers
    # ------------------------------------------------------------------
    def _normalise(self, data: Any, source: str) -> Optional[ExhibitorRecord]:
        if not isinstance(data, dict):
            return None
        flat = self._flatten_dict(data)

        used_keys: List[str] = []

        def pick_one(patterns: Sequence[str], excludes: Sequence[str] | None = None) -> Tuple[str, Optional[str]]:
            return self._find_value(flat, patterns, excludes, used_keys)

        def pick_many(patterns: Sequence[str], excludes: Sequence[str] | None = None, limit: Optional[int] = None) -> Tuple[str, List[str]]:
            return self._collect_values(flat, patterns, excludes, limit, used_keys)

        name, name_key = pick_one(NAME_PATTERNS, NAME_EXCLUDES)
        address, address_keys = pick_many(ADDRESS_PATTERNS, ADDRESS_EXCLUDES)
        city, city_key = pick_one(CITY_PATTERNS)
        country, country_key = pick_one(COUNTRY_PATTERNS)
        phone, phone_key = pick_one(PHONE_PATTERNS)
        fax, fax_key = pick_one(FAX_PATTERNS)
        email, email_key = pick_one(EMAIL_PATTERNS)
        website, website_key = pick_one(WEBSITE_PATTERNS, ["email"])
        hall, hall_key = pick_one(HALL_PATTERNS)
        stand, stand_key = pick_one(STAND_PATTERNS)

        used_keys.extend(
            key
            for key in (
                name_key,
                city_key,
                country_key,
                phone_key,
                fax_key,
                email_key,
                website_key,
                hall_key,
                stand_key,
            )
            if key
        )
        used_keys.extend(address_keys)

        other_payload = {
            key: self._stringify(value)
            for key, value in flat.items()
            if key not in used_keys and self._stringify(value)
        }

        other_fields = json.dumps(other_payload, ensure_ascii=False) if other_payload else ""

        if not any([name, address, city, country, phone, email]):
            return None

        return ExhibitorRecord(
            source=source,
            company_name=name,
            address=address,
            city=city,
            country=country,
            phone=phone,
            fax=fax,
            email=email,
            website=website,
            hall=hall,
            stand=stand,
            other_fields=other_fields,
        )

    def _flatten_dict(self, value: Any, prefix: str | None = None) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        if isinstance(value, dict):
            for key, item in value.items():
                new_prefix = f"{prefix}.{key}" if prefix else str(key)
                result.update(self._flatten_dict(item, new_prefix))
        elif isinstance(value, list):
            if all(isinstance(item, (str, int, float, bool)) or item is None for item in value):
                joined = ", ".join(self._stringify(item) for item in value if item not in (None, ""))
                if joined:
                    result[prefix or "value"] = joined
            else:
                for index, item in enumerate(value):
                    new_prefix = f"{prefix}[{index}]" if prefix else f"[{index}]"
                    result.update(self._flatten_dict(item, new_prefix))
        else:
            if prefix:
                result[prefix] = value
        return result

    def _find_value(
        self,
        flat: Dict[str, Any],
        patterns: Sequence[str],
        excludes: Sequence[str] | None,
        used_keys: List[str],
    ) -> Tuple[str, Optional[str]]:
        best_key: Optional[str] = None
        best_value: str = ""
        best_score: Optional[Tuple[int, int]] = None

        for key, raw_value in flat.items():
            if key in used_keys:
                continue
            text = self._stringify(raw_value)
            if not text:
                continue
            lower_key = key.lower()
            if excludes and any(ex in lower_key for ex in excludes):
                continue
            for priority, pattern in enumerate(patterns):
                lower_pattern = pattern.lower()
                if lower_key == lower_pattern:
                    score = (priority, 0)
                elif lower_key.endswith(lower_pattern):
                    score = (priority, 1)
                elif lower_pattern in lower_key:
                    score = (priority, 2)
                else:
                    continue
                if best_score is None or score < best_score:
                    best_score = score
                    best_key = key
                    best_value = text
                break
        if best_key:
            used_keys.append(best_key)
        return best_value, best_key

    def _collect_values(
        self,
        flat: Dict[str, Any],
        patterns: Sequence[str],
        excludes: Sequence[str] | None,
        limit: Optional[int],
        used_keys: List[str],
    ) -> Tuple[str, List[str]]:
        values: List[str] = []
        keys: List[str] = []

        for key, raw_value in flat.items():
            if key in used_keys:
                continue
            text = self._stringify(raw_value)
            if not text:
                continue
            lower_key = key.lower()
            if excludes and any(ex in lower_key for ex in excludes):
                continue
            if any(pattern.lower() in lower_key for pattern in patterns):
                if text not in values:
                    values.append(text)
                    keys.append(key)
                    if limit and len(values) >= limit:
                        break
        for key in keys:
            used_keys.append(key)
        return " | ".join(values), keys

    def _stringify(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return self._normalise_whitespace(self._strip_html(value.strip()))
        if isinstance(value, bool):
            return "Yes" if value else "No"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, list):
            joined = ", ".join(self._stringify(item) for item in value if item not in (None, ""))
            return joined
        return self._normalise_whitespace(str(value))

    def _strip_html(self, text: str) -> str:
        if "<" not in text:
            return text
        text = re.sub(r"<\s*br\s*/?>", "\n", text, flags=re.IGNORECASE)
        text = re.sub(r"<[^>]+>", " ", text)
        return text

    def _normalise_whitespace(self, text: str) -> str:
        normalized = unicodedata.normalize("NFKC", text)
        normalized = re.sub(r"\s+", " ", normalized)
        return normalized.strip()

    # ------------------------------------------------------------------
    # HTTP utilities
    # ------------------------------------------------------------------
    def _fetch_json(self, url: str) -> Any:
        response_text = self._fetch_text(url, accept="application/json")
        if not response_text:
            return None
        try:
            return json.loads(response_text)
        except json.JSONDecodeError:
            logger.debug("Failed to decode JSON from %s", url)
            return None

    def _fetch_text(self, url: str, accept: Optional[str] = None) -> str:
        last_error: Optional[Exception] = None
        headers = {"User-Agent": self.config.user_agent}
        if accept:
            headers["Accept"] = accept

        for attempt in range(1, self.config.retries + 1):
            request = Request(url, headers=headers)
            try:
                with urlopen(request, timeout=REQUEST_TIMEOUT) as response:
                    charset = response.headers.get_content_charset() or "utf-8"
                    return response.read().decode(charset, errors="replace")
            except (HTTPError, URLError, TimeoutError) as exc:
                last_error = exc
                logger.debug(
                    "Request failed for %s (attempt %s/%s): %s",
                    url,
                    attempt,
                    self.config.retries,
                    exc,
                )
                time.sleep(self.config.delay)
        if last_error:
            raise last_error
        return ""


class _CardParser(HTMLParser):
    """Extract text blocks from exhibitor card-like elements."""

    def __init__(self, keywords: Sequence[str]):
        super().__init__()
        self.keywords = [keyword.lower() for keyword in keywords]
        self.cards: List[str] = []
        self._collecting = False
        self._depth = 0
        self._buffer: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        attrs_dict = {name: value or "" for name, value in attrs}
        classes = attrs_dict.get("class", "").lower()
        data_type = attrs_dict.get("data-type", "").lower()
        matches = any(keyword in classes or keyword in data_type for keyword in self.keywords)

        if self._collecting:
            self._depth += 1
        elif matches:
            self._collecting = True
            self._depth = 1
            self._buffer = []

    def handle_endtag(self, tag: str) -> None:
        if not self._collecting:
            return
        self._depth -= 1
        if self._depth == 0:
            text = self._normalise_text("".join(self._buffer))
            if text:
                self.cards.append(text)
            self._collecting = False
            self._buffer = []

    def handle_data(self, data: str) -> None:
        if self._collecting:
            self._buffer.append(data)

    def handle_startendtag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        if self._collecting and tag.lower() == "br":
            self._buffer.append("\n")

    def _normalise_text(self, text: str) -> str:
        text = re.sub(r"\s+", " ", text)
        return text.strip()


# ----------------------------------------------------------------------
# Excel writer utilities
# ----------------------------------------------------------------------

def save_as_excel(path: Path, rows: Iterable[Dict[str, str]]) -> None:
    """Write rows to an XLSX file using only the standard library."""

    rows = [
        {header: row.get(header, "") for header in EXPORT_HEADERS}
        for row in rows
    ]
    headers: List[str] = list(EXPORT_HEADERS)

    sheet_xml = _build_sheet_xml(headers, rows)

    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", _CONTENT_TYPES_XML)
        zf.writestr("_rels/.rels", _RELS_XML)
        zf.writestr("docProps/app.xml", _APP_XML)
        zf.writestr("docProps/core.xml", _CORE_XML)
        zf.writestr("xl/_rels/workbook.xml.rels", _WORKBOOK_RELS_XML)
        zf.writestr("xl/workbook.xml", _build_workbook_xml())
        zf.writestr("xl/worksheets/sheet1.xml", sheet_xml)


def _build_sheet_xml(headers: List[str], rows: List[Dict[str, str]]) -> str:
    def cell_ref(column: int, row: int) -> str:
        return f"{_column_letter(column)}{row}"

    cells: List[str] = []
    row_index = 1

    if headers:
        header_cells = []
        for col_index, header in enumerate(headers, start=1):
            header_cells.append(
                f'<c r="{cell_ref(col_index, row_index)}" t="inlineStr"><is><t>{_escape_xml(header)}</t></is></c>'
            )
        cells.append(f"<row r=\"{row_index}\">{''.join(header_cells)}</row>")
        row_index += 1

    for row_data in rows:
        data_cells = []
        for col_index, header in enumerate(headers, start=1):
            value = row_data.get(header, "")
            if value is None:
                value = ""
            data_cells.append(
                f'<c r="{cell_ref(col_index, row_index)}" t="inlineStr"><is><t>{_escape_xml(value)}</t></is></c>'
            )
        cells.append(f"<row r=\"{row_index}\">{''.join(data_cells)}</row>")
        row_index += 1

    sheet_data = "".join(cells)
    return (
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        "<worksheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\""
        " xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\">"
        f"<sheetData>{sheet_data}</sheetData>"
        "</worksheet>"
    )


def _build_workbook_xml() -> str:
    return (
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        "<workbook xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\""
        " xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\">"
        "<sheets><sheet name=\"Exhibitors\" sheetId=\"1\" r:id=\"rId1\"/></sheets>"
        "</workbook>"
    )


def _column_letter(index: int) -> str:
    letters = ""
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters


def _escape_xml(value: str) -> str:
    value = value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    value = value.replace("\"", "&quot;").replace("'", "&apos;")
    return value


_CONTENT_TYPES_XML = (
    "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
    "<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\">"
    "<Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/>"
    "<Default Extension=\"xml\" ContentType=\"application/xml\"/>"
    "<Override PartName=\"/xl/workbook.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/>"
    "<Override PartName=\"/xl/worksheets/sheet1.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/>"
    "<Override PartName=\"/docProps/core.xml\" ContentType=\"application/vnd.openxmlformats-package.core-properties+xml\"/>"
    "<Override PartName=\"/docProps/app.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.extended-properties+xml\"/>"
    "</Types>"
)

_RELS_XML = (
    "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
    "<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">"
    "<Relationship Id=\"rId1\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument\" Target=\"xl/workbook.xml\"/>"
    "<Relationship Id=\"rId2\" Type=\"http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties\" Target=\"docProps/core.xml\"/>"
    "<Relationship Id=\"rId3\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties\" Target=\"docProps/app.xml\"/>"
    "</Relationships>"
)

_WORKBOOK_RELS_XML = (
    "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
    "<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">"
    "<Relationship Id=\"rId1\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet\" Target=\"worksheets/sheet1.xml\"/>"
    "</Relationships>"
)

_APP_XML = (
    "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
    "<Properties xmlns=\"http://schemas.openxmlformats.org/officeDocument/2006/extended-properties\""
    " xmlns:vt=\"http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes\">"
    "<Application>Python</Application>"
    "</Properties>"
)

_CORE_XML = (
    "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
    "<cp:coreProperties xmlns:cp=\"http://schemas.openxmlformats.org/package/2006/metadata/core-properties\""
    " xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\""
    " xmlns:dcmitype=\"http://purl.org/dc/dcmitype/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">"
    "<dc:creator>ExpomedScraper</dc:creator>"
    "<cp:lastModifiedBy>ExpomedScraper</cp:lastModifiedBy>"
    "<dcterms:created xsi:type=\"dcterms:W3CDTF\"></dcterms:created>"
    "<dcterms:modified xsi:type=\"dcterms:W3CDTF\"></dcterms:modified>"
    "</cp:coreProperties>"
)


# ----------------------------------------------------------------------
# Command line interface
# ----------------------------------------------------------------------


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download exhibitor contact information from Expomed Istanbul and export to CSV/Excel."
        )
    )
    parser.add_argument("--page-url", default=DEFAULT_PAGE_URL, help="Exhibitor list page URL")
    parser.add_argument(
        "--api-root",
        default=DEFAULT_API_ROOT,
        help="WordPress REST API root (used for discovery)",
    )
    parser.add_argument("--output-csv", default=DEFAULT_CSV, help="Output CSV filename")
    parser.add_argument("--output-xlsx", default=DEFAULT_XLSX, help="Output XLSX filename")
    parser.add_argument(
        "--delay",
        type=float,
        default=REQUEST_DELAY,
        help="Delay between retry attempts (seconds)",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=REQUEST_RETRIES,
        help="Maximum retry attempts for network requests",
    )
    parser.add_argument(
        "--user-agent",
        default="ExpomedScraper/1.0 (+https://github.com/openai/)",
        help="Custom User-Agent header",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable verbose debug logging for troubleshooting",
    )
    return parser.parse_args(argv)


def save_to_csv(path: Path, rows: Iterable[Dict[str, str]]) -> None:
    normalised_rows = [
        {header: row.get(header, "") for header in EXPORT_HEADERS}
        for row in rows
    ]

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=EXPORT_HEADERS)
        writer.writeheader()
        if normalised_rows:
            writer.writerows(normalised_rows)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if getattr(args, "debug", False) else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    config = ScraperConfig(
        page_url=args.page_url,
        api_root=args.api_root,
        output_csv=args.output_csv,
        output_xlsx=args.output_xlsx,
        delay=args.delay,
        retries=args.retries,
        user_agent=args.user_agent,
    )

    scraper = ExpomedScraper(config)
    records = scraper.collect()

    rows = [record.as_row() for record in records]

    csv_path = Path(config.output_csv)
    save_to_csv(csv_path, rows)
    logger.info("CSV export written to %s", csv_path.resolve())

    xlsx_path = Path(config.output_xlsx)
    save_as_excel(xlsx_path, rows)
    logger.info("Excel export written to %s", xlsx_path.resolve())

    if not records:
        logger.warning("No exhibitor data collected. Verify site structure or network access.")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

