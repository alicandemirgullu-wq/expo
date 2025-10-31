"""Minimal scraper for Messe Düsseldorf exhibitor directories.

The catalogue pages served under the `/vis/v1/` path expose their data through a
Nuxt payload that is embedded directly inside the HTML.  This script downloads a
single listing page, extracts the JSON structure, detects exhibitor records and
exports them to both CSV and Excel using only the Python standard library.

The goal is to provide a compact, easy-to-understand alternative to the previous
full-featured scraper while keeping the core functionality intact.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
from collections import deque
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import zipfile


LOGGER = logging.getLogger(__name__)

DEFAULT_PAGE_URL = "https://www.caravan-salon.com/vis/v1/en/exhprofiles/"
DEFAULT_OUTPUT_CSV = "messe_dusseldorf_exhibitors.csv"
DEFAULT_OUTPUT_XLSX = "messe_dusseldorf_exhibitors.xlsx"
DEFAULT_USER_AGENT = "MesseDusseldorfSimpleScraper/1.0 (+https://github.com/openai/)"
OUTPUT_HEADERS = [
    "Company Name",
    "Profile URL",
    "Address",
    "Country",
    "Phone",
    "Email",
    "Website",
    "Hall",
    "Stand",
    "Categories",
    "Source Page",
]
JSON_MARKERS = [
    "window.__NUXT__=",
    "window.__INITIAL_STATE__=",
    "window.__DATA__=",
    "var __NUXT__ =",
]
CANDIDATE_KEYWORDS = (
    "company",
    "exhibitor",
    "firma",
    "organisation",
    "participant",
    "name",
)

FIELD_KEYWORDS = {
    "Company Name": ["company", "firma", "exhibitor", "name", "title"],
    "Profile URL": ["profile", "detail", "url", "href", "link"],
    "Address": ["address", "street"],
    "Country": ["country"],
    "Phone": ["phone", "tel"],
    "Email": ["email", "mail"],
    "Website": ["website", "web", "url"],
    "Hall": ["hall"],
    "Stand": ["stand", "booth"],
    "Categories": ["category", "product"],
}


# ---------------------------------------------------------------------------
# Networking helpers
# ---------------------------------------------------------------------------

def fetch_page(url: str, user_agent: str) -> str:
    """Download a single web page and return its decoded body."""

    request = Request(url, headers={"User-Agent": user_agent})
    with urlopen(request, timeout=60) as response:  # nosec B310 - simple fetch
        content_type = response.headers.get_content_charset() or "utf-8"
        return response.read().decode(content_type, errors="replace")


# ---------------------------------------------------------------------------
# JSON extraction and traversal
# ---------------------------------------------------------------------------

def extract_json_payload(html: str) -> Optional[Any]:
    """Find the first JSON blob injected by Nuxt and decode it."""

    for marker in JSON_MARKERS:
        start = html.find(marker)
        if start == -1:
            continue
        start += len(marker)
        raw_json = _read_json_structure(html, start)
        if not raw_json:
            continue
        try:
            return json.loads(raw_json)
        except json.JSONDecodeError as exc:  # pragma: no cover - defensive
            LOGGER.debug("Failed to decode JSON for marker %s: %s", marker, exc)
    return None


def _read_json_structure(text: str, start_index: int) -> Optional[str]:
    """Extract a balanced JSON object/array starting from ``start_index``."""

    length = len(text)
    while start_index < length and text[start_index].isspace():
        start_index += 1

    if start_index >= length or text[start_index] not in "{[":
        return None

    opening = text[start_index]
    closing = "}" if opening == "{" else "]"
    depth = 0
    in_string = False
    escape = False
    index = start_index

    while index < length:
        char = text[index]
        if in_string:
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == "\"":
                in_string = False
        else:
            if char == "\"":
                in_string = True
            elif char == opening:
                depth += 1
            elif char == closing:
                depth -= 1
                if depth == 0:
                    return text[start_index : index + 1]
        index += 1

    return None


def find_exhibitor_dicts(payload: Any) -> List[Dict[str, Any]]:
    """Locate the list of exhibitor dictionaries within the decoded payload."""

    queue: deque[Any] = deque([payload])
    best_match: List[Dict[str, Any]] = []

    while queue:
        current = queue.popleft()
        if isinstance(current, dict):
            queue.extend(current.values())
        elif isinstance(current, list):
            if current and all(isinstance(item, dict) for item in current):
                keys = set()
                for item in current[:3]:
                    keys.update(key.lower() for key in item.keys())
                if any(
                    keyword in key
                    for key in keys
                    for keyword in CANDIDATE_KEYWORDS
                ):
                    if len(current) > len(best_match):
                        best_match = current
            queue.extend(current)
    return best_match


# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------

def normalise_exhibitors(
    exhibitors: Iterable[Dict[str, Any]], page_url: str, limit: Optional[int] = None
) -> List[Dict[str, str]]:
    """Convert raw dictionaries into a uniform row structure."""

    rows: List[Dict[str, str]] = []
    for index, entry in enumerate(exhibitors):
        if limit is not None and index >= limit:
            break

        row: Dict[str, str] = {header: "" for header in OUTPUT_HEADERS}
        for header, keywords in FIELD_KEYWORDS.items():
            value = _search_for_value(entry, keywords)
            if value:
                row[header] = value
        row["Source Page"] = page_url
        rows.append(row)

    return rows


def _search_for_value(data: Any, keywords: Sequence[str]) -> str:
    """Search ``data`` recursively for the first value matching ``keywords``."""

    if isinstance(data, dict):
        for key, value in data.items():
            lower_key = key.lower()
            if any(keyword in lower_key for keyword in keywords):
                normalised = _stringify(value)
                if normalised:
                    return normalised
            nested = _search_for_value(value, keywords)
            if nested:
                return nested
    elif isinstance(data, list):
        for item in data:
            nested = _search_for_value(item, keywords)
            if nested:
                return nested
    return ""


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        parts = [_stringify(item) for item in value.values()]
        parts = [part for part in parts if part]
        return ", ".join(dict.fromkeys(parts))
    if isinstance(value, list):
        parts = [_stringify(item) for item in value]
        parts = [part for part in parts if part]
        return ", ".join(dict.fromkeys(parts))
    return ""


# ---------------------------------------------------------------------------
# Export helpers
# ---------------------------------------------------------------------------

def save_to_csv(path: Path, rows: Iterable[Dict[str, str]]) -> None:
    data = [
        {header: row.get(header, "") for header in OUTPUT_HEADERS}
        for row in rows
    ]

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_HEADERS)
        writer.writeheader()
        if data:
            writer.writerows(data)


def save_to_excel(path: Path, rows: Iterable[Dict[str, str]]) -> None:
    data = [
        {header: row.get(header, "") for header in OUTPUT_HEADERS}
        for row in rows
    ]
    sheet_xml = _build_sheet_xml(OUTPUT_HEADERS, data)

    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", _CONTENT_TYPES_XML)
        archive.writestr("_rels/.rels", _RELS_XML)
        archive.writestr("docProps/app.xml", _APP_XML)
        archive.writestr("docProps/core.xml", _CORE_XML)
        archive.writestr("xl/_rels/workbook.xml.rels", _WORKBOOK_RELS_XML)
        archive.writestr("xl/workbook.xml", _build_workbook_xml())
        archive.writestr("xl/worksheets/sheet1.xml", sheet_xml)


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
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\"", "&quot;")
        .replace("'", "&apos;")
    )


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
    "<Properties xmlns=\"http://schemas.openxmlformats.org/officeDocument/2006/extended-properties\" xmlns:vt=\"http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes\">"
    "<Application>Python</Application>"
    "</Properties>"
)

_CORE_XML = (
    "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
    "<cp:coreProperties xmlns:cp=\"http://schemas.openxmlformats.org/package/2006/metadata/core-properties\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\" xmlns:dcmitype=\"http://purl.org/dc/dcmitype/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">"
    "<dc:creator>MesseDusseldorfSimpleScraper</dc:creator>"
    "<cp:lastModifiedBy>MesseDusseldorfSimpleScraper</cp:lastModifiedBy>"
    "<dcterms:created xsi:type=\"dcterms:W3CDTF\"></dcterms:created>"
    "<dcterms:modified xsi:type=\"dcterms:W3CDTF\"></dcterms:modified>"
    "</cp:coreProperties>"
)


# ---------------------------------------------------------------------------
# Command-line interface
# ---------------------------------------------------------------------------

def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download a Messe Düsseldorf exhibitor list and export CSV/XLSX files."
        )
    )
    parser.add_argument("--page-url", default=DEFAULT_PAGE_URL, help="Listing page URL")
    parser.add_argument(
        "--output-csv",
        default=DEFAULT_OUTPUT_CSV,
        help="Filename for the CSV export",
    )
    parser.add_argument(
        "--output-xlsx",
        default=DEFAULT_OUTPUT_XLSX,
        help="Filename for the Excel export",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional maximum number of exhibitors to export",
    )
    parser.add_argument(
        "--user-agent",
        default=DEFAULT_USER_AGENT,
        help="Custom User-Agent header for HTTP requests",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )
    return parser.parse_args(argv)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(levelname)s: %(message)s",
    )

    try:
        html = fetch_page(args.page_url, args.user_agent)
        LOGGER.info("Downloaded %s (%d characters)", args.page_url, len(html))
    except (HTTPError, URLError) as exc:
        LOGGER.error("Failed to download %s: %s", args.page_url, exc)
        return 1

    payload = extract_json_payload(html)
    if payload is None:
        LOGGER.error("Could not find an embedded JSON payload on the page")
        return 1

    exhibitor_dicts = find_exhibitor_dicts(payload)
    if not exhibitor_dicts:
        LOGGER.error("No exhibitor records discovered in the payload")
        return 1

    rows = normalise_exhibitors(exhibitor_dicts, args.page_url, args.limit)
    if not rows:
        LOGGER.warning("No rows produced after normalisation")

    csv_path = Path(args.output_csv)
    save_to_csv(csv_path, rows)
    LOGGER.info("Saved CSV export to %s", csv_path)

    xlsx_path = Path(args.output_xlsx)
    save_to_excel(xlsx_path, rows)
    LOGGER.info("Saved Excel export to %s", xlsx_path)

    LOGGER.info("Finished with %d exhibitors", len(rows))
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI behaviour
    raise SystemExit(main())
