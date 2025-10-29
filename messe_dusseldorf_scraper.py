"""Scraper for Messe Düsseldorf exhibitor directories.

This module extracts company and contact information from exhibitor lists that
share the structure used by Messe Düsseldorf trade fair websites.  The
implementation follows the behaviour of the public Apify actor described at
https://apify.com/skython/messe-duesse-ldorf-katilimci-liste-kaziyici: it
parses embedded JSON payloads from Nuxt/Vue powered pages, falls back to HTML
card parsing when structured data is unavailable, and finally exports the
results to both CSV and Excel formats without external dependencies.

Two export layouts are supported:

``compact``
    A single row per exhibitor with product categories collapsed into a single
    string column.

``expanded``
    Multiple rows per exhibitor – one for each product category level
    combination – to make filtering in spreadsheet tools easier.

The script intentionally avoids third-party packages so that it can be dropped
into automation environments that only provide the Python standard library.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import time
import unicodedata
import zipfile
from contextlib import closing
from dataclasses import dataclass, field
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import OpenerDirector, ProxyHandler, Request, build_opener, urlopen


logger = logging.getLogger(__name__)


DEFAULT_PAGE_URL = "https://www.caravan-salon.com/vis/v1/en/exhprofiles/"
DEFAULT_CSV = "messe_dusseldorf_exhibitors.csv"
DEFAULT_XLSX = "messe_dusseldorf_exhibitors.xlsx"
REQUEST_DELAY = 0.4
REQUEST_RETRIES = 3
REQUEST_TIMEOUT = 45
SUPPORTED_FORMATS = {"compact", "expanded"}

SOCIAL_NETWORKS = [
    "LinkedIn",
    "Facebook",
    "Instagram",
    "Twitter",
    "Youtube",
    "Tiktok",
]

COMPACT_HEADERS = [
    "Exhibitor Profile URL",
    "Company Name",
    "Company Address",
    "Company Country",
    "Company Phone",
    "Company Email",
    "Company Website",
    "Hall Stands",
    "Main Exhibitor Name",
    "Main Exhibitor Profile URL",
    "Co-Exhibitors",
]
COMPACT_HEADERS.extend(f"Company URL {network}" for network in SOCIAL_NETWORKS)
COMPACT_HEADERS.extend(
    [
        "Contact Person Name",
        "Contact Person Email",
        "Contact Person Phone",
        "Contact Person Position",
        "Contact Person LinkedIn",
        "Contact Person Instagram",
        "Contact Person Facebook",
        "Contact Person Youtube",
        "Product Categories",
    ]
)

EXPANDED_HEADERS = [*COMPACT_HEADERS, "Category Level 1", "Category Level 2"]

JSON_MARKERS = [
    "window.__NUXT__=",
    "window.__INITIAL_STATE__=",
    "window.__DATA__=",
    "var __NUXT__ =",
    "var nuxtState =",
    "var appData =",
]

CARD_KEYWORDS = ["exhibitor", "company", "profile", "card"]

PROFILE_PATTERNS = ["profile_url", "profileurl", "detailurl", "detail_url", "url"]
COMPANY_NAME_PATTERNS = [
    "company_name",
    "companyname",
    "exhibitorname",
    "firma",
    "name",
    "title",
]
ADDRESS_PATTERNS = [
    "address",
    "street",
    "postal",
    "zip",
    "city",
    "state",
    "country",
]
COUNTRY_PATTERNS = ["country", "nation", "land"]
PHONE_PATTERNS = ["phone", "telefon", "tel", "mobile", "gsm"]
EMAIL_PATTERNS = ["email", "mail"]
WEBSITE_PATTERNS = ["website", "homepage", "url", "site"]
HALL_PATTERNS = ["hall"]
STAND_PATTERNS = ["stand", "booth"]
MAIN_EXHIBITOR_PATTERNS = ["main_exhibitor", "mainexhibitor", "parent", "main"]
CO_EXHIBITOR_PATTERNS = ["coexhib", "co-exhib", "co_exhib", "partner"]
CATEGORY_LEVEL1_PATTERNS = ["category_level1", "category_level_1", "category", "segment"]
CATEGORY_LEVEL2_PATTERNS = ["category_level2", "category_level_2", "subcategory", "product"]
POSITION_PATTERNS = ["position", "title", "function", "role", "job"]
CONTACT_FIRSTNAME_PATTERNS = ["first_name", "firstname", "givenname"]
CONTACT_LASTNAME_PATTERNS = ["last_name", "lastname", "surname", "familyname"]

SOCIAL_MARKERS = {
    "linkedin": ["linkedin.com"],
    "facebook": ["facebook.com", "fb.com"],
    "instagram": ["instagram.com"],
    "twitter": ["twitter.com", "x.com"],
    "youtube": ["youtube.com", "youtu.be"],
    "tiktok": ["tiktok.com"],
}


@dataclass
class ScraperConfig:
    """Runtime configuration for the scraper."""

    page_url: str = DEFAULT_PAGE_URL
    output_csv: str = DEFAULT_CSV
    output_xlsx: str = DEFAULT_XLSX
    delay: float = REQUEST_DELAY
    retries: int = REQUEST_RETRIES
    user_agent: str = "MesseDuesseldorfScraper/1.0 (+https://github.com/openai/)"
    output_format: str = "compact"
    proxy: Optional[str] = None


@dataclass
class ContactPerson:
    name: str = ""
    email: str = ""
    phone: str = ""
    position: str = ""
    linkedin: str = ""
    instagram: str = ""
    facebook: str = ""
    youtube: str = ""


@dataclass
class Category:
    level1: str = ""
    level2: str = ""


@dataclass
class ExhibitorRecord:
    source: str
    profile_url: str = ""
    company_name: str = ""
    company_address: str = ""
    company_country: str = ""
    company_phone: str = ""
    company_email: str = ""
    company_website: str = ""
    hall_stands: str = ""
    main_exhibitor_name: str = ""
    main_exhibitor_profile_url: str = ""
    co_exhibitors: List[str] = field(default_factory=list)
    social_links: Dict[str, str] = field(default_factory=dict)
    contacts: List[ContactPerson] = field(default_factory=list)
    categories: List[Category] = field(default_factory=list)

    def to_compact_row(self) -> Dict[str, str]:
        """Represent the exhibitor as a single CSV/XLSX row."""

        row = {
            "Exhibitor Profile URL": self.profile_url,
            "Company Name": self.company_name,
            "Company Address": self.company_address,
            "Company Country": self.company_country,
            "Company Phone": self.company_phone,
            "Company Email": self.company_email,
            "Company Website": self.company_website,
            "Hall Stands": self.hall_stands,
            "Main Exhibitor Name": self.main_exhibitor_name,
            "Main Exhibitor Profile URL": self.main_exhibitor_profile_url,
            "Co-Exhibitors": " | ".join(sorted({name for name in self.co_exhibitors if name})),
        }

        for network in SOCIAL_NETWORKS:
            key = f"Company URL {network}"
            row[key] = self.social_links.get(network.lower(), "")

        contact_names = [contact.name for contact in self.contacts if contact.name]
        contact_emails = [contact.email for contact in self.contacts if contact.email]
        contact_phones = [contact.phone for contact in self.contacts if contact.phone]
        contact_positions = [contact.position for contact in self.contacts if contact.position]
        contact_linkedins = [contact.linkedin for contact in self.contacts if contact.linkedin]
        contact_instagrams = [contact.instagram for contact in self.contacts if contact.instagram]
        contact_facebooks = [contact.facebook for contact in self.contacts if contact.facebook]
        contact_youtubes = [contact.youtube for contact in self.contacts if contact.youtube]

        row.update(
            {
                "Contact Person Name": " | ".join(contact_names),
                "Contact Person Email": " | ".join(contact_emails),
                "Contact Person Phone": " | ".join(contact_phones),
                "Contact Person Position": " | ".join(contact_positions),
                "Contact Person LinkedIn": " | ".join(contact_linkedins),
                "Contact Person Instagram": " | ".join(contact_instagrams),
                "Contact Person Facebook": " | ".join(contact_facebooks),
                "Contact Person Youtube": " | ".join(contact_youtubes),
                "Product Categories": " | ".join(
                    self._format_category(category) for category in self.categories if category.level1 or category.level2
                ),
            }
        )

        return row

    def iter_expanded_rows(self) -> Iterator[Dict[str, str]]:
        """Yield rows expanded by category level."""

        base = self.to_compact_row()
        if self.categories:
            for category in self.categories:
                row = dict(base)
                row["Category Level 1"] = category.level1
                row["Category Level 2"] = category.level2
                yield row
        else:
            row = dict(base)
            row["Category Level 1"] = ""
            row["Category Level 2"] = ""
            yield row

    @staticmethod
    def _format_category(category: Category) -> str:
        if category.level1 and category.level2:
            return f"{category.level1} / {category.level2}"
        return category.level1 or category.level2 or ""


class MesseDuesseldorfScraper:
    """Scrape exhibitor data from Messe Düsseldorf directories."""

    def __init__(self, config: ScraperConfig) -> None:
        self.config = config
        self._page_html: Optional[str] = None
        self._opener = self._build_opener()

    def _build_opener(self) -> Optional[OpenerDirector]:
        if not self.config.proxy:
            return None
        proxy_address = self.config.proxy.strip()
        if not proxy_address:
            return None
        proxies = {"http": proxy_address, "https": proxy_address}
        logger.debug("Using proxy %s", proxy_address)
        return build_opener(ProxyHandler(proxies))

    def collect(self) -> List[ExhibitorRecord]:
        """Collect exhibitor records using embedded JSON and HTML fallbacks."""

        logger.info("Collecting exhibitor data from %s", self.config.page_url)

        records = self._collect_from_embedded_json()
        if records:
            logger.info("Collected %s entries from embedded JSON", len(records))
            return records

        logger.info("Falling back to HTML card parsing")
        records = self._collect_from_html_cards()
        if not records:
            logger.warning("No exhibitor entries detected. Check the input URL.")
        return records

    def diagnose(self) -> Dict[str, Any]:
        """Gather quick diagnostics to aid debugging and stability checks."""

        diagnostics: Dict[str, Any] = {
            "page_url": self.config.page_url,
            "proxy": self.config.proxy or "",
            "user_agent": self.config.user_agent,
            "retries": self.config.retries,
            "delay": self.config.delay,
        }

        download_start = time.perf_counter()
        try:
            html = self._fetch_text(self.config.page_url)
        except Exception as exc:  # pragma: no cover - network errors depend on runtime
            diagnostics.update(
                {
                    "download_success": False,
                    "error": str(exc),
                }
            )
            return diagnostics

        download_elapsed = time.perf_counter() - download_start
        diagnostics.update(
            {
                "download_success": True,
                "download_seconds": round(download_elapsed, 3),
                "download_bytes": len(html.encode("utf-8")),
                "html_characters": len(html),
            }
        )

        self._page_html = html

        parse_start = time.perf_counter()
        json_objects = self._extract_json_objects(html)
        candidate_lengths: List[int] = []
        for obj in json_objects:
            candidates = self._find_candidate_records(obj)
            if candidates:
                candidate_lengths.append(len(candidates))
        json_elapsed = time.perf_counter() - parse_start
        diagnostics.update(
            {
                "embedded_json_objects": len(json_objects),
                "candidate_lengths": candidate_lengths,
                "embedded_json_parse_seconds": round(json_elapsed, 3),
            }
        )

        records = self._collect_from_embedded_json()
        diagnostics["normalised_records"] = len(records)
        if records:
            sample = records[0]
            diagnostics["sample_record"] = {
                "company_name": sample.company_name,
                "profile_url": sample.profile_url,
                "category_count": len(sample.categories),
                "contact_count": len(sample.contacts),
            }

        fallback_start = time.perf_counter()
        fallback_records = self._collect_from_html_cards()
        fallback_elapsed = time.perf_counter() - fallback_start
        diagnostics.update(
            {
                "fallback_records": len(fallback_records),
                "fallback_parse_seconds": round(fallback_elapsed, 3),
            }
        )
        if fallback_records:
            diagnostics.setdefault("sample_fallback_record", {
                "company_name": fallback_records[0].company_name,
                "company_address": fallback_records[0].company_address,
            })

        return diagnostics

    # ------------------------------------------------------------------
    # Embedded JSON parsing
    # ------------------------------------------------------------------
    def _collect_from_embedded_json(self) -> List[ExhibitorRecord]:
        try:
            html = self._get_page_html()
        except Exception as exc:  # pragma: no cover - network/HTTP errors
            logger.debug("Failed to download page HTML: %s", exc, exc_info=True)
            return []
        if not html:
            return []

        json_objects = self._extract_json_objects(html)
        records: List[ExhibitorRecord] = []

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
                elif char == '"':
                    in_string = False
            else:
                if char == '"':
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
                if any(term in key_lower for term in ("company", "exhibitor", "profile")):
                    score += 3
                if any(term in key_lower for term in ("address", "contact", "category")):
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
        except Exception:
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
                    company_address=address,
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

        def pick_many(
            patterns: Sequence[str],
            excludes: Sequence[str] | None = None,
            limit: Optional[int] = None,
        ) -> Tuple[str, List[str]]:
            return self._collect_values(flat, patterns, excludes, limit, used_keys)

        profile_url, _ = pick_one(PROFILE_PATTERNS, ["image", "logo", "thumb"])
        company_name, _ = pick_one(COMPANY_NAME_PATTERNS, ["contact", "person", "author"])
        address, _ = pick_many(ADDRESS_PATTERNS, ["email", "mail", "http"], None)
        country, _ = pick_one(COUNTRY_PATTERNS)
        phone, _ = pick_one(PHONE_PATTERNS)
        email, _ = pick_one(EMAIL_PATTERNS)
        website, _ = pick_one(WEBSITE_PATTERNS, ["facebook", "instagram", "linkedin", "twitter", "youtube", "tiktok"])
        hall, _ = pick_many(HALL_PATTERNS, None, None)
        stand, _ = pick_many(STAND_PATTERNS, None, None)
        main_exhibitor, _ = pick_one(MAIN_EXHIBITOR_PATTERNS, ["url"])
        main_exhibitor_url, _ = pick_one([pattern + "_url" for pattern in MAIN_EXHIBITOR_PATTERNS])

        co_exhibitors = self._extract_co_exhibitors(data)
        social_links = self._extract_social_links(flat)
        contacts = self._extract_contacts(data)
        categories = self._extract_categories(data, flat)

        profile_url = self._absolute_url(profile_url)
        main_exhibitor_url = self._absolute_url(main_exhibitor_url)

        if not any([company_name, profile_url, address]):
            return None

        hall_stands = " | ".join(value for value in (hall, stand) if value)

        return ExhibitorRecord(
            source=source,
            profile_url=profile_url,
            company_name=company_name,
            company_address=address,
            company_country=country,
            company_phone=phone,
            company_email=email,
            company_website=website,
            hall_stands=hall_stands,
            main_exhibitor_name=main_exhibitor,
            main_exhibitor_profile_url=main_exhibitor_url,
            co_exhibitors=co_exhibitors,
            social_links=social_links,
            contacts=contacts,
            categories=categories,
        )

    def _absolute_url(self, url: str) -> str:
        if not url:
            return ""
        url = url.strip()
        if url.startswith("http://") or url.startswith("https://"):
            return url
        return urljoin(self.config.page_url, url)

    def _extract_co_exhibitors(self, data: Any) -> List[str]:
        names: List[str] = []

        def walk(value: Any, key_hint: str = "") -> None:
            if isinstance(value, dict):
                for key, child in value.items():
                    lower = str(key).lower()
                    if any(pattern in lower for pattern in CO_EXHIBITOR_PATTERNS):
                        text = self._stringify(child)
                        if text and len(text) > 2:
                            names.extend(part.strip() for part in re.split(r"\s*[|,]\s*", text) if part.strip())
                    else:
                        walk(child, lower)
            elif isinstance(value, list):
                for item in value:
                    walk(item, key_hint)
            elif key_hint and any(pattern in key_hint for pattern in CO_EXHIBITOR_PATTERNS):
                text = self._stringify(value)
                if text and len(text) > 2:
                    names.append(text)

        walk(data)
        deduped: List[str] = []
        seen = set()
        for name in names:
            normalised = name.strip()
            key = normalised.lower()
            if normalised and key not in seen:
                deduped.append(normalised)
                seen.add(key)
        return deduped

    def _extract_social_links(self, flat: Dict[str, Any]) -> Dict[str, str]:
        links: Dict[str, str] = {}
        for value in flat.values():
            text = self._stringify(value)
            if not text:
                continue
            lower = text.lower()
            if not lower.startswith("http"):
                continue
            for network, markers in SOCIAL_MARKERS.items():
                if any(marker in lower for marker in markers):
                    links.setdefault(network, text)
        return links

    def _extract_contacts(self, data: Any) -> List[ContactPerson]:
        candidates = self._find_structured_list(data, ["contact", "person", "name", "email"])
        contacts: List[ContactPerson] = []

        for item in candidates:
            if not isinstance(item, dict):
                continue
            flat = self._flatten_dict(item)
            used: List[str] = []
            first, _ = self._find_value(flat, CONTACT_FIRSTNAME_PATTERNS, None, used)
            last, _ = self._find_value(flat, CONTACT_LASTNAME_PATTERNS, None, used)
            name, _ = self._find_value(flat, ["name", "full_name", "display_name"], None, used)
            if not name:
                name = " ".join(part for part in [first, last] if part).strip()

            email, _ = self._find_value(flat, EMAIL_PATTERNS, None, used)
            phone, _ = self._find_value(flat, PHONE_PATTERNS, None, used)
            position, _ = self._find_value(flat, POSITION_PATTERNS, None, used)
            socials = self._extract_social_links(flat)

            if not any([name, email, phone, position, socials]):
                continue
            contacts.append(
                ContactPerson(
                    name=name,
                    email=email,
                    phone=phone,
                    position=position,
                    linkedin=socials.get("linkedin", ""),
                    instagram=socials.get("instagram", ""),
                    facebook=socials.get("facebook", ""),
                    youtube=socials.get("youtube", ""),
                )
            )

        if contacts:
            return contacts

        flat = self._flatten_dict(data)
        contact_names = []
        for key, value in flat.items():
            lower = key.lower()
            if "contact" in lower and "name" in lower:
                text = self._stringify(value)
                if text:
                    contact_names.append(text)
        if contact_names:
            return [ContactPerson(name=" | ".join(contact_names))]
        return []

    def _extract_categories(self, data: Any, flat: Dict[str, Any]) -> List[Category]:
        categories: List[Category] = []

        candidates = self._find_structured_list(data, ["category", "product", "segment"])
        for item in candidates:
            if isinstance(item, dict):
                cat_flat = self._flatten_dict(item)
                used: List[str] = []
                level1, _ = self._find_value(cat_flat, CATEGORY_LEVEL1_PATTERNS, None, used)
                level2, _ = self._find_value(cat_flat, CATEGORY_LEVEL2_PATTERNS, None, used)
                if not level1 and not level2:
                    level1 = self._stringify(cat_flat.get("name"))
                if level1 or level2:
                    categories.append(Category(level1=level1, level2=level2))
            elif isinstance(item, str):
                categories.append(Category(level1=self._stringify(item)))

        for key, value in flat.items():
            lower = key.lower()
            if any(term in lower for term in ("category", "product")) and not isinstance(value, dict):
                text = self._stringify(value)
                if text and len(text) > 2 and not text.startswith("http"):
                    categories.append(Category(level1=text))

        deduped: List[Category] = []
        seen = set()
        for category in categories:
            key = (category.level1.lower(), category.level2.lower())
            if key not in seen:
                seen.add(key)
                deduped.append(category)
        return deduped

    def _find_structured_list(self, data: Any, keywords: Sequence[str]) -> List[Any]:
        best: List[Any] = []
        best_score = 0

        def score_dict(item: Dict[str, Any]) -> int:
            score = 0
            for key in item.keys():
                lower = str(key).lower()
                if any(keyword in lower for keyword in keywords):
                    score += 2
            for value in item.values():
                if isinstance(value, (str, int, float)) and any(
                    keyword in str(value).lower() for keyword in keywords
                ):
                    score += 1
            return score

        def walk(value: Any) -> None:
            nonlocal best, best_score
            if isinstance(value, list):
                if value:
                    if all(isinstance(item, dict) for item in value):
                        score = sum(score_dict(item) for item in value[: min(5, len(value))])
                        if score > best_score:
                            best = value
                            best_score = score
                    elif all(isinstance(item, str) for item in value):
                        score = sum(
                            1 for item in value if any(keyword in item.lower() for keyword in keywords)
                        )
                        if score > best_score:
                            best = value
                            best_score = score
                for item in value:
                    walk(item)
            elif isinstance(value, dict):
                for nested in value.values():
                    walk(nested)

        walk(data)
        if isinstance(best, list):
            return best
        return []

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
    def _fetch_text(self, url: str, accept: Optional[str] = None) -> str:
        last_error: Optional[Exception] = None
        headers = {"User-Agent": self.config.user_agent}
        if accept:
            headers["Accept"] = accept

        for attempt in range(1, self.config.retries + 1):
            request = Request(url, headers=headers)
            start_time = time.perf_counter()
            try:
                opener = self._opener.open if self._opener else urlopen
                with closing(opener(request, timeout=REQUEST_TIMEOUT)) as response:
                    charset = response.headers.get_content_charset() or "utf-8"
                    body = response.read()
                    elapsed = time.perf_counter() - start_time
                    logger.debug(
                        "Fetched %s in %.2fs (attempt %s/%s, %s bytes)",
                        url,
                        elapsed,
                        attempt,
                        self.config.retries,
                        len(body),
                    )
                    return body.decode(charset, errors="replace")
            except (HTTPError, URLError, TimeoutError) as exc:
                last_error = exc
                elapsed = time.perf_counter() - start_time
                logger.debug(
                    "Request failed for %s (attempt %s/%s after %.2fs): %s",
                    url,
                    attempt,
                    self.config.retries,
                    elapsed,
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

def save_as_excel(path: Path, headers: Sequence[str], rows: Iterable[Dict[str, str]]) -> None:
    """Write rows to an XLSX file using only the standard library."""

    rows = [
        {header: row.get(header, "") for header in headers}
        for row in rows
    ]
    headers = list(headers)

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
    value = value.replace('"', "&quot;").replace("'", "&apos;")
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
    "<dc:creator>MesseDuesseldorfScraper</dc:creator>"
    "<cp:lastModifiedBy>MesseDuesseldorfScraper</cp:lastModifiedBy>"
    "<dcterms:created xsi:type=\"dcterms:W3CDTF\"></dcterms:created>"
    "<dcterms:modified xsi:type=\"dcterms:W3CDTF\"></dcterms:modified>"
    "</cp:coreProperties>"
)


# ----------------------------------------------------------------------
# CSV helpers and command line interface
# ----------------------------------------------------------------------

def save_to_csv(path: Path, headers: Sequence[str], rows: Iterable[Dict[str, str]]) -> None:
    normalised_rows = [
        {header: row.get(header, "") for header in headers}
        for row in rows
    ]

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(headers))
        writer.writeheader()
        if normalised_rows:
            writer.writerows(normalised_rows)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download exhibitor contact information from Messe Düsseldorf directories and export to CSV/Excel."
        )
    )
    parser.add_argument("--page-url", default=DEFAULT_PAGE_URL, help="Exhibitor list page URL")
    parser.add_argument("--output-csv", default=DEFAULT_CSV, help="Output CSV filename")
    parser.add_argument("--output-xlsx", default=DEFAULT_XLSX, help="Output XLSX filename")
    parser.add_argument(
        "--output-format",
        choices=sorted(SUPPORTED_FORMATS),
        default="compact",
        help="Output layout: compact (single row per exhibitor) or expanded (one row per product category)",
    )
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
        default="MesseDuesseldorfScraper/1.0 (+https://github.com/openai/)",
        help="Custom User-Agent header",
    )
    parser.add_argument(
        "--proxy",
        help=(
            "HTTP/HTTPS proxy URL, for example http://user:pass@host:port. "
            "Applied to all outbound requests."
        ),
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable verbose debug logging for troubleshooting",
    )
    parser.add_argument(
        "--diagnose",
        action="store_true",
        help="Run diagnostics instead of scraping, printing timing and parsing stats",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if getattr(args, "debug", False) else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    config = ScraperConfig(
        page_url=args.page_url,
        output_csv=args.output_csv,
        output_xlsx=args.output_xlsx,
        delay=args.delay,
        retries=args.retries,
        user_agent=args.user_agent,
        output_format=args.output_format,
        proxy=args.proxy,
    )

    scraper = MesseDuesseldorfScraper(config)

    if getattr(args, "diagnose", False):
        diagnostics = scraper.diagnose()
        print(json.dumps(diagnostics, indent=2, ensure_ascii=False))
        return 0 if diagnostics.get("download_success") else 1

    records = scraper.collect()

    if config.output_format == "expanded":
        headers = EXPANDED_HEADERS
        rows: List[Dict[str, str]] = []
        for record in records:
            rows.extend(record.iter_expanded_rows())
    else:
        headers = COMPACT_HEADERS
        rows = [record.to_compact_row() for record in records]

    csv_path = Path(config.output_csv)
    save_to_csv(csv_path, headers, rows)
    logger.info("CSV export written to %s", csv_path.resolve())

    xlsx_path = Path(config.output_xlsx)
    save_as_excel(xlsx_path, headers, rows)
    logger.info("Excel export written to %s", xlsx_path.resolve())

    if not records:
        logger.warning("No exhibitor data collected. Verify site structure or network access.")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
