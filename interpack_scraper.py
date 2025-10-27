"""Command-line tool to export exhibitor data from Messe Düsseldorf fairs.

This script accesses the JSON API that powers the public exhibitor search on
interpack.com-like trade fair websites. It iterates through the available first
letter filters and paginated results, exporting the collected company data to
CSV and Excel formats.

The tool is intended for legitimate use. Make sure you are allowed to crawl the
chosen website and respect its robots.txt as well as the fair organiser's terms
of service.
"""
from __future__ import annotations

import argparse
import importlib
import logging
import os
import queue
import string
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - imported for type checking only
    import pandas as pd  # noqa: F401
    from pandas import DataFrame  # noqa: F401
    import requests  # noqa: F401


DataFrame = Any
pd = None
requests = None

LOGGER = logging.getLogger(__name__)


ProgressCallback = Callable[[Optional[str], int, int, int, int, bool], None]


@dataclass
class FetchConfig:
    """Configuration for API requests."""

    base_url: str
    ticket: str
    lang: str
    query: str
    page_size: int
    delay: float
    retries: int
    first_letters: Iterable[str]
    extra_params: Dict[str, Any]
    disable_proxy: bool


DEFAULT_DEPENDENCIES: tuple[str, ...] = ("requests", "pandas", "openpyxl")


class DependencyError(RuntimeError):
    """Raised when dependencies cannot be ensured."""


def remove_proxy_env(env: Dict[str, str]) -> Dict[str, str]:
    """Return a copy of *env* without proxy variables."""

    cleaned = {key: value for key, value in env.items() if "proxy" not in key.lower()}
    return cleaned


def missing_dependencies(packages: Iterable[str]) -> List[str]:
    """Return packages whose import spec cannot be located."""

    missing: List[str] = []
    for package in packages:
        if importlib.util.find_spec(package) is None:
            missing.append(package)
    return missing


def install_with_pip(packages: Iterable[str], *, upgrade: bool, disable_proxy: bool) -> None:
    """Install *packages* via pip, optionally upgrading them."""

    package_list = list(packages)
    if not package_list:
        return

    command = [sys.executable, "-m", "pip", "install"]
    if upgrade:
        command.append("--upgrade")
    command.extend(package_list)

    LOGGER.info("Installing dependencies with pip: %s", " ".join(package_list))

    env = os.environ.copy()
    if disable_proxy:
        env = remove_proxy_env(env)

    try:
        subprocess.check_call(command, env=env)
    except subprocess.CalledProcessError as exc:  # pragma: no cover - subprocess failure
        raise DependencyError(
            (
                "pip failed with exit code "
                f"{exc.returncode}. Check your internet access, consider --disable-proxy, "
                "or install the packages manually."
            )
        ) from exc


def load_runtime_modules(disable_proxy: bool) -> None:
    """Import pandas and requests after dependencies are present."""

    global pd, requests, DataFrame

    pd = importlib.import_module("pandas")
    requests = importlib.import_module("requests")
    DataFrame = getattr(pd, "DataFrame")

    if disable_proxy and hasattr(requests, "Session"):
        LOGGER.debug("Proxy usage disabled for outgoing HTTP requests.")


def ensure_runtime_environment(
    *, bootstrap: bool, upgrade: bool, disable_proxy: bool
) -> None:
    """Verify dependencies, optionally install them, and load runtime modules."""

    missing = missing_dependencies(DEFAULT_DEPENDENCIES)

    packages_to_install: List[str] = []
    pip_upgrade = upgrade
    if bootstrap:
        packages_to_install = list(DEFAULT_DEPENDENCIES)
        pip_upgrade = True
    elif missing:
        packages_to_install = missing

    if pip_upgrade and not packages_to_install:
        packages_to_install = list(DEFAULT_DEPENDENCIES)

    if packages_to_install:
        install_with_pip(packages_to_install, upgrade=pip_upgrade, disable_proxy=disable_proxy)

    load_runtime_modules(disable_proxy)


def coalesce(*values: Any) -> str:
    """Return the first truthy string from *values.*"""

    for value in values:
        if isinstance(value, str):
            candidate = value.strip()
            if candidate:
                return candidate
        elif value:
            return str(value)
    return ""


def join_list(value: Any) -> str:
    """Join list-like values with a comma; return strings unchanged."""

    if isinstance(value, (list, tuple, set)):
        parts: List[str] = []
        for item in value:
            if isinstance(item, dict):
                # Prefer common name keys if present.
                name = coalesce(
                    item.get("name"),
                    item.get("description"),
                    item.get("title"),
                )
                if name:
                    parts.append(name)
            elif item:
                parts.append(str(item))
        return ", ".join(parts)
    if isinstance(value, str):
        return value.strip()
    if value is None:
        return ""
    return str(value)


def prepare_output_path(path: str) -> Path:
    """Expand *path* to a :class:`Path` and ensure its parent directory exists."""

    output_path = Path(path).expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    return output_path


def first_contact(item: Dict[str, Any]) -> Dict[str, Any]:
    """Return the first contact dictionary found inside *item*."""

    for key in ("contacts", "contactPersons", "contactPerson", "contact"):
        value = item.get(key)
        if isinstance(value, list) and value:
            contact = value[0]
            if isinstance(contact, dict):
                return contact
        elif isinstance(value, dict):
            return value
    return {}


def extract_location(item: Dict[str, Any]) -> Dict[str, Any]:
    """Return a location dictionary, if present."""

    for key in ("location", "locations", "stand", "halls"):
        value = item.get(key)
        if isinstance(value, list) and value:
            element = value[0]
            if isinstance(element, dict):
                return element
        elif isinstance(value, dict):
            return value
    return {}


def extract_address(item: Dict[str, Any]) -> Dict[str, Any]:
    """Return an address dictionary from the item or nested company data."""

    address_candidates = [
        item.get("address"),
        item.get("company", {}).get("address") if isinstance(item.get("company"), dict) else None,
    ]
    for candidate in address_candidates:
        if isinstance(candidate, dict):
            return candidate
    return {}


def parse_company_record(item: Dict[str, Any]) -> Dict[str, Any]:
    """Extract a flat company record from a raw API item."""

    company = item.get("company") if isinstance(item.get("company"), dict) else {}
    address = extract_address(item)
    location = extract_location(item)
    contact = first_contact(item)

    hall = coalesce(
        location.get("hall"),
        location.get("hallName"),
        location.get("hallNumber"),
    )
    stand_number = join_list(
        location.get("standNumber")
        or location.get("standNumbers")
        or location.get("stand")
    )

    product_categories = join_list(
        item.get("productCategories")
        or company.get("productCategories")
    )

    return {
        "companyName": coalesce(
            company.get("companyName"),
            item.get("companyName"),
            item.get("name"),
        ),
        "country": coalesce(
            address.get("country"),
            company.get("country"),
            item.get("country"),
        ),
        "website": coalesce(
            company.get("website"),
            item.get("website"),
            contact.get("website"),
            contact.get("url"),
        ),
        "hall": hall,
        "standNumber": stand_number,
        "productCategories": product_categories,
        "address": coalesce(
            address.get("street"),
            address.get("address1"),
            address.get("address"),
        ),
        "zipCode": coalesce(address.get("zipCode"), address.get("postalCode")),
        "city": coalesce(address.get("city")),
        "contactName": coalesce(
            contact.get("name"),
            coalesce(contact.get("firstName"), contact.get("lastName")),
        ),
        "contactEmail": coalesce(contact.get("email")),
        "contactPhone": coalesce(contact.get("phone"), contact.get("telephone")),
    }


def locate_items(data: Any) -> List[Dict[str, Any]]:
    """Locate the list of item dictionaries inside a response payload."""

    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]

    if isinstance(data, dict):
        for key in ("items", "itemList", "results", "data", "result"):
            value = data.get(key)
            items = locate_items(value)
            if items:
                return items
        # Fallback: search nested dictionaries
        for value in data.values():
            items = locate_items(value)
            if items:
                return items
    return []


def fetch_page(
    session: requests.Session,
    config: FetchConfig,
    letter: Optional[str],
    page: int,
    retry_delay: float,
) -> List[Dict[str, Any]]:
    """Fetch a single page for *letter* and *page* using *session*."""

    params: Dict[str, Any] = {
        "ticket": config.ticket,
        "lang": config.lang,
        "query": config.query,
        "page": page,
        "pageSize": config.page_size,
    }
    if letter:
        params["filter[firstLetter]"] = letter
    params.update(config.extra_params)

    for attempt in range(1, config.retries + 1):
        try:
            LOGGER.debug("Fetching letter=%s page=%s", letter or "ALL", page)
            response = session.get(config.base_url, params=params, timeout=30)
            if response.status_code >= 500:
                raise requests.HTTPError(
                    f"Server error {response.status_code}",
                    response=response,
                )
            response.raise_for_status()
            payload = response.json()
            items = locate_items(payload)
            return [parse_company_record(item) for item in items]
        except requests.RequestException as exc:  # pragma: no cover - network errors
            LOGGER.warning(
                "Request failed for letter=%s page=%s (attempt %s/%s): %s",
                letter or "ALL",
                page,
                attempt,
                config.retries,
                exc,
            )
            if attempt == config.retries:
                break
            if retry_delay:
                time.sleep(retry_delay)
        except ValueError as exc:  # pragma: no cover - invalid JSON
            LOGGER.error("Failed to decode JSON response: %s", exc)
            break
    return []


def fetch_all(
    config: FetchConfig,
    progress: Optional[ProgressCallback] = None,
    stop_event: Optional[threading.Event] = None,
) -> DataFrame:
    """Fetch all exhibitors according to *config* and return a DataFrame."""

    if pd is None or requests is None:
        raise DependencyError(
            "Runtime dependencies are not loaded. Call ensure_runtime_environment() before fetch_all."
        )

    records: List[Dict[str, Any]] = []
    letters: List[Optional[str]] = list(config.first_letters) or [None]
    session = requests.Session()
    if config.disable_proxy:
        session.trust_env = False
        session.proxies.clear()
    try:
        for letter_index, letter in enumerate(letters):
            page = 1
            while True:
                if stop_event and stop_event.is_set():
                    LOGGER.info("Fetch cancelled at letter=%s page=%s", letter, page)
                    if progress:
                        progress(letter, page, len(records), letter_index, len(letters), True)
                    return pd.DataFrame(records)

                if progress:
                    progress(letter, page, len(records), letter_index, len(letters), False)

                page_records = fetch_page(session, config, letter, page, config.delay)
                if not page_records:
                    if progress:
                        progress(letter, page, len(records), letter_index, len(letters), True)
                    break

                records.extend(page_records)

                if progress:
                    progress(letter, page, len(records), letter_index, len(letters), False)

                page += 1
                if config.delay:
                    time.sleep(config.delay)
    finally:
        session.close()

    df = pd.DataFrame(records)
    return df


def save_to_csv(df: DataFrame, filename: str) -> None:
    """Save *df* to CSV using UTF-8 encoding."""

    output_path = prepare_output_path(filename)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    LOGGER.info("CSV saved to %s", output_path)


def save_to_excel(df: DataFrame, filename: str) -> None:
    """Save *df* to an Excel workbook."""

    output_path = prepare_output_path(filename)
    df.to_excel(output_path, index=False, engine="openpyxl")
    LOGGER.info("Excel file saved to %s", output_path)


def upload_to_google_drive(_file_path: str, credentials_path: Optional[str] = None) -> None:
    """Placeholder for future Google Drive upload support."""

    if credentials_path:
        LOGGER.info(
            "Google Drive upload is not implemented. Provide implementation before use."
        )
    else:
        LOGGER.debug("Google Drive upload skipped (no credentials provided).")


def send_via_microsoft_graph(
    _file_path: str,
    token: Optional[str] = None,
    recipient: Optional[str] = None,
) -> None:
    """Placeholder for future Microsoft Graph e-mail integration."""

    if token and recipient:
        LOGGER.info(
            "Microsoft Graph integration is not implemented. Provide implementation before use."
        )
    else:
        LOGGER.debug("Microsoft Graph e-mail skipped (missing token or recipient).")


def parse_letters(first_letter: Optional[str]) -> Iterable[str]:
    """Compute the list of first-letter filters to iterate over."""

    if first_letter:
        return [first_letter.upper()]
    return list(string.ascii_uppercase)


def build_config(args: argparse.Namespace) -> FetchConfig:
    """Create a :class:`FetchConfig` instance from CLI arguments."""

    extra_params: Dict[str, Any] = {}
    if args.additional_param:
        for param in args.additional_param:
            if "=" not in param:
                LOGGER.warning("Skipping malformed extra param: %s", param)
                continue
            key, value = param.split("=", 1)
            extra_params[key] = value

    return FetchConfig(
        base_url=args.base_url,
        ticket=args.ticket,
        lang=args.lang,
        query=args.query or "",
        page_size=args.page_size,
        delay=args.delay,
        retries=args.retries,
        first_letters=list(parse_letters(args.first_letter)),
        extra_params=extra_params,
        disable_proxy=args.disable_proxy,
    )


def run_gui(
    disable_proxy_default: bool = False,
    install_deps_default: bool = False,
) -> None:
    """Launch a Tkinter-based graphical interface for the exporter."""

    import tkinter as tk
    from tkinter import filedialog, messagebox, ttk

    root = tk.Tk()
    root.title("Exhibitor Exporter")
    root.minsize(760, 620)
    root.columnconfigure(0, weight=1)
    root.rowconfigure(0, weight=1)

    mainframe = ttk.Frame(root, padding=16)
    mainframe.grid(row=0, column=0, sticky="nsew")
    mainframe.columnconfigure(1, weight=1)

    info_text = (
        "Use the fields below to configure the exhibitor export. Respect the fair "
        "organiser's terms of service and robots.txt when scraping."
    )
    ttk.Label(mainframe, text=info_text, wraplength=700, justify="left").grid(
        row=0, column=0, columnspan=3, sticky="w", pady=(0, 12)
    )

    entry_vars: Dict[str, tk.StringVar] = {
        "base_url": tk.StringVar(value="https://www.interpack.com/vis/v1/api/searchResult"),
        "ticket": tk.StringVar(value="g_u_e_s_t"),
        "lang": tk.StringVar(value="en"),
        "query": tk.StringVar(value=""),
        "page_size": tk.StringVar(value="50"),
        "delay": tk.StringVar(value="1"),
        "retries": tk.StringVar(value="3"),
        "first_letter": tk.StringVar(value=""),
        "extra_params": tk.StringVar(value=""),
        "output_csv": tk.StringVar(value="exhibitors.csv"),
        "output_xlsx": tk.StringVar(value="exhibitors.xlsx"),
        "drive_credentials": tk.StringVar(value=""),
        "graph_token": tk.StringVar(value=""),
        "graph_recipient": tk.StringVar(value=""),
    }

    disable_proxy_var = tk.BooleanVar(value=disable_proxy_default)
    install_deps_var = tk.BooleanVar(value=install_deps_default)

    field_definitions = [
        ("Base API URL", "base_url", None),
        ("Ticket", "ticket", None),
        ("Language", "lang", None),
        ("Query", "query", None),
        ("Page size", "page_size", None),
        ("Delay (seconds)", "delay", None),
        ("Retries", "retries", None),
        ("First letter (optional)", "first_letter", None),
        (
            "Additional params (key=value; one per line)",
            "extra_params",
            "text",
        ),
        ("CSV output", "output_csv", "file_csv"),
        ("Excel output", "output_xlsx", "file_xlsx"),
        (
            "Google Drive credentials (optional)",
            "drive_credentials",
            "file_any",
        ),
        ("Graph token (optional)", "graph_token", None),
        ("Graph recipient (optional)", "graph_recipient", None),
    ]

    widgets: Dict[str, Any] = {}

    def browse_file(var_name: str, filetypes: List[tuple[str, str]], defaultext: str = "") -> None:
        initial = entry_vars[var_name].get() or ""
        if defaultext:
            filename = filedialog.asksaveasfilename(
                initialfile=initial,
                defaultextension=defaultext,
                filetypes=filetypes,
            )
        else:
            filename = filedialog.askopenfilename(initialfile=initial, filetypes=filetypes)
        if filename:
            entry_vars[var_name].set(filename)

    row_index = 1
    for label_text, key, field_type in field_definitions:
        ttk.Label(mainframe, text=label_text).grid(row=row_index, column=0, sticky="w", pady=2)
        if field_type == "text":
            text_widget = tk.Text(mainframe, height=3, width=40)
            text_widget.insert("1.0", entry_vars[key].get())
            text_widget.grid(row=row_index, column=1, columnspan=2, sticky="nsew", pady=2)
            widgets[key] = text_widget
        else:
            entry = ttk.Entry(mainframe, textvariable=entry_vars[key])
            entry.grid(row=row_index, column=1, sticky="ew", pady=2)
            widgets[key] = entry
            if field_type == "file_csv":
                ttk.Button(
                    mainframe,
                    text="Browse…",
                    command=lambda k=key: browse_file(k, [("CSV files", "*.csv"), ("All files", "*.*")], ".csv"),
                ).grid(row=row_index, column=2, padx=4, sticky="w")
            elif field_type == "file_xlsx":
                ttk.Button(
                    mainframe,
                    text="Browse…",
                    command=lambda k=key: browse_file(k, [("Excel files", "*.xlsx"), ("All files", "*.*")], ".xlsx"),
                ).grid(row=row_index, column=2, padx=4, sticky="w")
            elif field_type == "file_any":
                ttk.Button(
                    mainframe,
                    text="Browse…",
                    command=lambda k=key: browse_file(k, [("All files", "*.*")]),
                ).grid(row=row_index, column=2, padx=4, sticky="w")
        if field_type == "text":
            mainframe.rowconfigure(row_index, weight=1)
        row_index += 1

    options_frame = ttk.Frame(mainframe)
    options_frame.grid(row=row_index, column=0, columnspan=3, sticky="w", pady=(6, 0))
    ttk.Checkbutton(
        options_frame,
        text="HTTP proxy ayarlarını yok say",
        variable=disable_proxy_var,
    ).grid(row=0, column=0, sticky="w")
    ttk.Checkbutton(
        options_frame,
        text="Gerekli Python paketlerini pip ile indir/güncelle",
        variable=install_deps_var,
    ).grid(row=0, column=1, sticky="w", padx=(12, 0))

    row_index += 1

    log_label_row = row_index
    ttk.Label(mainframe, text="Log output").grid(row=log_label_row, column=0, sticky="w", pady=(12, 2))
    log_text = tk.Text(mainframe, height=12, state="disabled")
    log_text.grid(row=log_label_row + 1, column=0, columnspan=3, sticky="nsew")
    scrollbar = ttk.Scrollbar(mainframe, orient="vertical", command=log_text.yview)
    scrollbar.grid(row=log_label_row + 1, column=3, sticky="ns")
    log_text.configure(yscrollcommand=scrollbar.set)
    mainframe.rowconfigure(log_label_row + 1, weight=1)

    status_var = tk.StringVar(value="Idle")
    status_label = ttk.Label(mainframe, textvariable=status_var)
    status_label.grid(row=log_label_row + 2, column=0, sticky="w", pady=(8, 0))

    progress = ttk.Progressbar(mainframe, mode="determinate", maximum=1, value=0)
    progress.grid(row=log_label_row + 2, column=1, columnspan=2, sticky="ew", pady=(8, 0))

    message_queue: "queue.Queue[tuple[str, Any]]" = queue.Queue()
    worker_thread: Optional[threading.Thread] = None
    stop_event: Optional[threading.Event] = None

    def append_log(message: str) -> None:
        log_text.configure(state="normal")
        log_text.insert("end", message + "\n")
        log_text.see("end")
        log_text.configure(state="disabled")

    def parse_extra_params(raw: str) -> Dict[str, str]:
        params: Dict[str, str] = {}
        for line in raw.replace(";", "\n").splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            if "=" not in stripped:
                raise ValueError(f"Invalid parameter '{stripped}'. Use key=value format.")
            key, value = stripped.split("=", 1)
            params[key.strip()] = value.strip()
        return params

    def start_scrape() -> None:
        nonlocal worker_thread, stop_event

        if worker_thread and worker_thread.is_alive():
            messagebox.showinfo("Exhibitor Exporter", "Zaten devam eden bir işlem var.")
            return

        csv_path = entry_vars["output_csv"].get().strip()
        xlsx_path = entry_vars["output_xlsx"].get().strip()
        base_url = entry_vars["base_url"].get().strip()
        ticket = entry_vars["ticket"].get().strip()

        if not base_url or not ticket:
            messagebox.showerror("Eksik bilgi", "Base URL ve ticket alanları zorunludur.")
            return

        try:
            page_size = int(entry_vars["page_size"].get() or 50)
        except ValueError:
            messagebox.showerror("Geçersiz değer", "Sayfa boyutu tam sayı olmalıdır.")
            return

        try:
            delay = float(entry_vars["delay"].get() or 0)
        except ValueError:
            messagebox.showerror("Geçersiz değer", "Bekleme süresi sayısal olmalıdır.")
            return

        try:
            retries = int(entry_vars["retries"].get() or 1)
        except ValueError:
            messagebox.showerror("Geçersiz değer", "Tekrar sayısı tam sayı olmalıdır.")
            return

        extra_params_input = entry_vars["extra_params"].get()
        if widgets.get("extra_params") and isinstance(widgets["extra_params"], tk.Text):
            extra_params_input = widgets["extra_params"].get("1.0", "end").strip()

        try:
            extra_params = parse_extra_params(extra_params_input)
        except ValueError as exc:
            messagebox.showerror("Geçersiz parametre", str(exc))
            return

        first_letter_value = entry_vars["first_letter"].get().strip() or None
        letters_to_use: List[str] = list(parse_letters(first_letter_value))

        log_text.configure(state="normal")
        log_text.delete("1.0", "end")
        log_text.configure(state="disabled")

        total_letters = max(1, len(letters_to_use))
        progress.configure(mode="determinate", maximum=total_letters, value=0)
        status_var.set("Hazırlanıyor…")

        disable_proxy = disable_proxy_var.get()
        install_dependencies = install_deps_var.get()

        stop_event = threading.Event()

        while not message_queue.empty():
            try:
                message_queue.get_nowait()
            except queue.Empty:
                break

        def worker(letters: List[Optional[str]], cancel_event: threading.Event) -> None:
            message_queue.put(("status", "Hazırlanıyor…"))
            progress_state = {"last_records": 0}

            def progress_callback(
                letter: Optional[str],
                page: int,
                total_records: int,
                letter_index: int,
                letter_total: int,
                done: bool,
            ) -> None:
                display_letter = letter or "Tümü"
                if letter:
                    base_text = f"{display_letter} harfi - {page}. sayfa"
                else:
                    base_text = f"Tüm kayıtlar - {page}. sayfa"
                status_text = base_text + (" tamamlandı" if done else "")
                message_queue.put(
                    (
                        "progress",
                        {
                            "letter_index": letter_index,
                            "letter_total": letter_total,
                            "done": done,
                            "status": status_text,
                            "records": total_records,
                        },
                    )
                )
                if total_records > progress_state["last_records"]:
                    message_queue.put(
                        (
                            "log",
                            f"{base_text}: toplam {total_records} kayıt indirildi.",
                        )
                    )
                    progress_state["last_records"] = total_records
                if done:
                    if letter is not None:
                        message_queue.put(("log", f"{display_letter} harfi tamamlandı."))
                    else:
                        message_queue.put(("log", "Tüm kayıtlar tamamlandı."))

            try:
                if install_dependencies:
                    message_queue.put(("status", "Bağımlılıklar indiriliyor…"))
                    message_queue.put(
                        ("log", "pip ile gerekli Python paketleri güncelleniyor…")
                    )
                    ensure_runtime_environment(
                        bootstrap=True,
                        upgrade=True,
                        disable_proxy=disable_proxy,
                    )
                    message_queue.put(("log", "Python paketleri güncellendi."))
                message_queue.put(("status", "Veri çekiliyor…"))
                config = FetchConfig(
                    base_url=base_url,
                    ticket=ticket,
                    lang=entry_vars["lang"].get().strip() or "en",
                    query=entry_vars["query"].get(),
                    page_size=page_size,
                    delay=delay,
                    retries=retries,
                    first_letters=letters,
                    extra_params=extra_params,
                    disable_proxy=disable_proxy,
                )
                df = fetch_all(config, progress=progress_callback, stop_event=cancel_event)
                if cancel_event.is_set():
                    message_queue.put(("log", "İşlem iptal edildi, dosyalar kaydedilmedi."))
                    message_queue.put(("status", "İptal edildi"))
                elif df.empty:
                    message_queue.put(("log", "Herhangi bir firma bulunamadı. Filtreleri kontrol edin."))
                    message_queue.put(("status", "Kayıt bulunamadı"))
                else:
                    message_queue.put(("log", f"Toplam {len(df)} kayıt alındı."))
                    message_queue.put(
                        ("log", f"CSV dosyası kaydediliyor: {csv_path or 'exhibitors.csv'}")
                    )
                    save_to_csv(df, csv_path or "exhibitors.csv")
                    message_queue.put(
                        (
                            "log",
                            f"Excel dosyası kaydediliyor: {xlsx_path or 'exhibitors.xlsx'}",
                        )
                    )
                    save_to_excel(df, xlsx_path or "exhibitors.xlsx")

                    drive_credentials = entry_vars["drive_credentials"].get().strip()
                    if drive_credentials:
                        message_queue.put(
                            ("log", "Google Drive yükleme yer tutucusu çalıştırıldı.")
                        )
                        upload_to_google_drive(xlsx_path or "exhibitors.xlsx", drive_credentials)

                    graph_token = entry_vars["graph_token"].get().strip()
                    graph_recipient = entry_vars["graph_recipient"].get().strip()
                    if graph_token and graph_recipient:
                        message_queue.put(
                            ("log", "Microsoft Graph yer tutucusu çalıştırıldı."),
                        )
                        send_via_microsoft_graph(
                            xlsx_path or "exhibitors.xlsx",
                            token=graph_token,
                            recipient=graph_recipient,
                        )

                    message_queue.put(("status", "Tamamlandı"))
            except DependencyError as exc:
                message_queue.put(("error", str(exc)))
                message_queue.put(("status", "Hata"))
                LOGGER.error("Bağımlılık kurulumu başarısız: %s", exc)
            except Exception as exc:  # pragma: no cover - GUI runtime errors
                message_queue.put(("error", str(exc)))
                LOGGER.exception("GUI worker failed: %s", exc)
            finally:
                message_queue.put(("finished", None))

        worker_thread = threading.Thread(
            target=worker,
            args=(letters_to_use, stop_event),
            daemon=True,
        )
        worker_thread.start()

    def cancel_scrape() -> None:
        nonlocal stop_event, worker_thread

        if worker_thread and worker_thread.is_alive():
            if stop_event and not stop_event.is_set():
                stop_event.set()
                status_var.set("İptal ediliyor…")
                message_queue.put(("log", "İptal isteği iletildi. Devam eden sayfa tamamlanacak."))
            else:
                message_queue.put(("log", "İptal isteği zaten işleniyor."))
        else:
            messagebox.showinfo("Exhibitor Exporter", "Aktif bir işlem bulunmuyor.")

    def poll_queue() -> None:
        nonlocal worker_thread, stop_event
        try:
            while True:
                kind, payload = message_queue.get_nowait()
                if kind == "log" and payload is not None:
                    append_log(payload)
                elif kind == "status" and payload is not None:
                    status_var.set(payload)
                    if payload == "Tamamlandı":
                        max_value = float(progress.cget("maximum") or 0) or 1.0
                        progress.configure(value=max_value)
                elif kind == "progress" and isinstance(payload, dict):
                    maximum = payload.get("letter_total") or 1
                    current_max = float(progress.cget("maximum") or 0) or 1.0
                    if abs(current_max - float(maximum)) > 1e-9:
                        progress.configure(maximum=maximum)
                        current_max = float(progress.cget("maximum") or 0) or 1.0
                    value = float(payload.get("letter_index", 0))
                    if payload.get("done"):
                        value = min(float(maximum), value + 1.0)
                    value = max(0.0, min(value, current_max))
                    progress.configure(value=value)
                    status_text = payload.get("status")
                    records = payload.get("records")
                    if status_text:
                        if isinstance(records, int) and records >= 0:
                            status_var.set(f"{status_text} (toplam {records})")
                        else:
                            status_var.set(status_text)
                elif kind == "error" and payload is not None:
                    append_log(payload)
                    messagebox.showerror("Export failed", payload)
                    status_var.set("Hata")
                elif kind == "finished":
                    if stop_event and stop_event.is_set():
                        status_var.set("İptal edildi")
                    if not (stop_event and stop_event.is_set()):
                        max_value = float(progress.cget("maximum") or 0) or 1.0
                        progress.configure(value=max_value)
                    stop_event = None
                    worker_thread = None
        except queue.Empty:
            pass
        root.after(150, poll_queue)

    buttons_frame = ttk.Frame(mainframe)
    buttons_frame.grid(row=log_label_row + 3, column=0, columnspan=3, pady=(12, 0), sticky="e")

    ttk.Button(buttons_frame, text="Dışa aktarımı başlat", command=start_scrape).grid(
        row=0, column=0, padx=(0, 8)
    )
    ttk.Button(buttons_frame, text="İptal et", command=cancel_scrape).grid(
        row=0, column=1, padx=(0, 8)
    )
    ttk.Button(buttons_frame, text="Kapat", command=root.destroy).grid(row=0, column=2)

    root.after(150, poll_queue)
    root.mainloop()


def build_parser() -> argparse.ArgumentParser:
    """Create an argument parser for the CLI."""

    parser = argparse.ArgumentParser(
        description="Export exhibitor data from Messe Düsseldorf JSON APIs.",
    )
    parser.add_argument(
        "--base-url",
        default="https://www.interpack.com/vis/v1/api/searchResult",
        help="Base API endpoint used by the exhibitor search.",
    )
    parser.add_argument(
        "--ticket",
        default="g_u_e_s_t",
        help="Access ticket parameter (use browser devtools to obtain if required).",
    )
    parser.add_argument(
        "--lang",
        default="en",
        help="Language code for results (default: en).",
    )
    parser.add_argument(
        "--query",
        default="",
        help="Optional search query to limit the results.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=50,
        help="Number of records per request (default: 50).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Delay in seconds between requests (default: 1).",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Number of retries for failed requests (default: 3).",
    )
    parser.add_argument(
        "--first-letter",
        help="Fetch only companies starting with the specified letter.",
    )
    parser.add_argument(
        "--output-csv",
        default="exhibitors.csv",
        help="Path to the CSV output file (default: exhibitors.csv).",
    )
    parser.add_argument(
        "--output-xlsx",
        default="exhibitors.xlsx",
        help="Path to the Excel output file (default: exhibitors.xlsx).",
    )
    parser.add_argument(
        "--additional-param",
        action="append",
        help=(
            "Additional query string parameters formatted as key=value. "
            "Use this to mirror hidden fields observed in the network panel."
        ),
    )
    parser.add_argument(
        "--bootstrap-deps",
        action="store_true",
        help=(
            "Install and upgrade required Python packages (requests, pandas, openpyxl) "
            "before running the exporter."
        ),
    )
    parser.add_argument(
        "--upgrade-deps",
        action="store_true",
        help=(
            "Force pip --upgrade for the required Python packages before running without "
            "re-installing everything."
        ),
    )
    parser.add_argument(
        "--disable-proxy",
        action="store_true",
        help=(
            "Ignore HTTP(S)_PROXY environment variables when installing dependencies "
            "and performing API requests."
        ),
    )
    parser.add_argument(
        "--drive-credentials",
        help="Optional path to Google Drive credentials for future integrations.",
    )
    parser.add_argument(
        "--graph-token",
        help="Optional Microsoft Graph token for future integrations.",
    )
    parser.add_argument(
        "--graph-recipient",
        help="Recipient e-mail for Microsoft Graph placeholder integration.",
    )
    parser.add_argument(
        "--gui",
        action="store_true",
        help="Launch the graphical interface instead of running in CLI mode.",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    """Entry point for command line execution."""

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    LOGGER.info(
        "Ensure you comply with robots.txt and the organiser's terms before crawling."
    )

    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        ensure_runtime_environment(
            bootstrap=args.bootstrap_deps,
            upgrade=args.upgrade_deps,
            disable_proxy=args.disable_proxy,
        )
    except DependencyError as exc:
        LOGGER.error("%s", exc)
        return 1

    if args.gui:
        run_gui(
            disable_proxy_default=args.disable_proxy,
            install_deps_default=args.bootstrap_deps or args.upgrade_deps,
        )
        return 0

    config = build_config(args)
    df = fetch_all(config)

    if df.empty:
        LOGGER.warning("No exhibitor data retrieved. Check filters or parameters.")
    else:
        save_to_csv(df, args.output_csv)
        save_to_excel(df, args.output_xlsx)

    if args.drive_credentials:
        upload_to_google_drive(args.output_xlsx, args.drive_credentials)
    if args.graph_token and args.graph_recipient:
        send_via_microsoft_graph(
            args.output_xlsx, token=args.graph_token, recipient=args.graph_recipient
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
