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
import logging
import queue
import string
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd
import requests
from pandas import DataFrame

LOGGER = logging.getLogger(__name__)


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
    first_letters: Tuple[str, ...]
    extra_params: Dict[str, Any]


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


ProgressCallback = Callable[[Optional[str], int, int, int], None]


def fetch_records(
    config: FetchConfig, progress_callback: Optional[ProgressCallback] = None
) -> List[Dict[str, Any]]:
    """Return a list of exhibitor dictionaries using *config*."""

    records: List[Dict[str, Any]] = []
    session = requests.Session()
    letters = config.first_letters or tuple()
    total_letters = len(letters) or 1

    try:
        for letter_index, letter in enumerate(letters, start=1):
            page = 1
            while True:
                if progress_callback:
                    progress_callback(letter, page, letter_index, total_letters)
                page_records = fetch_page(session, config, letter, page, config.delay)
                if not page_records:
                    break
                records.extend(page_records)
                page += 1
                if config.delay:
                    time.sleep(config.delay)
    finally:
        session.close()

    if progress_callback:
        progress_callback(None, 0, total_letters, total_letters)

    return records


def fetch_all(
    config: FetchConfig, progress_callback: Optional[ProgressCallback] = None
) -> DataFrame:
    """Fetch all exhibitors according to *config* and return a DataFrame."""

    records = fetch_records(config, progress_callback=progress_callback)
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


def parse_letters(first_letter: Optional[str]) -> Tuple[str, ...]:
    """Compute the list of first-letter filters to iterate over."""

    if first_letter:
        return (first_letter.upper(),)
    return tuple(string.ascii_uppercase)


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
        first_letters=parse_letters(args.first_letter),
        extra_params=extra_params,
    )


def run_gui() -> None:
    """Launch a Tkinter-based graphical interface for the exporter."""

    import tkinter as tk
    from tkinter import filedialog, messagebox, ttk

    root = tk.Tk()
    root.title("Fuar Katılımcı Aktarıcı")
    root.minsize(760, 620)
    root.columnconfigure(0, weight=1)
    root.rowconfigure(0, weight=1)

    mainframe = ttk.Frame(root, padding=16)
    mainframe.grid(row=0, column=0, sticky="nsew")
    mainframe.columnconfigure(1, weight=1)

    info_text = (
        "Aşağıdaki alanları kullanarak katılımcı listesinin çekilmesini yapılandırın. "
        "Lütfen işlem öncesinde fuar organizatörünün kullanım şartlarına ve robots.txt "
        "kurallarına uyduğunuzdan emin olun."
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

    field_definitions = [
        ("Temel API URL'si", "base_url", None),
        ("Ticket (giriş anahtarı)", "ticket", None),
        ("Dil", "lang", None),
        ("Arama sorgusu", "query", None),
        ("Sayfa başına kayıt", "page_size", None),
        ("İstekler arası bekleme (saniye)", "delay", None),
        ("Yeniden deneme sayısı", "retries", None),
        ("İlk harf (opsiyonel)", "first_letter", None),
        (
            "Ek parametreler (anahtar=değer; satır satır)",
            "extra_params",
            "text",
        ),
        ("CSV çıktısı", "output_csv", "file_csv"),
        ("Excel çıktısı", "output_xlsx", "file_xlsx"),
        (
            "Google Drive kimlik bilgileri (opsiyonel)",
            "drive_credentials",
            "file_any",
        ),
        ("Microsoft Graph token (opsiyonel)", "graph_token", None),
        ("Microsoft Graph alıcısı (opsiyonel)", "graph_recipient", None),
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
                    text="Gözat…",
                    command=lambda k=key: browse_file(k, [("CSV files", "*.csv"), ("All files", "*.*")], ".csv"),
                ).grid(row=row_index, column=2, padx=4, sticky="w")
            elif field_type == "file_xlsx":
                ttk.Button(
                    mainframe,
                    text="Gözat…",
                    command=lambda k=key: browse_file(k, [("Excel files", "*.xlsx"), ("All files", "*.*")], ".xlsx"),
                ).grid(row=row_index, column=2, padx=4, sticky="w")
            elif field_type == "file_any":
                ttk.Button(
                    mainframe,
                    text="Gözat…",
                    command=lambda k=key: browse_file(k, [("All files", "*.*")]),
                ).grid(row=row_index, column=2, padx=4, sticky="w")
        if field_type == "text":
            mainframe.rowconfigure(row_index, weight=1)
        row_index += 1

    log_label_row = row_index
    ttk.Label(mainframe, text="Kayıt günlüğü").grid(
        row=log_label_row, column=0, sticky="w", pady=(12, 2)
    )
    log_text = tk.Text(mainframe, height=12, state="disabled")
    log_text.grid(row=log_label_row + 1, column=0, columnspan=3, sticky="nsew")
    scrollbar = ttk.Scrollbar(mainframe, orient="vertical", command=log_text.yview)
    scrollbar.grid(row=log_label_row + 1, column=3, sticky="ns")
    log_text.configure(yscrollcommand=scrollbar.set)
    mainframe.rowconfigure(log_label_row + 1, weight=1)

    status_var = tk.StringVar(value="Hazır")
    status_frame = ttk.Frame(mainframe)
    status_frame.grid(row=log_label_row + 2, column=0, columnspan=1, sticky="w", pady=(8, 0))
    ttk.Label(status_frame, text="Durum:").grid(row=0, column=0, padx=(0, 4))
    status_label = ttk.Label(status_frame, textvariable=status_var)
    status_label.grid(row=0, column=1)

    progress = ttk.Progressbar(mainframe, mode="determinate", maximum=1)
    progress.grid(row=log_label_row + 2, column=1, columnspan=2, sticky="ew", pady=(8, 0))

    message_queue: "queue.Queue[tuple[str, Any]]" = queue.Queue()
    worker_thread: Optional[threading.Thread] = None
    start_button: Optional[ttk.Button] = None
    progress_state = {"total": 0}

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
        nonlocal worker_thread, start_button

        if worker_thread and worker_thread.is_alive():
            messagebox.showinfo(
                "Fuar Katılımcı Aktarıcı",
                "Halihazırda bir aktarım işlemi çalışıyor.",
            )
            return

        csv_path = entry_vars["output_csv"].get().strip()
        xlsx_path = entry_vars["output_xlsx"].get().strip()
        base_url = entry_vars["base_url"].get().strip()
        ticket = entry_vars["ticket"].get().strip()

        if not base_url or not ticket:
            messagebox.showerror(
                "Eksik bilgi",
                "Temel API URL'si ve ticket değeri zorunludur.",
            )
            return

        try:
            page_size = int(entry_vars["page_size"].get() or 50)
        except ValueError:
            messagebox.showerror(
                "Geçersiz değer",
                "Sayfa başına kayıt sayısı tam sayı olmalıdır.",
            )
            return

        try:
            delay = float(entry_vars["delay"].get() or 0)
        except ValueError:
            messagebox.showerror(
                "Geçersiz değer",
                "Bekleme süresi sayısal bir değer olmalıdır.",
            )
            return

        try:
            retries = int(entry_vars["retries"].get() or 1)
        except ValueError:
            messagebox.showerror(
                "Geçersiz değer",
                "Yeniden deneme sayısı tam sayı olmalıdır.",
            )
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

        log_text.configure(state="normal")
        log_text.delete("1.0", "end")
        log_text.configure(state="disabled")

        status_var.set("İşlem başlatılıyor…")
        progress_state["total"] = 0
        progress.configure(mode="determinate", maximum=1)
        progress["value"] = 0
        if start_button:
            start_button.config(state="disabled")

        while not message_queue.empty():
            try:
                message_queue.get_nowait()
            except queue.Empty:
                break

        def worker() -> None:
            message_queue.put(("status", "Veriler çekiliyor…"))
            try:
                config = FetchConfig(
                    base_url=base_url,
                    ticket=ticket,
                    lang=entry_vars["lang"].get().strip() or "en",
                    query=entry_vars["query"].get(),
                    page_size=page_size,
                    delay=delay,
                    retries=retries,
                    first_letters=parse_letters(first_letter_value),
                    extra_params=extra_params,
                )
                df = fetch_all(
                    config,
                    progress_callback=lambda letter, page, letter_index, total_letters: message_queue.put(
                        (
                            "progress",
                            {
                                "letter": letter,
                                "page": page,
                                "letter_index": letter_index,
                                "total_letters": total_letters,
                            },
                        )
                    ),
                )
                if df.empty:
                    message_queue.put(
                        ("log", "Hiç katılımcı verisi alınamadı. Parametreleri kontrol edin."),
                    )
                else:
                    message_queue.put(("log", f"{len(df)} kayıt alındı."))
                    message_queue.put(
                        (
                            "log",
                            f"CSV dosyası kaydediliyor: {csv_path or 'exhibitors.csv'}",
                        )
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
                            ("log", "Google Drive yükleme şablonu çalıştırıldı (elle tamamlanmalı)."),
                        )
                        upload_to_google_drive(xlsx_path or "exhibitors.xlsx", drive_credentials)

                    graph_token = entry_vars["graph_token"].get().strip()
                    graph_recipient = entry_vars["graph_recipient"].get().strip()
                    if graph_token and graph_recipient:
                        message_queue.put(
                            ("log", "Microsoft Graph e-posta şablonu çalıştırıldı (elle tamamlanmalı)."),
                        )
                        send_via_microsoft_graph(
                            xlsx_path or "exhibitors.xlsx",
                            token=graph_token,
                            recipient=graph_recipient,
                        )

                message_queue.put(("status", "Tamamlandı"))
            except Exception as exc:  # pragma: no cover - GUI runtime errors
                message_queue.put(("error", str(exc)))
                message_queue.put(("status", "Hata"))
                LOGGER.exception("GUI worker failed: %s", exc)
            finally:
                message_queue.put(("finished", None))

        worker_thread = threading.Thread(target=worker, daemon=True)
        worker_thread.start()

    def poll_queue() -> None:
        nonlocal worker_thread, start_button
        try:
            while True:
                kind, payload = message_queue.get_nowait()
                if kind == "log" and payload is not None:
                    append_log(payload)
                elif kind == "status" and payload is not None:
                    status_var.set(payload)
                elif kind == "progress" and isinstance(payload, dict):
                    total_letters = payload.get("total_letters") or progress_state["total"] or 1
                    if total_letters != progress_state["total"]:
                        progress_state["total"] = total_letters
                        progress.configure(mode="determinate", maximum=max(total_letters, 1))
                    letter_index = payload.get("letter_index", 0)
                    progress["value"] = min(letter_index, progress_state["total"])
                    letter = payload.get("letter")
                    page = payload.get("page", 0)
                    if letter is None:
                        status_var.set("Tamamlandı")
                    else:
                        human_letter = letter or "(tümü)"
                        if page:
                            status_var.set(f"{human_letter} harfi - {page}. sayfa")
                        else:
                            status_var.set(f"{human_letter} harfi işleniyor")
                elif kind == "error" and payload is not None:
                    append_log(payload)
                    messagebox.showerror("Aktarım başarısız", payload)
                    status_var.set("Hata")
                elif kind == "finished":
                    if progress_state["total"]:
                        progress["value"] = progress_state["total"]
                    if start_button:
                        start_button.config(state="normal")
                    worker_thread = None
        except queue.Empty:
            pass
        root.after(150, poll_queue)

    buttons_frame = ttk.Frame(mainframe)
    buttons_frame.grid(row=log_label_row + 3, column=0, columnspan=3, pady=(12, 0), sticky="e")

    start_button = ttk.Button(buttons_frame, text="Verileri aktar", command=start_scrape)
    start_button.grid(row=0, column=0, padx=(0, 8))
    ttk.Button(buttons_frame, text="Kapat", command=root.destroy).grid(row=0, column=1)

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

    if args.gui:
        run_gui()
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
