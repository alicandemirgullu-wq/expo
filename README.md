# Expo scrapers

This repository collects standalone Python utilities for downloading exhibitor
information from different trade fair catalogues.

## Messe Düsseldorf exhibitor scraper

`messe_dusseldorf_scraper.py` is a compact command line utility for Messe
Düsseldorf catalogues (for example Caravan Salon, MEDICA, ProWein) that expose
their data through the `/vis/v1/` Nuxt payload.  It focuses on the essentials:
download the listing page, extract the embedded JSON, detect exhibitor records
and export the normalised result set.

### Features

- Downloads a single page and looks for the Nuxt payload injected into the
  markup—no browser automation required.
- Automatically detects the exhibitor list inside the payload by scanning for
  company-related keys.
- Normalises common fields such as company name, contact details, booth
  information and product categories.
- Writes both CSV and Excel exports using only the Python standard library.
- Keeps configuration intentionally simple: supply a page URL, optionally set a
  limit or custom user agent, and run the script.

### Usage

```bash
python messe_dusseldorf_scraper.py \
  --page-url "https://www.caravan-salon.com/vis/v1/en/exhprofiles/" \
  --output-csv caravan.csv \
  --output-xlsx caravan.xlsx \
  --limit 100 \
  --verbose
```

The script expects the catalogue page to embed a Nuxt payload.  When that is the
case, the resulting files contain the columns listed under `OUTPUT_HEADERS` in
the script (company name, profile URL, address, country, phone, email, website,
hall, stand, categories and the source page).

## Expomed Istanbul scraper

`interpack_scraper.py` remains available for the Expomed Istanbul exhibitor
list.  It implements a similar pipeline but is tailored to the WordPress REST
API and data layout used on that website.
