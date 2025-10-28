# Expo scrapers

This repository collects standalone Python utilities for downloading exhibitor
information from different trade fair catalogues.

## Messe Düsseldorf exhibitor scraper

`messe_dusseldorf_scraper.py` targets Messe Düsseldorf directories (for example,
Caravan Salon, MEDICA, ProWein) that expose their data through the `/vis/v1/`
Nuxt payload.  The script mirrors the behaviour of the public Apify actor and
exports CSV and Excel files without third-party dependencies.

### Features

- Parses embedded JSON blobs or, if necessary, falls back to HTML card scraping.
- Normalises company details, social links, product categories, and contact
  persons.
- Supports **compact** (single row per exhibitor) and **expanded** (one row per
  product category) output layouts.
- Writes both CSV and XLSX using the Python standard library only.

### Usage

```bash
python messe_dusseldorf_scraper.py \
  --page-url "https://www.caravan-salon.com/vis/v1/en/exhprofiles/" \
  --output-format expanded \
  --output-csv caravan.csv \
  --output-xlsx caravan.xlsx
```

## Expomed Istanbul scraper

`interpack_scraper.py` remains available for the Expomed Istanbul exhibitor
list.  It implements a similar pipeline but is tailored to the WordPress REST
API and data layout used on that website.
