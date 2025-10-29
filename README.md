# expo

Utility scripts for scraping exhibitor directories.

## ceramitec scraper

`ceramitec_scraper.py` downloads the ceramitec exhibitor catalogue and exports a
normalised CSV or Excel file.

```bash
python ceramitec_scraper.py --output ceramitec.csv
```

To write an Excel workbook install the optional `pandas` dependency (which
pulls in `openpyxl`) and use an `.xlsx` filename or the `--format xlsx` flag:

```bash
pip install pandas openpyxl
python ceramitec_scraper.py --output ceramitec.xlsx
```

### Command line options

* `--url`: Custom ceramitec listing URL (defaults to the standard full-text list).
* `--output`: Destination file path (defaults to `ceramitec_exhibitors.csv`).
* `--format`: Force either `csv` or `xlsx` output (otherwise inferred from the
  `--output` extension).
* `--retries`: Number of download retries (defaults to 3).
* `--delay`: Delay between retries in seconds (defaults to 1.0).
* `--verbose`: Enable debug logging.

The scraper only uses the Python standard library for CSV exports. Generating
Excel output requires the optional `pandas` dependency.
