# expo

## Exhibitor export utility

This repository contains a Python script, `interpack_scraper.py`, that exports
company data from Messe Düsseldorf exhibitor directories such as
[interpack.com](https://www.interpack.com/). The tool consumes the same JSON API
used by the website, iterating through the A–Z exhibitor index and collecting
all available records.

### Features

- Configurable base API endpoint, language, ticket and query parameters.
- Automatic pagination across all first-letter filters or a specific letter.
- Robust request handling with retries and an optional delay between requests.
- CSV and Excel exports generated from the collected exhibitor data.
- Türkçe Tkinter tabanlı grafik arayüz; ilerleme çubuğu ve durum mesajlarıyla süreç takibi.
- Placeholder hooks for future Google Drive or Microsoft Graph integrations.

### Basic usage

```bash
python interpack_scraper.py \
  --base-url https://www.interpack.com/vis/v1/api/searchResult \
  --output-csv interpack_exhibitors.csv \
  --output-xlsx interpack_exhibitors.xlsx
```

> **Note:** Respect the organiser's terms of service and robots.txt when using
> this script. Some fairs may require a different access ticket; inspect the
> browser developer tools to obtain the correct value and pass it via
> `--ticket`.

Run `python interpack_scraper.py --help` for the complete list of options.

### Türkçe kullanım rehberi

#### Komut satırı (CLI)

1. Python 3 ortamında gerekli paketleri yükleyin:

   ```bash
   pip install requests pandas openpyxl
   ```

2. Temel çekim için aşağıdaki komutu çalıştırın (çıktı dosya adlarını değiştirilebilir):

   ```bash
   python interpack_scraper.py \
     --base-url https://www.interpack.com/vis/v1/api/searchResult \
     --output-csv interpack_exhibitors.csv \
     --output-xlsx interpack_exhibitors.xlsx
   ```

3. Başka parametrelere ihtiyaç duyarsanız `--help` çıktısını inceleyin. `--ticket`, `--lang`, `--first-letter` gibi seçenekler istekleri özelleştirir.

#### Grafik arayüz (GUI)

1. Arayüzü açmak için:

   ```bash
   python interpack_scraper.py --gui
   ```

2. Açılan penceredeki alanlar Türkçe açıklamalar içerir. Temel API URL'si ve ticket alanlarını zorunlu olarak doldurun; diğer ayarları isteğe göre değiştirin.

3. `Verileri aktar` düğmesine bastığınızda durum satırı seçilen harf ve sayfa numarasını, ilerleme çubuğu ise toplam harf sayısındaki ilerlemeyi gösterir. Günlük alanında kaydedilen dosya yollarını ve olası uyarıları takip edebilirsiniz.

4. İşlem bittiğinde `Durum` alanı "Tamamlandı" olarak güncellenir ve CSV/Excel dosyaları belirtilen yollara kaydedilir. Hata oluşursa pencere uyarı mesajı gösterir ve log alanı ayrıntıları listeler.

5. Yeni bir çekim başlatmadan önce gerekli ayarları güncelleyin ve aynı düğmeye tekrar basın.

### Grafik arayüz

Komut satırı yerine görsel bir arayüz tercih ediyorsanız aşağıdaki komutu
çalıştırın:

```bash
python interpack_scraper.py --gui
```

Arayüz üzerinden API adresi, ticket değeri, sayfa boyutu gibi parametreleri
girip **Verileri aktar** düğmesine basmanız yeterlidir. Uygulama seçilen harfi ve
sayfa numarasını durum satırında, genel ilerlemeyi ise çubuk üzerinde gösterir;
işlem bittiğinde CSV/Excel dosyalarını belirttiğiniz yollara kaydeder.

### EXE üretimi

Windows üzerinde çalıştırılabilir (``.exe``) dosya oluşturmak için
[PyInstaller](https://pyinstaller.org/en/stable/) kullanabilirsiniz. Önce
bağımlılıkları yükleyin:

```bash
pip install requests pandas openpyxl pyinstaller
```

Ardından tek dosyalık, konsol penceresi açmayan bir paket üretmek için:

```bash
pyinstaller --onefile --windowed interpack_scraper.py
```

Oluşan çalıştırılabilir `dist/interpack_scraper.exe` altında bulunur ve
hem komut satırı hem de `--gui` parametresi ile açılan arayüzü destekler. PyInstaller
komutu yürütülürken Tkinter'in sisteminizde mevcut olduğundan emin olun.
