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
- Tkinter tabanlı grafik arayüz; ilerleme çubuğu ve iptal düğmesi ile işlemleri takip edebilirsiniz.
- Otomatik pip bağımlılık kurulumu (`requests`, `pandas`, `openpyxl`) ve proxy kullanımını devre dışı bırakma seçeneği.
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

Missing packages (for example on a fresh Python installation) are detected and
installed automatically. Add `--bootstrap-deps` if you want to force a clean
`pip --upgrade` run before exporting data.

Run `python interpack_scraper.py --help` for the complete list of options, including:

- `--bootstrap-deps` – install/upgrade all required Python packages before execution.
- `--upgrade-deps` – re-run `pip --upgrade` without reinstalling from scratch.
- `--disable-proxy` – ignore `HTTP(S)_PROXY` variables for both pip and API requests.

### Grafik arayüz

Komut satırı yerine görsel bir arayüz tercih ediyorsanız aşağıdaki komutu
çalıştırın:

```bash
python interpack_scraper.py --gui
```

Arayüz üzerinden API adresi, ticket değeri, sayfa boyutu gibi parametreleri
girip **Dışa aktarımı başlat** düğmesine basmanız yeterlidir. Uygulama ilerlemeyi
pencere içindeki log alanında gösterir ve işlem bittiğinde CSV/Excel dosyalarını
belirttiğiniz yollara kaydeder.

### Türkçe kullanım rehberi

1. **Bağımlılıkları hazırlayın.** Script eksik paketleri otomatik olarak indirir; ekstra
   kontrol için aşağıdaki komutlardan birini kullanabilirsiniz:

   - `python interpack_scraper.py --bootstrap-deps` tüm bağımlılıkları en güncel sürümleriyle
     indirir/günceller.
   - `python interpack_scraper.py --upgrade-deps` yalnızca güncelleme yapar.

   Proxy engellerinden kaçınmak için `--disable-proxy` ekleyebilirsiniz; bu seçenek hem pip
   komutlarında hem de API isteklerinde ortam değişkenlerindeki proxy ayarlarını yok sayar.

2. **Komut satırından çalıştırın.**
   ```bash
   python interpack_scraper.py \
     --base-url https://www.interpack.com/vis/v1/api/searchResult \
     --output-csv veriler/exhibitors.csv \
     --output-xlsx veriler/exhibitors.xlsx \
     --delay 1 --page-size 50 \
     --disable-proxy
   ```
   - `--ticket g_u_e_s_t` varsayılan misafir erişimidir; farklı bir token
     gerekiyorsa tarayıcı geliştirici araçlarından alıp parametreye ekleyin.
   - `--first-letter A` yalnızca seçtiğiniz harfle başlayan firmaları getirir.
   - Hata durumlarında script otomatik tekrar dener ve sonuç yoksa döngüyü
     sonlandırır.

3. **Grafik arayüzü açın.**
   ```bash
   python interpack_scraper.py --gui
   ```
   - Form alanlarını doldurup **Dışa aktarımı başlat** butonuna basın.
   - İsterseniz arayüzdeki "HTTP proxy ayarlarını yok say" ve "Gerekli Python paketlerini pip
     ile indir/güncelle" kutucuklarını işaretleyerek aynı ayarları GUI’den uygulayabilirsiniz.
   - İlerleme çubuğu hangi harfin tarandığını gösterir; günlük alanında ayrıntılı
     loglar Türkçe olarak listelenir.
   - **İptal et** düğmesi devam eden isteği sayfa tamamlandıktan sonra durdurur
     ve dosyalar kaydedilmez; ister yeniden başlatabilir ister parametreleri
     değiştirebilirsiniz.
   - İşlem başarıyla biterse durum satırı **Tamamlandı** olarak güncellenir ve
     dosya yolları logda görünür.

4. **Çıktıları kontrol edin.** CSV ve Excel dosyaları varsayılan olarak proje
   dizininde oluşur; `prepare_output_path` fonksiyonu dizinleri otomatik
   oluşturur. Dosyalar üzerinde ek işlemler yapmadan önce fuar sitesinin kullanım
   şartlarına ve robots.txt kurallarına uyduğunuzdan emin olun.

### EXE üretimi

Windows üzerinde çalıştırılabilir (``.exe``) dosya oluşturmak için
[PyInstaller](https://pyinstaller.org/en/stable/) kullanabilirsiniz. Önce
bağımlılıkları yükleyin (komut, GUI'yi açacak; pencere açıldıktan sonra
kapatarak devam edebilirsiniz):

```bash
python interpack_scraper.py --bootstrap-deps --disable-proxy --gui
pip install pyinstaller
```

Ardından tek dosyalık, konsol penceresi açmayan bir paket üretmek için:

```bash
pyinstaller --onefile --windowed interpack_scraper.py
```

Oluşan çalıştırılabilir `dist/interpack_scraper.exe` altında bulunur ve
hem komut satırı hem de `--gui` parametresi ile açılan arayüzü destekler. PyInstaller
komutu yürütülürken Tkinter'in sisteminizde mevcut olduğundan emin olun.
