import os, time, json, shutil, re, io, sqlite3, hashlib, sys
import pandas as pd
import pymupdf as fitz
import pytesseract
from PIL import Image
from PyPDF2 import PdfReader, PdfWriter
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
import arabic_reshaper
from bidi.algorithm import get_display
import win32print
import win32api

# ==================================================
# BASE DIR (PyInstaller SAFE)
# ==================================================
if getattr(sys, 'frozen', False):
    BASE_DIR = sys._MEIPASS
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ==================================================
# TESSERACT
# ==================================================
TESSERACT_CMD = os.path.join(BASE_DIR, "tesseract", "tesseract.exe")
TESSDATA_DIR  = os.path.join(BASE_DIR, "tesseract", "tessdata")
pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD
os.environ["TESSDATA_PREFIX"] = TESSDATA_DIR
OCR_CONFIG = "-l eng+ara"

# ==================================================
# FONT
# ==================================================
FONT_PATH = os.path.join(BASE_DIR, "fonts", "arial.ttf")
pdfmetrics.registerFont(TTFont("Arabic", FONT_PATH))

# ==================================================
# CONFIG
# ==================================================
with open(os.path.join(BASE_DIR, "config.json"), "r", encoding="utf-8") as f:
    cfg = json.load(f)

INBOX = cfg["watch_folder"]
PROC  = cfg["processing_folder"]
DONE  = cfg["done_folder"]
ERR   = cfg["error_folder"]
EXCEL = cfg["excel_file"]
PRINTER = cfg["printer_name"]

for d in [INBOX, PROC, DONE, ERR]:
    os.makedirs(d, exist_ok=True)

# ==================================================
# LOAD PRODUCT & PESTICIDE LISTS
# ==================================================
products_df = pd.read_csv(os.path.join(BASE_DIR, "products_list.csv"))
PRODUCTS = [p.strip().lower() for p in products_df.iloc[:, 0].dropna()]

pesticides_df = pd.read_csv(os.path.join(BASE_DIR, "pesticides_list.csv"))
PESTICIDES = [p.strip().lower() for p in pesticides_df.iloc[:, 0].dropna()]

# ==================================================
# LOAD EXCEL (WAREHOUSES)
# ==================================================
sheets = pd.read_excel(EXCEL, sheet_name=None)
df = pd.concat(sheets.values(), ignore_index=True)

df["Lot_external"] = (
    df.iloc[:, 0].astype(str)
    .str.replace(".0", "", regex=False)
    .str.findall(r"\d+")
)
df = df.explode("Lot_external")
df = df[df["Lot_external"].notna()]
df["Lot_external"] = df["Lot_external"].astype(str).str.strip()

# ==================================================
# HASH DB
# ==================================================
db = sqlite3.connect(os.path.join(BASE_DIR, "certagent_log.db"))
cur = db.cursor()
cur.execute("""
CREATE TABLE IF NOT EXISTS processed (
    filehash TEXT PRIMARY KEY,
    filename TEXT,
    processed_at TEXT
)
""")
db.commit()

def file_hash(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def already_done(h):
    cur.execute("SELECT 1 FROM processed WHERE filehash=?", (h,))
    return cur.fetchone() is not None

def mark_done(h, name):
    cur.execute(
        "INSERT OR IGNORE INTO processed VALUES (?, ?, datetime('now'))",
        (h, name)
    )
    db.commit()

# ==================================================
# OCR
# ==================================================
def ocr_text(pdf_path):
    text = ""
    doc = fitz.open(pdf_path)
    try:
        for page in doc:
            pix = page.get_pixmap(dpi=300)
            img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
            text += pytesseract.image_to_string(img, config=OCR_CONFIG)
    finally:
        doc.close()
    return text

# ==================================================
# HEADER EXTRACTION (HUMAN LOGIC)
# ==================================================
def extract_header_fields(raw_text):
    cert = ""
    sample = ""
    lot = ""

    lines = raw_text.splitlines()

    for l in lines:
        ll = l.lower()

        if "certificate number" in ll and not cert:
            m = re.search(r"dokki[-\d]+", ll)
            if m:
                cert = m.group(0).replace("dokki", "Dokki")

        if "lot number" in ll and not lot:
            m = re.search(r"([0-9]+(?:\/[0-9]+)?)", l)
            if m:
                lot = m.group(1)

    # Sample from PRODUCTS whitelist
    for prod in PRODUCTS:
        if prod in raw_text.lower():
            sample = prod.title()
            break

    # Fallback: Sample :
    if not sample:
        for l in lines:
            if l.lower().startswith("sample :"):
                val = l.split(":", 1)[1]
                val = re.split(r"(fax|phone|id|\d)", val, flags=re.I)[0]
                sample = val.strip()
                break

    return {
        "CertificateNumber": cert,
        "Sample": sample,
        "LotNumber": lot
    }

# ==================================================
# RESULTS EXTRACTION (NO BREAK BUG)
# ==================================================
def extract_results_to_rows(raw_text, header):
    rows = []
    lines = raw_text.splitlines()
    capture = False

    for l in lines:
        ll = l.lower()

        if "results of analysis" in ll:
            capture = True
            continue

        if capture:
            if "measurement uncertainty" in ll:
                break

            if not ll.strip():
                continue

            # Whole certificate not detected
            if "not detected" in ll:
                rows.append({
                    **header,
                    "Analyte": "Pesticide Residues",
                    "Result": "Not detected",
                    "Unit": ""
                })
                return rows

            matched = False
            for pest in PESTICIDES:
                if pest in ll:
                    matched = True

                    if "<loq" in ll:
                        rows.append({
                            **header,
                            "Analyte": pest.title(),
                            "Result": "LOQ",
                            "Unit": ""
                        })
                    else:
                        m = re.search(r"([0-9]+\.[0-9]+)", ll)
                        if m:
                            rows.append({
                                **header,
                                "Analyte": pest.title(),
                                "Result": m.group(1),
                                "Unit": "mg/kg"
                            })

            if matched:
                continue

    return rows

# ==================================================
# PDF ANNOTATION
# ==================================================
def build_annotated_pdf(pdf_path, supplier, internal_lots_text):
    reader = PdfReader(pdf_path)
    writer = PdfWriter()

    packet = io.BytesIO()
    can = canvas.Canvas(packet, pagesize=A4)

    supplier_ar = get_display(arabic_reshaper.reshape(supplier))
    text = f"{internal_lots_text}  {supplier_ar}"

    font = "Arabic"
    size = 14
    can.setFont(font, size)

    width = pdfmetrics.stringWidth(text, font, size)
    x = 560
    y = 810

    can.setFillColorRGB(0.85, 0.85, 0.85)
    can.rect(x - width - 12, y - 4, width + 12, size + 8, fill=1, stroke=0)

    can.setFillColorRGB(0, 0, 0)
    can.drawRightString(x - 6, y, text)

    can.save()
    packet.seek(0)

    overlay = PdfReader(packet)
    page = reader.pages[0]
    page.merge_page(overlay.pages[0])
    writer.add_page(page)

    for i in range(1, len(reader.pages)):
        writer.add_page(reader.pages[i])

    out_pdf = os.path.join(
        DONE,
        os.path.basename(pdf_path).replace(".pdf", "_ANNOTATED.pdf")
    )

    with open(out_pdf, "wb") as f:
        writer.write(f)

    return out_pdf

# ==================================================
# PRINT
# ==================================================
def print_pdf(pdf_path, printer):
    try:
        default = win32print.GetDefaultPrinter()
        win32print.SetDefaultPrinter(printer)
        win32api.ShellExecute(0, "print", pdf_path, None, ".", 0)
        return True
    except:
        return False
    finally:
        try:
            win32print.SetDefaultPrinter(default)
        except:
            pass

# ==================================================
# MAIN LOOP
# ==================================================
print("=== CERT AGENT RUNNING ===")

while True:
    for file in os.listdir(INBOX):
        if not file.lower().endswith(".pdf"):
            continue

        src = os.path.join(INBOX, file)
        work = os.path.join(PROC, file)

        try:
            shutil.move(src, work)

            h = file_hash(work)
            if already_done(h):
                shutil.move(work, os.path.join(DONE, file))
                continue

            raw_text = ocr_text(work)

            header = extract_header_fields(raw_text)
            rows = extract_results_to_rows(raw_text, header)

            if rows:
                pd.DataFrame(rows).to_csv(
                    os.path.join(DONE, file.replace(".pdf", "_RESULTS.csv")),
                    index=False
                )

            external_lots = re.findall(r"\d+", raw_text)
            rows_xl = df[df["Lot_external"].isin(external_lots)]
            if rows_xl.empty:
                raise Exception("LOT NOT FOUND")

            supplier = str(rows_xl.iloc[0].iloc[3])
            internal_lots = rows_xl.iloc[:, 2].astype(str).unique()
            internal_text = " / ".join(internal_lots)

            annotated = build_annotated_pdf(work, supplier, internal_text)
            print_pdf(annotated, PRINTER)

            shutil.move(work, os.path.join(DONE, file))
            mark_done(h, file)

        except Exception as e:
            if os.path.exists(work):
                shutil.move(work, os.path.join(ERR, file))
            print("ERROR:", file, e)

    time.sleep(5)
