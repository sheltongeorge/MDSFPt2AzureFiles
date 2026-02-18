import os
import csv
import zipfile
import re
import argparse
import tempfile
import pandas as pd
import json

# ---------------------------------------------------------------------
# Embedded lookup data (ALL rows you provided; no external files needed)
# ---------------------------------------------------------------------

cover_data = [
    {"binding": "PB", "lamination": "Gloss", "pt": "12pt", "final_size": "less than or equal to 6x9", "is_pod": "no", "spot_uv_pdf": "none", "ticket_template": "*CVR C1S12PT GLOSS sm"},
    {"binding": "PB", "lamination": "Gloss", "pt": "12pt", "final_size": "6x9 to 8.5x11", "is_pod": "no", "spot_uv_pdf": "none", "ticket_template": "*CVR C1S12PT GLOSS md"},
    {"binding": "PB", "lamination": "Gloss", "pt": "12pt", "final_size": "greater than 8.5x11", "is_pod": "no", "spot_uv_pdf": "none", "ticket_template": "*CVR C1S12PT Gloss Oversized"},
    {"binding": "PB", "lamination": "Gloss", "pt": "10pt", "final_size": "less than or equal to 6x9", "is_pod": "yes", "spot_uv_pdf": "none", "ticket_template": "*CVR C1S10PT Gloss sm POD"},
    {"binding": "PB", "lamination": "Gloss", "pt": "10pt", "final_size": "6x9 to 8.5x11", "is_pod": "yes", "spot_uv_pdf": "none", "ticket_template": "*CVR C1S10PT Gloss md POD"},
    {"binding": "PB", "lamination": "Gloss", "pt": "10pt", "final_size": "less than or equal to 6x9", "is_pod": "no", "spot_uv_pdf": "none", "ticket_template": "*CVR C1S10PT Gloss sm"},
    {"binding": "PB", "lamination": "Gloss", "pt": "10pt", "final_size": "6x9 to 8.5x11", "is_pod": "no", "spot_uv_pdf": "none", "ticket_template": "*CVR C1S10PT Gloss md"},
    {"binding": "PB", "lamination": "Gloss", "pt": "10pt", "final_size": "greater than 8.5x11", "is_pod": "no", "spot_uv_pdf": "none", "ticket_template": "*CVR C1S10PT Gloss Oversized"},
    {"binding": "PB", "lamination": "Matte", "pt": "12pt", "final_size": "less than or equal to 6x9", "is_pod": "no", "spot_uv_pdf": "yes", "ticket_template": "*CVR C1S12PT MATTE sm SPOTUV"},
    {"binding": "PB", "lamination": "Matte", "pt": "12pt", "final_size": "6x9 to 8.5x11", "is_pod": "no", "spot_uv_pdf": "yes", "ticket_template": "*CVR C1S12PT MATTE md SPOTUV"},
    {"binding": "PB", "lamination": "Matte", "pt": "12pt", "final_size": "greater than 8.5x11", "is_pod": "no", "spot_uv_pdf": "yes", "ticket_template": "*CVR C1S12PT MATTE Oversized SPOTUV"},
    {"binding": "PB", "lamination": "Matte", "pt": "12pt", "final_size": "less than or equal to 6x9", "is_pod": "no", "spot_uv_pdf": "none", "ticket_template": "*CVR C1S12PT MATTE sm"},
    {"binding": "PB", "lamination": "Matte", "pt": "12pt", "final_size": "6x9 to 8.5x11", "is_pod": "no", "spot_uv_pdf": "none", "ticket_template": "*CVR C1S12PT MATTE md"},
    {"binding": "PB", "lamination": "Matte", "pt": "12pt", "final_size": "greater than 8.5x11", "is_pod": "no", "spot_uv_pdf": "none", "ticket_template": "*CVR C1S12PT MATTE OVERSIZED"},
    {"binding": "PB", "lamination": "Matte", "pt": "10pt", "final_size": "less than or equal to 6x9", "is_pod": "yes", "spot_uv_pdf": "none", "ticket_template": "*CVR C1S10PT Matte sm POD"},
    {"binding": "PB", "lamination": "Matte", "pt": "10pt", "final_size": "6x9 to 8.5x11", "is_pod": "yes", "spot_uv_pdf": "none", "ticket_template": "*CVR C1S10PT Matte md POD"},
    {"binding": "PB", "lamination": "Matte", "pt": "10pt", "final_size": "less than or equal to 6x9", "is_pod": "no", "spot_uv_pdf": "yes", "ticket_template": "*CVR C1S10PT Matte sm SPOTUV"},
    {"binding": "PB", "lamination": "Matte", "pt": "10pt", "final_size": "6x9 to 8.5x11", "is_pod": "no", "spot_uv_pdf": "yes", "ticket_template": "*CVR C1S10PT Matte md SPOTUV"},
    {"binding": "PB", "lamination": "Matte", "pt": "10pt", "final_size": "greater than 8.5x11", "is_pod": "no", "spot_uv_pdf": "yes", "ticket_template": "*CVR C1S10PT Matte Oversized SPOTUV"},
    {"binding": "PB", "lamination": "Matte", "pt": "10pt", "final_size": "less than or equal to 6x9", "is_pod": "no", "spot_uv_pdf": "none", "ticket_template": "*CVR C1S10PT Matte sm"},
    {"binding": "PB", "lamination": "Matte", "pt": "10pt", "final_size": "6x9 to 8.5x11", "is_pod": "no", "spot_uv_pdf": "none", "ticket_template": "*CVR C1S10PT Matte md"},
    {"binding": "PB", "lamination": "Matte", "pt": "10pt", "final_size": "greater than 8.5x11", "is_pod": "no", "spot_uv_pdf": "none", "ticket_template": "*CVR C1S10PT Matte Oversized"},
    {"binding": "PB", "lamination": "Soft Touch", "pt": "12pt", "final_size": "less than or equal to 6x9", "is_pod": "no", "spot_uv_pdf": "none", "ticket_template": "*CVR C1S12PT Soft sm SPOTUV"},
    {"binding": "SS", "lamination": "Matte", "pt": "10pt", "final_size": "less than or equal to 6x9", "is_pod": "no", "spot_uv_pdf": "yes", "ticket_template": "*SS CVR C1S10PT Matte ≤6x9 SPOTUV"},
    {"binding": "SS", "lamination": "Matte", "pt": "10pt", "final_size": "less than or equal to 6x9", "is_pod": "no", "spot_uv_pdf": "none", "ticket_template": "*SS CVR C1S10PT MATTE ≤6x9"},
    {"binding": "PB", "lamination": "Gloss", "pt": "10pt", "final_size": "Any", "is_pod": "yes", "spot_uv_pdf": "none", "ticket_template": "*CVR 4/4 C1S10PT Gloss sm POD"},
    {"binding": "PB", "lamination": "Matte", "pt": "10pt", "final_size": "Any", "is_pod": "yes", "spot_uv_pdf": "none", "ticket_template": "*CVR 4/4 C1S10PT Matte sm POD"},
    {"binding": "HB", "lamination": "Matte", "pt": "12pt", "final_size": "Any", "is_pod": "no", "spot_uv_pdf": "none", "ticket_template": "*HB/PB CVR MATTE (Must Hardcode Price)"},
    {"binding": "HB", "lamination": "Matte", "pt": "12pt", "final_size": "Any", "is_pod": "no", "spot_uv_pdf": "yes", "ticket_template": "*HB/PB CVR C1S12PT MATTE md SPOTUV"},
]

guts_data = [
    {"binding": "PB", "guts_paper": "Medium Art Gloss", "color_type": "4-Apr", "final_size": "Any", "ticket_template": "*TXT 4/4 Med Art Gloss (Imposed File)"},
    {"binding": "PB", "guts_paper": "Heavy Art Gloss", "color_type": "4-Apr", "final_size": "Any", "ticket_template": "*TXT 4/4 Heavy Art Gloss (Imposed File)"},
    {"binding": "HB", "guts_paper": "Medium Art Gloss", "color_type": "4-Apr", "final_size": "Any", "ticket_template": "*HB/PB TXT 4/4 Med Art Gloss (Imposed File)"},
    {"binding": "HB", "guts_paper": "Heavy Art Gloss", "color_type": "4-Apr", "final_size": "Any", "ticket_template": "*HB/PB TXT 4/4 Heavy Art Gloss (Imposed File)"},
    {"binding": "PB", "guts_paper": "Medium Art Satin", "color_type": "4-Apr", "final_size": "Any", "ticket_template": "*TXT 4/4 Med Art Satin (Imposed File)"},
    {"binding": "PB", "guts_paper": "Heavy Art Satin", "color_type": "4-Apr", "final_size": "Any", "ticket_template": "*TXT 4/4 Heavy Art Satin (Imposed File)"},
    {"binding": "HB", "guts_paper": "Medium Art Satin", "color_type": "4-Apr", "final_size": "Any", "ticket_template": "*HB/PB TXT 4/4 Med Art Satin (Imposed File)"},
    {"binding": "HB", "guts_paper": "Heavy Art Satin", "color_type": "4-Apr", "final_size": "Any", "ticket_template": "*HB/PB TXT 4/4 Heavy Art Satin (Imposed File)"},
    {"binding": "PB", "guts_paper": "Standard", "color_type": "1-Jan", "final_size": "less than or equal to 6x9", "ticket_template": "*TXT 1/1 60# White 0.0045 sm"},
    {"binding": "PB", "guts_paper": "Standard", "color_type": "1-Jan", "final_size": "6x9 to 8.5x11", "ticket_template": "*TXT 1/1 60# White 0.0045 md"},
    {"binding": "PB", "guts_paper": "Fine", "color_type": "4-Apr", "final_size": "less than or equal to 6x9", "ticket_template": "*TXT 4/4 65# Coated 0.0048 sm"},
    {"binding": "PB", "guts_paper": "Fine", "color_type": "4-Apr", "final_size": "6x9 to 8.5x11", "ticket_template": "*TXT 4/4 65# Coated 0.0048 md"},
    {"binding": "PB", "guts_paper": "Thin", "color_type": "1-Jan", "final_size": "less than or equal to 6x9", "ticket_template": "*TXT 1/1 50# Uncoated 0.0038 sm"},
    {"binding": "PB", "guts_paper": "Thin", "color_type": "1-Jan", "final_size": "6x9 to 8.5x11", "ticket_template": "*TTXT 1/1 50# Hi Bulk Cream 0.0045 smXT 1/1 50# Uncoated 0.0038 md"},
    {"binding": "SS", "guts_paper": "Standard", "color_type": "1-Jan", "final_size": "less than or equal to 6x9", "ticket_template": "*SS TXT 1/1 60# White 0.0045 sm"},
    {"binding": "SS", "guts_paper": "Standard", "color_type": "1-Jan", "final_size": "6x9 to 8.5x11", "ticket_template": "*SS TXT 1/1 60# White 0.0045 md"},
    {"binding": "SS", "guts_paper": "Fine", "color_type": "4-Apr", "final_size": "less than or equal to 6x9", "ticket_template": "*SS TXT 4/4 65# Coated 0.0048 sm"},
    {"binding": "SS", "guts_paper": "Medium Art Satin", "color_type": "4-Apr", "final_size": "6x9 to 8.5x11", "ticket_template": "*SS TXT 4/4 80# Dull md"},
    {"binding": "HB", "guts_paper": "Fine", "color_type": "4-Apr", "final_size": "6x9 to 8.5x11", "ticket_template": "*HB/PB TXT 4/4 65# md"},
    {"binding": "HB", "guts_paper": "Fine", "color_type": "4-Apr", "final_size": "less than or equal to 6x9", "ticket_template": "*HB/PB TXT 4/4 65# sm"},
    {"binding": "HB", "guts_paper": "Standard", "color_type": "1-Jan", "final_size": "less than or equal to 6x9", "ticket_template": "*HB/PB TXT 1/1 60# sm"},
    {"binding": "HB", "guts_paper": "Standard", "color_type": "1-Jan", "final_size": "6x9 to 8.5x11", "ticket_template": "*HB/PB TXT 1/1 WEB60 20\" (6x9–8.5x11)"},
    {"binding": "PB", "guts_paper": "White Hi Bulk", "color_type": "1-Jan", "final_size": "less than or equal to 6x9", "ticket_template": "*TXT 1/1 50# Hi Bulk White 0.0045 sm"},
    {"binding": "PB", "guts_paper": "Cream Hi Bulk", "color_type": "1-Jan", "final_size": "less than or equal to 6x9", "ticket_template": "*TXT 1/1 50# Hi Bulk Cream 0.0045 sm"},
    {"binding": "PB", "guts_paper": "White Hi Bulk", "color_type": "1-Jan", "final_size": "6x9 to 8.5x11", "ticket_template": "*TXT 1/1 65# Coated 0.0048 md"},
]

cover_table = pd.DataFrame(cover_data)
guts_table = pd.DataFrame(guts_data)

import fitz  # PyMuPDF

def get_pdf_info(pdf_path, sample_pages=2, zoom=0.25, color_threshold=10):
    """
    Return (page_count, is_color) for a PDF.
    'is_color' means: there exists a noticeable amount of true color
    (R,G,B channels differ by more than `color_threshold` on any pixel).
    """
    doc = fitz.open(pdf_path)
    pages = doc.page_count
    is_color = False

    try:
        to_check = min(pages, sample_pages)
        mat = fitz.Matrix(zoom, zoom)

        for i in range(to_check):
            page = doc[i]
            pix = page.get_pixmap(matrix=mat, alpha=False)  # RGB samples
            s = pix.samples  # bytes, length = w*h*3

            # Scan quickly for any pixel with meaningful channel separation
            # (avoid numpy dependency)
            for j in range(0, len(s), 3):
                r = s[j]
                g = s[j + 1]
                b = s[j + 2]
                if abs(r - g) > color_threshold or abs(g - b) > color_threshold or abs(r - b) > color_threshold:
                    is_color = True
                    break

            if is_color:
                break

    finally:
        doc.close()

    return pages, is_color



# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

FIELDNAMES = [
    'Name','DisplayName','Type','ProductId','Icon','DetailImage','Active','TurnAroundTime',
    'TurnAroundTimeUnit','QuantityType','MaxOrderQuantityPermitted','Quantities',
    'MaxQuantityPerSubcontainer','WeightValue','WeightUnit','WidthValue',
    'LengthValue','HeightValue','DimensionUnit','ContentFile','TicketTemplate',
    'ProductNameToCopySecuritySettings','Storefront/Categories'
]

def parse_metadata_json(meta_json):
    books = []
    for item in meta_json:
        md_list = item.get("metadata") or []
        for md in md_list:
            # skip non-book metadata blobs
            if not isinstance(md, dict):
                continue
            # only accept ones that actually have the fields we need
            if "title" not in md or "isbn" not in md:
                continue

            upload_type = str(md.get("uploadType1", "")).strip()

            book = {
                "title": str(md.get("title", "")).strip(),
                "isbn": str(md.get("isbn", "")).strip(),
                "final size": str(md.get("finalSize", "")).strip(),
                "binding": str(md.get("binding1", "")).strip(),
                "lamination": str(md.get("lamination1", "")).strip(),
                "cover paper": str(md.get("coverPaper1", "")).strip(),
                "guts paper": str(md.get("gutsPaper1", "")).strip(),
                "embellishment": str(md.get("embellishment", "")).strip(),
                "upload type": upload_type,
                "is pod": "yes" if upload_type.strip().lower() == "print on demand" else "no",
            }
            books.append(book)
    return books


def normalize_binding(raw: str) -> str:
    s = (raw or "").strip().lower()
    if 'perfect' in s or 'paper' in s or s in {'pb'}:
        return 'PB'
    if 'hard' in s or s == 'hb':
        return 'HB'
    if 'saddle' in s or s == 'ss':
        return 'SS'
    return raw.strip() if raw else ''

def normalize_lamination(raw: str) -> str:
    s = (raw or '').strip().lower()
    if 'gloss' in s:
        return 'Gloss'
    if 'soft' in s:
        return 'Soft Touch'
    if 'matte' in s:
        return 'Matte'
    return raw.strip() if raw else ''

def normalize_cover_pt(raw: str) -> str:
    s = (raw or '').strip().lower()
    if '12' in s:
        return '12pt'
    if '10' in s:
        return '10pt'
    return raw.strip() if raw else ''

def parse_size_label(size_str: str) -> str:
    """
    Map numeric WxH (e.g., '5.5 x 8.5', '6x9', '8.5 x 11') into the table's
    textual buckets: 'less than or equal to 6x9', '6x9 to 8.5x11', 'greater than 8.5x11'.
    """
    if not size_str:
        return "Any"
    nums = [float(x) for x in re.findall(r"[\d.]+", size_str)]
    if len(nums) < 2:
        return "Any"
    w, h = sorted(nums[:2])  # ensure w <= h
    # <= 6x9 means w <= 6 and h <= 9
    if w <= 6.0 and h <= 9.0:
        return "less than or equal to 6x9"
    # 6x9 to 8.5x11 inclusive upper: w <= 8.5 and h <= 11, but not already in <=6x9
    if w <= 8.5 and h <= 11.0:
        return "6x9 to 8.5x11"
    # otherwise greater than 8.5x11
    return "greater than 8.5x11"

def detect_has_spot(raw_embellishment: str) -> bool:
    return 'spot' in (raw_embellishment or '').strip().lower()

def detect_pod(raw_is_pod: str) -> bool:
    s = (raw_is_pod or "").strip().lower()
    return s in {"true", "yes", "y"} or "print on demand" in s

def normalize_guts_paper(raw: str) -> str:
    s = (raw or '').strip().lower()
    # Map specs terms to table terms
    if 'natural' in s:
        return 'Cream Hi Bulk'
    if 'standard' in s:
        return 'Standard'
    if 'fine' in s:
        return 'Fine'
    if 'thin' in s:
        return 'Thin'
    if 'white hi bulk' in s:
        return 'White Hi Bulk'
    if 'cream hi bulk' in s:
        return 'Cream Hi Bulk'
    if 'art gloss' in s:
        return 'Medium Art Gloss' if 'medium' in s else 'Heavy Art Gloss' if 'heavy' in s else 'Medium Art Gloss'
    if 'art satin' in s:
        return 'Medium Art Satin' if 'medium' in s else 'Heavy Art Satin' if 'heavy' in s else 'Medium Art Satin'
    # Publisher words
    if 'publisher' in s:
        if 'standard' in s:
            return 'Standard'
        if 'fine' in s:
            return 'Fine'
        if 'thin' in s:
            return 'Thin'
    # fallback
    return (raw or '').strip()

def infer_color_type_for_guts(guts_paper_raw: str, explicit_color: str = "") -> str:
    """
    Color type comes ONLY from metadata.json:
    - If gutsPaper1 == "Standard Publisher" -> 1/1 (B&W)
    - Else -> 4/4 (Color)
    Returns your internal tokens: '1-Jan' (1/1) or '4-Apr' (4/4).
    """
    gp = (guts_paper_raw or "").strip().lower()
    if gp == "standard publisher".lower():
        return "1-Jan"   # 1/1
    return "4-Apr"       # 4/4


def first_letters_for_db_category(title: str) -> str:
    """
    DB category initial(s):
    - usually first letter of the title
    - if starts with 'The ', include 'T' and the first letter of the next word, e.g., 'DB S, DB T'
      per instructions: 'include both T and the first letter of the next word'
    We'll implement as: 'DB X' OR 'DB X, DB T' (with the two tags).
    """
    if not title:
        return ""
    t = title.strip()
    parts = t.split()
    if len(parts) >= 2 and parts[0].lower() == 'the':
        return f"{parts[1][0].upper()}, DB T"
    return f"{t[0].upper()}"

def lookup_cover_template(binding, lamination, pt, size_cat, pod, has_spot) -> str:
    df = cover_table.copy()
    df = df[
        (df["binding"].str.lower() == binding.lower()) &
        (df["lamination"].str.lower() == lamination.lower()) &
        (df["pt"].str.lower() == pt.lower()) &
        # match exact size OR an 'Any' rule
        (df["final_size"].str.lower().isin([size_cat.lower(), "any"])) &
        (df["is_pod"].str.lower() == ("yes" if pod else "no")) &
        # spot rule: if has_spot -> must be 'yes'; else allow 'none' or 'no'
        (df["spot_uv_pdf"].str.lower().isin(["yes"] if has_spot else ["none", "no"]))
    ]
    return df.iloc[0]["ticket_template"] if len(df) else ""

def lookup_guts_template(binding, guts_paper, color_type, size_cat) -> str:
    df = guts_table.copy()
    # guts_paper can be a partial match (e.g., 'Cream Hi Bulk' for 'Natural' in specs)
    df = df[
        (df["binding"].str.lower() == binding.lower()) &
        (df["guts_paper"].str.lower().str.contains(guts_paper.lower(), na=False)) &
        # color_type in table is '1-Jan' or '4-Apr'; accept prefix match on '1' or '4'
        (df["color_type"].str.lower().str.startswith(color_type.split('-')[0].lower(), na=False)) &
        (df["final_size"].str.lower().isin([size_cat.lower(), "any"]))
    ]
    return df.iloc[0]["ticket_template"] if len(df) else ""

def turnaround_days(binding, color_is_color: bool, has_spot: bool, size_cat: str) -> int:
    days = 5
    if color_is_color:
        days += 1
    if has_spot:
        days += 1
    b = binding.lower()
    if 'hb' in b or 'hard' in b:
        days += 5
    if 'ss' in b or 'saddle' in b:
        days += 3
    if 'greater' in size_cat.lower():  # oversized
        days += 1
    return days

# ---------------------------------------------------------------------
# CSV row generation
# ---------------------------------------------------------------------
def detect_assets_for_books(folder_path: str, books: list) -> list:
    """
    Returns a list of dicts parallel to `books`, each with:
      {'cover': <filename>, 'guts': <filename>, 'spot': <filename|''>, 'image': <filename|''>}
    Uses your content-based PDF rules (pages/color) and image extension rules.
    """
    # collect file names
    pdf_names = [f for f in os.listdir(folder_path) if f.lower().endswith(".pdf")]
    # img_names = [f for f in os.listdir(folder_path) if f.lower().endswith((".png", ".jpg", ".jpeg"))]

    if not pdf_names:
        raise ValueError("No PDF files found in folder.")
    # if not img_names:
    #     raise ValueError("No product image file found (.png, .jpg, .jpeg).")

    # precompute pdf info once
    pdf_info = []
    for name in pdf_names:
        path = os.path.join(folder_path, name)
        pages, is_color = get_pdf_info(path)
        pdf_info.append({"name": name, "pages": pages, "color": is_color})

    # partition
    guts_pool = [p for p in pdf_info if p["pages"] > 5]
    cover_pool = [p for p in pdf_info if p["pages"] <= 5 and p["color"]]
    spot_pool  = [p for p in pdf_info if p["pages"] <= 5 and not p["color"]]

    if len(guts_pool) < len(books):
        raise ValueError(f"Not enough guts PDFs (>5 pages). Found {len(guts_pool)}, need {len(books)}.")
    if len(cover_pool) < len(books):
        raise ValueError(f"Not enough cover PDFs (<=5 pages + color). Found {len(cover_pool)}, need {len(books)}.")

    # image assignment: if exactly one image, reuse it for all books; else must match book count
    # if len(img_names) == 1:
    #     img_for_books = [img_names[0]] * len(books)
    # elif len(img_names) == len(books):
    #     img_for_books = img_names[:]  # order-dependent; adjust if you have a better rule
    # else:
    #     raise ValueError(
    #         f"Found {len(img_names)} image files, but {len(books)} books. "
    #         f"Expected 1 image or exactly {len(books)} images."
    #     )

    # Assign per book (greedy). Spot is optional: only if enough spot files exist.
    assets = []
    used = set()

    for i in range(len(books)):
        # pick a guts
        guts = next(p for p in guts_pool if p["name"] not in used)
        used.add(guts["name"])

        # pick a cover
        cover = next(p for p in cover_pool if p["name"] not in used)
        used.add(cover["name"])

        # pick spot if available
        spot = next((p for p in spot_pool if p["name"] not in used), None)
        if spot:
            used.add(spot["name"])
            spot_name = spot["name"]
        else:
            spot_name = ""

        assets.append({
            "guts": guts["name"],
            "cover": cover["name"],
            "spot": spot_name,
            # "image": img_for_books[i],
        })

    return assets


def make_csv_rows(customer, books, assets_per_book):
    rows = []
    for idx, book in enumerate(books):
        # pull + normalize
        title = (book.get('title') or "").strip()
        isbn = (book.get('isbn') or "").strip()
        raw_size = (book.get('final size') or "").strip()
        size_cat = parse_size_label(raw_size)

        binding = normalize_binding(book.get('binding') or "")
        lamination = normalize_lamination(book.get('lamination') or "")
        cover_pt = normalize_cover_pt(book.get('cover paper') or "")

        guts_paper_raw = (book.get('guts paper') or "").strip()
        guts_paper_norm = normalize_guts_paper(guts_paper_raw)

        color_type = infer_color_type_for_guts(guts_paper_raw)  # <-- use RAW value

        pod = detect_pod(book.get('is pod') or "")

        assets = assets_per_book[idx]  # <-- add idx from enumerate
        cover_file = assets["cover"]
        guts_file  = assets["guts"]
        spot_file  = assets["spot"]
        # image_file = assets["image"]

        has_spot = bool(spot_file)

        # Ticket lookups
        cover_ticket = lookup_cover_template(binding, lamination, cover_pt, size_cat, pod, has_spot)
        guts_ticket = lookup_guts_template(binding, guts_paper_norm, color_type, size_cat)


        # --- COVER (Document) ---
        cover_name = (
            f"{title} {binding} CVR w/ SpotUV {raw_size}"
            if has_spot else
            f"{title} {binding} CVR {raw_size}"
        )
        cover_display_name = f"{title} {binding} (Cover)"
        rows.append({
            'Name': cover_name,
            'DisplayName': cover_display_name,
            'Type': 'Document',
            'ProductId': isbn,
            'Icon': 'AutoThumbnail',
            'DetailImage': 'AutoThumbnail',
            'Active': 'TRUE',
            'TurnAroundTime': '',
            'TurnAroundTimeUnit': '',
            'QuantityType': '',
            'MaxOrderQuantityPermitted': '',
            'Quantities': '',
            'MaxQuantityPerSubcontainer': '1',
            'WeightValue': '1',
            'WeightUnit': 'oz',
            'WidthValue': '',
            'LengthValue': '',
            'HeightValue': '.25',
            'DimensionUnit': 'Inches',
            'ContentFile': cover_file,
            'TicketTemplate': cover_ticket,
            'ProductNameToCopySecuritySettings': '',
            'Storefront/Categories': 'Rework Book Covers'
        })

        # --- GUTS (Document) ---
        guts_name = f"{title} {binding} TXT {'4/4' if color_type.startswith('4') else '1/1'} {raw_size}"
        guts_display_name = f"{title} {binding} (Text)"
        rows.append({
            'Name': guts_name,
            'DisplayName': guts_display_name,
            'Type': 'Document',
            'ProductId': isbn,
            'Icon': 'AutoThumbnail',
            'DetailImage': 'AutoThumbnail',
            'Active': 'TRUE',
            'TurnAroundTime': '',
            'TurnAroundTimeUnit': '',
            'QuantityType': '',
            'MaxOrderQuantityPermitted': '',
            'Quantities': '',
            'MaxQuantityPerSubcontainer': '1',
            'WeightValue': '1',
            'WeightUnit': 'oz',
            'WidthValue': '',
            'LengthValue': '',
            'HeightValue': '.25',
            'DimensionUnit': 'Inches',
            'ContentFile': guts_file,
            'TicketTemplate': guts_ticket,
            'ProductNameToCopySecuritySettings': '',
            'Storefront/Categories': 'Rework Book Guts'
        })

        # --- SPOT UV (Document, optional) ---
        if has_spot:
            spot_name = f"{title} SPOTUV {binding} {raw_size}"
            spot_display_name = f"{title} {binding} (SpotUV)"
            rows.append({
                'Name': spot_name,
                'DisplayName': spot_display_name,
                'Type': 'Document',
                'ProductId': isbn,
                'Icon': 'AutoThumbnail',
                'DetailImage': 'AutoThumbnail',
                'Active': 'TRUE',
                'TurnAroundTime': '',
                'TurnAroundTimeUnit': '',
                'QuantityType': '',
                'MaxOrderQuantityPermitted': '',
                'Quantities': '',
                'MaxQuantityPerSubcontainer': '1',
                'WeightValue': '1',
                'WeightUnit': 'oz',
                'WidthValue': '',
                'LengthValue': '',
                'HeightValue': '.25',
                'DimensionUnit': 'Inches',
                'ContentFile': spot_file,
                'TicketTemplate': '*SPOTUV 13x19',
                'ProductNameToCopySecuritySettings': '',
                'Storefront/Categories': ''
            })

        # --- KIT ---
        kit_name = f"Book {title} {binding} {raw_size}"
        kit_display_name = f"{title} {binding}"
        t_days = turnaround_days(binding, color_type.startswith('4'), has_spot, size_cat)
        qty_type = "Any" if (customer == "Emerging Bookshelf" or pod) else "Multiple"
        storefront = "DESERET BOOK STORE Storefront"

        rows.append({
            'Name': kit_name,
            'DisplayName': kit_display_name,
            'Type': 'Kit',
            'ProductId': isbn,
            'Icon': cover_file,  #If we want this to take the screenshot Product Image instead (like we used to), then use image_file instead. I will leave all the logic in place to do this just in case.
            'DetailImage': cover_file,
            'Active': 'TRUE',
            'TurnAroundTime': t_days,
            'TurnAroundTimeUnit': 'Day',
            'QuantityType': qty_type,
            'MaxOrderQuantityPermitted': '5000',
            'Quantities': '' if qty_type == 'Any' else '3-5000-3',
            'MaxQuantityPerSubcontainer': '1',
            'WeightValue': '1',
            'WeightUnit': 'oz',
            'WidthValue': '',
            'LengthValue': '',
            'HeightValue': '.25',
            'DimensionUnit': 'Inches',
            'ContentFile': '',
            'TicketTemplate': '',
            'ProductNameToCopySecuritySettings': 'zz DO NOT DELETE - DB Security Settings Template',
            'Storefront/Categories': storefront
        })
    return rows

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def process_folder(folder_path: str) -> str:
    base = os.path.basename(folder_path.rstrip("/\\"))
    customer = "Deseret Book"
    all_files = os.listdir(folder_path)

    # --- read metadata.json instead of specs .txt ---
    meta_path = os.path.join(folder_path, "metadata.json")
    if not os.path.exists(meta_path):
        raise FileNotFoundError("metadata.json not found in folder.")

    with open(meta_path, "r", encoding="utf-8") as f:
        meta_json = json.load(f)

    books = parse_metadata_json(meta_json)  # or parse_specs_txt(...)
    assets_per_book = detect_assets_for_books(folder_path, books)
    rows = make_csv_rows(customer, books, assets_per_book)

    # Name outputs from the first book title when available; fallback to folder name.
    title = (books[0].get("title") or "").strip() if books else ""
    output_stem = title or base

    csv_path = os.path.join(folder_path, f"{output_stem}.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    return csv_path, output_stem


def create_zip_of_folder(folder_path: str, zip_name: str = "package.zip") -> str:
    """
    Zips all files in folder_path into zip_name inside folder_path.
    Returns full zip path.
    """
    zip_path = os.path.join(folder_path, zip_name)

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(folder_path):
            for f in files:
                full = os.path.join(root, f)
                # avoid zipping the zip into itself if re-running
                if os.path.abspath(full) == os.path.abspath(zip_path):
                    continue
                # store only filename (flat) OR change to relative paths if you prefer
                rel = os.path.relpath(full, folder_path)
                z.write(full, arcname=rel)

    return zip_path


def run_pipeline(folder_path: str) -> dict:
    """
    Azure-friendly entry point:
      - expects folder_path already contains all input files + metadata.json
      - generates CSV
      - generates ZIP containing everything (including the CSV)
      - returns file paths
    """
    csv_path, output_stem = process_folder(folder_path)
    zip_path = create_zip_of_folder(folder_path, f"{output_stem}.zip")
    return {
        "csv_path": csv_path,
        "zip_path": zip_path
    }


