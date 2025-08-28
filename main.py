import os
import re
import json
import tempfile
import requests
import pandas as pd

# --- libs externas ---
import tabula              # tabula-py (requiere Java)
import gspread             # gspread + google-auth
from google.oauth2.service_account import Credentials

# =================== CONFIG ===================
PDF_URL = "https://quimicacromax.com.ar/lista-de-precios.pdf"
SPREADSHEET_ID = "1zEe-kSeaygPG8QwFY3OoUYg0YYEwLOBg9U3BnNUCa6Y"
SHEET_NAME = "LISTA_PROVEEDOR"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# ============== REGLAS DE PARSEO ==============
CODE_RE  = re.compile(r"^\d{2,6}$")
UNITS_RE = re.compile(r"\b(\d{1,4})\s*(ml|cc|l|lt|lts|litro?s?|kg|g)\b", re.I)
PACK_RE  = re.compile(r"x\s*\d+\s*u", re.I)

def tidy_text(x: object) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    s = str(x).replace("\n", " ")
    s = PACK_RE.sub("", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def normalize_price(x: object):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = str(x).strip()
    s = re.sub(r"\s+", "", s)                       # "00 700.00" -> "00700.00"
    if re.match(r"^\d{1,3}(?:\.\d{3})+$", s):
        s = s.replace(".", "")                      # "1.234.567" -> "1234567"
    s = s.replace(",", ".")
    s = re.sub(r"[^\d.]", "", s)
    if not s:
        return None
    try:
        v = float(s)
        return v if v >= 100 else None
    except Exception:
        return None

def last_price_in_row(cells):
    vals = []
    for c in cells:
        v = normalize_price(c)
        if v is not None:
            vals.append(v)
    return vals[-1] if vals else None

def extract_unit(text: str) -> str:
    if not text:
        return ""
    m = UNITS_RE.search(text)
    if not m:
        return ""
    n, u = m.group(1), m.group(2).lower()
    if u in ("l", "lt", "lts") or u.startswith("litro"):
        u = "lt"
    return f"{n} {u}"

def load_creds():
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if not sa_json:
        raise SystemExit("[ERROR] Falta el secreto GOOGLE_SERVICE_ACCOUNT_JSON")
    creds_info = json.loads(sa_json)
    creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    return creds

def download_pdf(path):
    print(f"[INFO] Descargando PDF desde {PDF_URL}")
    r = requests.get(PDF_URL, timeout=60)
    r.raise_for_status()
    with open(path, "wb") as f:
        f.write(r.content)
    print("[INFO] PDF descargado OK")

def read_all_tables(pdf_path):
    dfs = []
    try:
        dfs += tabula.read_pdf(pdf_path, pages="all", lattice=True, multiple_tables=True)
    except Exception as e:
        print("[WARN] Lattice failed:", e)
    try:
        dfs += tabula.read_pdf(pdf_path, pages="all", stream=True, multiple_tables=True, guess=True)
    except Exception as e:
        print("[WARN] Stream failed:", e)
    clean = []
    for df in dfs:
        if isinstance(df, pd.DataFrame) and df.size > 0:
            df = df.copy()
            df.columns = [tidy_text(c) for c in df.columns]
            clean.append(df)
    return clean

def parse_rows(dfs):
    rows = []
    for df in dfs:
        df = df.applymap(tidy_text)
        for _, r in df.iterrows():
            cells = [r[c] for c in df.columns]

            # código
            code = ""
            for c in cells[:3]:
                if CODE_RE.match(c):
                    code = c
                    break

            # descripción
            text_cells = []
            for c in cells:
                if CODE_RE.match(c):
                    continue
                if re.search(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]", c):
                    text_cells.append(c)
            desc = " ".join(text_cells).strip()
            if not desc:
                continue

            # precio
            price = last_price_in_row(cells)
            if price is None and desc:
                tail = desc.split()[-1]
                tail_num = normalize_price(tail)
                if tail_num is not None:
                    price = tail_num
                    desc = re.sub(r"\s+\d[\d.,]*$", "", desc).strip()

            if price is None:
                continue

            unit = extract_unit(desc)
            desc = re.sub(r"\s+[.,]00\b", "", desc)
            rows.append([code, desc, unit, int(round(price))])

    # de-duplicar
    seen = set()
    unique = []
    for row in rows:
        key = (row[0], row[1])
        if key in seen:
            continue
        seen.add(key)
        unique.append(row)
    return unique

def write_to_sheet(rows, creds):
    print(f"[INFO] Escribiendo {len(rows)} filas a Google Sheets…")
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)
    try:
        ws = sh.worksheet(SHEET_NAME)
        ws.clear()
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=SHEET_NAME,
                              rows=str(max(len(rows)+10, 100)),
                              cols="4")
    header = [["codigo","descripcion","presentacion","precio_final"]]
    ws.update("A1:D1", header)

    if rows:
        CHUNK = 5000
        for i in range(0, len(rows), CHUNK):
            block = rows[i:i+CHUNK]
            ws.update(f"A{2+i}:D{2+i+len(block)-1}", block)
        ws.format(f"D2:D{len(rows)+1}",
                  {"numberFormat": {"type":"NUMBER","pattern":"#,##0"}})
    print("[INFO] Listo")

def main():
    creds = load_creds()
    with tempfile.TemporaryDirectory() as tmp:
        pdf_path = os.path.join(tmp, "lista.pdf")
        download_pdf(pdf_path)

        dfs = read_all_tables(pdf_path)
        print(f"[INFO] TABLAS_ENCONTRADAS={len(dfs)}")

        rows = parse_rows(dfs)
        print(f"[INFO] FILAS_FINAL={len(rows)}")

        # ordenar por código
        def code_key(c):
            try:
                return int(c)
            except Exception:
                return 10**9
        rows.sort(key=lambda r: (code_key(r[0]), r[1]))

        # CSV SIEMPRE en el workspace (para artifact)
        workspace = os.getenv("GITHUB_WORKSPACE", os.getcwd())
        csv_path = os.path.join(workspace, "proveedor_extracted.csv")
        pd.DataFrame(rows, columns=["codigo","descripcion","presentacion","precio_final"]).to_csv(
            csv_path, index=False, encoding="utf-8"
        )
        print(f"[INFO] CSV_SAVED={csv_path}")

        # escribir a Sheets (no falla si 0 filas, solo deja header)
        write_to_sheet(rows, creds)

if __name__ == "__main__":
    main()
