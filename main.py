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

# ============== REGLAS / REGEX ==============
CODE_RE  = re.compile(r"^\d{2,6}$")
UNITS_RE = re.compile(r"\b(\d{1,4})\s*(ml|cc|l|lt|lts|litro?s?|kg|g)\b", re.I)
PACK_RE  = re.compile(r"x\s*\d+\s*u", re.I)
HAS_LETTERS_RE = re.compile(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]")

def tidy_text(x: object) -> str:
    """Normaliza texto de celdas (quita saltos, packs, espacios extras)."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    s = str(x).replace("\n", " ")
    s = PACK_RE.sub("", s)            # quita “x5u”, “x 12 u”, etc.
    s = re.sub(r"\s+", " ", s).strip()
    return s

def normalize_price(x: object):
    """Convierte valores a número; descarta ruidos (<100)."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = str(x).strip()
    s = re.sub(r"\s+", "", s)                    # "00 700.00" -> "00700.00"
    if re.match(r"^\d{1,3}(?:\.\d{3})+$", s):
        s = s.replace(".", "")                   # "1.234.567" -> "1234567"
    s = s.replace(",", ".")
    s = re.sub(r"[^\d.]", "", s)
    if not s:
        return None
    try:
        v = float(s)
        return v if v >= 100 else None          # ignoro basura tipo 1, 7, .00
    except Exception:
        return None

def numbers_in_row_with_positions(cells):
    """Devuelve [(idx, valor_num)] de todos los números creíbles en la fila."""
    out = []
    for i, c in enumerate(cells):
        v = normalize_price(c)
        if v is not None:
            out.append((i, v))
    return out

def has_letters(s: str) -> bool:
    return bool(HAS_LETTERS_RE.search(s or ""))

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

# ============== GOOGLE CREDS / DESCARGA ==============
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

# ============== LECTURA DE TABLAS (todas las páginas) ==============
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
            # pandas >= 2.1: applymap se deprecó, usamos map por columnas
            for c in df.columns:
                df[c] = df[c].map(tidy_text)
            clean.append(df)
    return clean

# ============== PARSEO CON FUSIÓN DE FILAS ==============
def parse_rows(dfs):
    rows = []
    merged_count = 0

    for df in dfs:
        pending = None  # (code, desc, unit) a la espera de precios en la siguiente fila

        for _, r in df.iterrows():
            cells = [r[c] for c in df.columns]
            # Clasificamos fila
            has_text = any(has_letters(c) for c in cells)
            nums = numbers_in_row_with_positions(cells)

            # Detecto código al inicio si viene
            code = ""
            for c in cells[:3]:
                if CODE_RE.match(c or ""):
                    code = c
                    break

            if has_text and nums:
                # Caso A: descripción y números en la MISMA fila
                # Regla: precio = último número (derecha)
                text_cells = []
                for c in cells:
                    if CODE_RE.match(c or ""):
                        continue
                    if has_letters(c):
                        text_cells.append(c)
                desc = " ".join(text_cells).strip()
                if not desc:
                    continue
                unit = extract_unit(desc)
                desc = re.sub(r"\s+[.,]00\b", "", desc)

                price = nums[-1][1]  # ÚLTIMO número de la fila
                rows.append([code, desc, unit, int(round(price))])
                pending = None

            elif has_text and not nums:
                # Caso B: descripción sin números -> guardo pendiente
                text_cells = []
                for c in cells:
                    if CODE_RE.match(c or ""):
                        continue
                    if has_letters(c):
                        text_cells.append(c)
                desc = " ".join(text_cells).strip()
                if not desc:
                    continue
                unit = extract_unit(desc)
                desc = re.sub(r"\s+[.,]00\b", "", desc)
                pending = (code, desc, unit)

            elif (not has_text) and nums and pending:
                # Caso C: fila SOLO con números que sigue a una descripción pendiente
                # Regla: precio = PRIMER número (izquierda)  -> corrige 500 vs 600
                price = nums[0][1]
                pcode, pdesc, punit = pending
                rows.append([pcode, pdesc, punit, int(round(price))])
                pending = None
                merged_count += 1

            else:
                # Fila sólo números sin pendiente, o basura -> ignorar
                continue

        # Si quedó un pendiente sin precios, lo ignoramos (sin precio no sirve)

    # de-duplicar manteniendo (code, desc, price) para no borrar variantes
    seen = set()
    unique = []
    for code, desc, unit, price in rows:
        key = (code or "", desc, price)
        if key in seen:
            continue
        seen.add(key)
        unique.append([code, desc, unit, price])

    print(f"[INFO] Filas fusionadas (desc + precios siguiente fila): {merged_count}")
    return unique

# ============== ESCRITURA EN GOOGLE SHEETS ==============
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

# ============== MAIN ==============
def main():
    creds = load_creds()
    with tempfile.TemporaryDirectory() as tmp:
        pdf_path = os.path.join(tmp, "lista.pdf")
        download_pdf(pdf_path)

        dfs = read_all_tables(pdf_path)
        print(f"[INFO] TABLAS_ENCONTRADAS={len(dfs)}")

        rows = parse_rows(dfs)
        print(f"[INFO] FILAS_FINAL={len(rows)}")

        # ordenar por código asc (numéricos primero) y luego por descripción
        def code_key(c):
            try:
                return int(c)
            except Exception:
                return 10**9
        rows.sort(key=lambda r: (code_key(r[0]), r[1]))

        # CSV en workspace para artefacto
        workspace = os.getenv("GITHUB_WORKSPACE", os.getcwd())
        csv_path = os.path.join(workspace, "proveedor_extracted.csv")
        pd.DataFrame(
            rows, columns=["codigo","descripcion","presentacion","precio_final"]
        ).to_csv(csv_path, index=False, encoding="utf-8")
        print(f"[INFO] CSV_SAVED={csv_path}")

        write_to_sheet(rows, creds)

if __name__ == "__main__":
    main()
