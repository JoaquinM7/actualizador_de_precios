import os
import re
import json
import tempfile
import requests
import pandas as pd

# --- libs externas ---
import pdfplumber          # pip install pdfplumber
import tabula              # pip install tabula-py (requiere Java)
import gspread
from google.oauth2.service_account import Credentials

# =================== CONFIG ===================
PDF_URL = "https://quimicacromax.com.ar/lista-de-precios.pdf"
SPREADSHEET_ID = "1zEe-kSeaygPG8QwFY3OoUYg0YYEwLOBg9U3BnNUCa6Y"
SHEET_NAME = "LISTA_PROVEEDOR"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# =================== REGEX =====================
CODE_RE  = re.compile(r"^\d{2,6}$")  # 2 a 6 dígitos
LETTERS  = re.compile(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]")
PRICE_RE = re.compile(r"^\$?\s*\d{1,3}(?:[.\s]\d{3})*(?:,\d{2})?$|^\$?\s*\d+(?:[.,]\d{2})?$")
SKIP_WORDS = (
    "fecha", "hora", "página", "pag", "cliente", "subtotal",
    "total", "cuit", "c.u.i.t", "condición", "condicion",
    "responsable", "inscripto", "domicilio", "original"
)

# ============== HELPERS ==============
def load_creds():
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if not sa_json:
        raise SystemExit("[ERROR] Falta el secreto GOOGLE_SERVICE_ACCOUNT_JSON")
    info = json.loads(sa_json)
    return Credentials.from_service_account_info(info, scopes=SCOPES)

def download_pdf(path):
    print(f"[INFO] Descargando PDF: {PDF_URL}")
    r = requests.get(PDF_URL, timeout=60)
    r.raise_for_status()
    with open(path, "wb") as f:
        f.write(r.content)
    print("[INFO] PDF descargado OK")

def tidy_text(s):
    if s is None:
        return ""
    s = str(s).replace("\n", " ")
    s = re.sub(r"x\s*\d+\s*u\b", "", s, flags=re.I)  # quita "x5u"
    s = re.sub(r"\s+", " ", s).strip()
    return s

def has_letters(s):
    return bool(LETTERS.search(s or ""))

def is_price_token(tok):
    return bool(PRICE_RE.match((tok or "").strip()))

def to_price(tok):
    s = (tok or "").strip().replace(" ", "")
    if re.match(r"^\d{1,3}(?:\.\d{3})+$", s):
        s = s.replace(".", "")
    s = s.replace(",", ".")
    s = re.sub(r"[^\d.]", "", s)
    if not s:
        return None
    try:
        v = float(s)
        return v if v >= 1 else None
    except Exception:
        return None

def extract_unit(desc):
    m = re.search(r"\b(\d{1,4})\s*(ml|cc|l|lt|lts|litro?s?|kg|g)\b", desc, re.I)
    if not m:
        return ""
    n, u = m.group(1), m.group(2).lower()
    if u in ("l", "lt", "lts") or u.startswith("litro"):
        u = "lt"
    return f"{n} {u}"

def dedupe_rows(rows):
    # dedup por (code, desc) conservando el último precio visto
    dedup = {}
    for code, desc, unit, price in rows:
        dedup[(code, desc)] = [code, desc, unit, price]
    return list(dedup.values())

# ============== PARSEO CON pdfplumber ==============
def page_lines(page, y_tol=2.5):
    words = page.extract_words(
        keep_blank_chars=False,
        use_text_flow=False,
        extra_attrs=["x0", "x1", "top", "bottom"]
    )
    words.sort(key=lambda w: (w["top"], w["x0"]))

    lines = []
    current = []
    cur_top = None

    for w in words:
        if cur_top is None or abs(w["top"] - cur_top) <= y_tol:
            current.append(w)
            cur_top = w["top"] if cur_top is None else cur_top
        else:
            lines.append(sorted(current, key=lambda x: x["x0"]))
            current = [w]
            cur_top = w["top"]
    if current:
        lines.append(sorted(current, key=lambda x: x["x0"]))

    out = []
    for L in lines:
        toks = [tidy_text(w["text"]) for w in L]
        xs   = [w["x0"] for w in L]
        out.append((toks, xs))
    return out

def parse_pdfplumber(pdf_path):
    rows = []
    merged = 0
    total_lines = 0

    # pendiente (code, desc, unit) persiste por página
    pending = None

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            pending = None
            for toks, xs in page_lines(page):
                total_lines += 1
                if any(sw in " ".join(toks).lower() for sw in SKIP_WORDS):
                    continue

                # código = token que cumple CODE_RE con menor x (más a la izquierda)
                code_idx = None
                for i, t in enumerate(toks):
                    if CODE_RE.match(t):
                        if code_idx is None or xs[i] < xs[code_idx]:
                            code_idx = i
                code = toks[code_idx] if code_idx is not None else ""

                # desc = tokens con letras, excluyendo el token del código
                desc_tokens = []
                for i, t in enumerate(toks):
                    if code_idx is not None and i == code_idx:
                        continue
                    if has_letters(t):
                        desc_tokens.append(t)
                desc = " ".join(desc_tokens).strip()

                # números en la línea
                nums_idx = [(i, to_price(toks[i])) for i in range(len(toks)) if is_price_token(toks[i])]
                nums_idx = [(i, v) for i, v in nums_idx if v is not None]

                if desc and nums_idx and code:
                    # A: todo en la misma línea → último número
                    unit = extract_unit(desc)
                    desc = re.sub(r"\s+[.,]00\b", "", desc)
                    price = nums_idx[-1][1]
                    rows.append([code, desc, unit, int(round(price))])
                    pending = None

                elif desc and not nums_idx and code:
                    # B: desc sin números → queda pendiente
                    unit = extract_unit(desc)
                    desc = re.sub(r"\s+[.,]00\b", "", desc)
                    pending = (code, desc, unit)

                elif (not desc) and nums_idx and pending:
                    # C: línea de solo números justo después de una desc pendiente → primer número
                    price = nums_idx[0][1]
                    pcode, pdesc, punit = pending
                    rows.append([pcode, pdesc, punit, int(round(price))])
                    pending = None
                    merged += 1
                else:
                    pending = None

    rows = dedupe_rows(rows)
    print(f"[INFO] pdfplumber: lineas={total_lines}, items={len(rows)}, fusionadas={merged}")
    return rows

# ============== FALLBACK CON TABULA ==============
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
            for c in df.columns:
                df[c] = df[c].map(tidy_text)
            clean.append(df)
    return clean

def numbers_in_row_with_positions(cells):
    out = []
    for i, c in enumerate(cells):
        v = to_price(c) if is_price_token(c) else None
        if v is not None:
            out.append((i, v))
    return out

def parse_tabula(pdf_path):
    dfs = read_all_tables(pdf_path)
    total_rows = 0
    merged = 0
    rows = []

    for df in dfs:
        pending = None
        for _, r in df.iterrows():
            total_rows += 1
            cells = [r[c] for c in df.columns]
            # código = primer celda que cumple CODE_RE (en cualquier columna)
            code = ""
            for c in cells:
                if CODE_RE.match(c or ""):
                    code = c
                    break

            # desc = concat de celdas con letras (excluyendo el código exacto)
            desc_parts = []
            for c in cells:
                if c == code:
                    continue
                if has_letters(c):
                    desc_parts.append(c)
            desc = " ".join(desc_parts).strip()

            nums = numbers_in_row_with_positions(cells)

            if code and desc and nums:
                unit = extract_unit(desc)
                desc = re.sub(r"\s+[.,]00\b", "", desc)
                price = nums[-1][1]  # último número en la fila
                rows.append([code, desc, unit, int(round(price))])
                pending = None

            elif code and desc and not nums:
                unit = extract_unit(desc)
                desc = re.sub(r"\s+[.,]00\b", "", desc)
                pending = (code, desc, unit)

            elif (not desc) and nums and pending:
                price = nums[0][1]  # primer número de la fila siguiente
                pcode, pdesc, punit = pending
                rows.append([pcode, pdesc, punit, int(round(price))])
                pending = None
                merged += 1

            else:
                pending = None

    rows = dedupe_rows(rows)
    print(f"[INFO] tabula: filas={total_rows}, items={len(rows)}, fusionadas={merged}, dfs={len(dfs)}")
    return rows

# ============== ESCRITURA EN SHEETS ==============
def write_to_sheet(rows, creds):
    print(f"[INFO] Escribiendo {len(rows)} filas en Google Sheets…")
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
        CHUNK = 4000
        for i in range(0, len(rows), CHUNK):
            block = rows[i:i+CHUNK]
            ws.update(f"A{2+i}:D{2+i+len(block)-1}", block)
        ws.format(f"D2:D{len(rows)+1}",
                  {"numberFormat": {"type":"NUMBER","pattern":"#,##0"}})
    print("[INFO] Listo.")

# ============== MAIN ==============
def main():
    creds = load_creds()
    with tempfile.TemporaryDirectory() as tmp:
        pdf_path = os.path.join(tmp, "lista.pdf")
        download_pdf(pdf_path)

        # 1) pdfplumber
        rows_pdfplumber = parse_pdfplumber(pdf_path)

        # 2) Fallback con Tabula si no obtuvimos nada
        if len(rows_pdfplumber) == 0:
            print("[WARN] pdfplumber devolvió 0 ítems. Intento con Tabula…")
            rows = parse_tabula(pdf_path)
        else:
            rows = rows_pdfplumber

        # ordenar por código y descripción
        def code_key(c):
            try:
                return int(c)
            except Exception:
                return 10**9
        rows.sort(key=lambda r: (code_key(r[0]), r[1]))

        # CSV artefacto
        workspace = os.getenv("GITHUB_WORKSPACE", os.getcwd())
        csv_path = os.path.join(workspace, "proveedor_extracted.csv")
        pd.DataFrame(rows, columns=["codigo","descripcion","presentacion","precio_final"]).to_csv(
            csv_path, index=False, encoding="utf-8"
        )
        print(f"[INFO] CSV guardado: {csv_path}")

        # escribir
        write_to_sheet(rows, creds)

if __name__ == "__main__":
    main()



if __name__ == "__main__":
    main()
