import os
import re
import json
import requests
import tempfile
import pandas as pd
import pdfplumber

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
PRICE_RE = re.compile(
    r"^\$?\s*\d{1,3}(?:[.\s]\d{3})*(?:,\d{2})?$|^\$?\s*\d+(?:[.,]\d{2})?$"
)

SKIP_WORDS = ("fecha", "hora", "página", "pag", "cliente", "subtotal",
              "total", "cuit", "c.u.i.t", "condición", "condicion",
              "responsable", "inscripto", "domicilio", "original")


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
    tok = (tok or "").strip()
    return bool(PRICE_RE.match(tok))

def to_price(tok):
    s = (tok or "").strip()
    s = s.replace(" ", "")
    if re.match(r"^\d{1,3}(?:\.\d{3})+$", s):
        s = s.replace(".", "")
    s = s.replace(",", ".")
    s = re.sub(r"[^\d.]", "", s)
    if not s:
        return None
    try:
        v = float(s)
        return v if v >= 1 else None   # evitar ".00"
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


# ============== PARSEO CON pdfplumber ==============
def page_lines(page, y_tol=2.5):
    """Agrupa palabras por renglón usando tolerancia en Y."""
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

    # a cada línea la convierto en lista de tokens (texto) y guardo también x0
    out = []
    for L in lines:
        toks = [tidy_text(w["text"]) for w in L]
        xs   = [w["x0"] for w in L]
        out.append((toks, xs))
    return out

def parse_pdf_to_rows(pdf_path):
    rows = []
    merged = 0

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            for toks, xs in page_lines(page):
                # texto completo & filtros de basura
                line_text = " ".join(toks).lower()
                if any(sw in line_text for sw in SKIP_WORDS):
                    continue

                # Clasificación de la línea
                has_text = any(has_letters(t) for t in toks)
                nums_idx = [(i, to_price(toks[i])) for i in range(len(toks)) if is_price_token(toks[i])]
                nums_idx = [(i, v) for i, v in nums_idx if v is not None]

                # Detecto código: primer token de 2–6 dígitos
                code = ""
                for t in toks[:3]:
                    if CODE_RE.match(t):
                        code = t
                        break

                # Mantengo un "pendiente" por página (descripción sola)
                if not hasattr(parse_pdf_to_rows, "_pending"):
                    parse_pdf_to_rows._pending = None

                if has_text and nums_idx:
                    # Caso A: todo en la misma línea → precio = último número
                    # armo descripción con tokens alfabéticos distintos de código
                    desc_tokens = []
                    for t in toks:
                        if CODE_RE.match(t):
                            continue
                        if has_letters(t):
                            desc_tokens.append(t)
                    desc = " ".join(desc_tokens).strip()
                    if not desc:
                        parse_pdf_to_rows._pending = None
                        continue
                    unit = extract_unit(desc)
                    desc = re.sub(r"\s+[.,]00\b", "", desc)

                    price = nums_idx[-1][1]  # último número
                    if code and price is not None:
                        rows.append([code, desc, unit, int(round(price))])
                    parse_pdf_to_rows._pending = None

                elif has_text and not nums_idx:
                    # Caso B: descripción sola → guardo pendiente
                    desc_tokens = []
                    for t in toks:
                        if CODE_RE.match(t):  # codigo
                            continue
                        if has_letters(t):
                            desc_tokens.append(t)
                    desc = " ".join(desc_tokens).strip()
                    if not desc:
                        continue
                    unit = extract_unit(desc)
                    desc = re.sub(r"\s+[.,]00\b", "", desc)
                    # guardo pendiente sólo si hay código
                    parse_pdf_to_rows._pending = (code, desc, unit)

                elif (not has_text) and nums_idx and getattr(parse_pdf_to_rows, "_pending", None):
                    # Caso C: fila sólo números que sigue a una descripción pendiente
                    # regla: uso el PRIMER número (columna más a la izquierda)
                    price = nums_idx[0][1]
                    pcode, pdesc, punit = parse_pdf_to_rows._pending
                    if pcode and price is not None:
                        rows.append([pcode, pdesc, punit, int(round(price))])
                        merged += 1
                    parse_pdf_to_rows._pending = None

                else:
                    # otra cosa → ignorar y limpiar pendiente
                    parse_pdf_to_rows._pending = None

    print(f"[INFO] Filas fusionadas (desc + números siguiente línea): {merged}")
    # deduplico por (code, desc) manteniendo el último precio visto
    dedup = {}
    for code, desc, unit, price in rows:
        dedup[(code, desc)] = [code, desc, unit, price]
    return list(dedup.values())


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

        rows = parse_pdf_to_rows(pdf_path)
        print(f"[INFO] Ítems válidos: {len(rows)}")

        # orden: por código (numérico) y luego descripción
        def code_key(c):
            try:
                return int(c)
            except Exception:
                return 10**9
        rows.sort(key=lambda r: (code_key(r[0]), r[1]))

        # artefacto CSV para inspección
        workspace = os.getenv("GITHUB_WORKSPACE", os.getcwd())
        csv_path = os.path.join(workspace, "proveedor_extracted.csv")
        pd.DataFrame(rows, columns=["codigo","descripcion","presentacion","precio_final"]).to_csv(
            csv_path, index=False, encoding="utf-8"
        )
        print(f"[INFO] CSV guardado: {csv_path}")

        write_to_sheet(rows, creds)

if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()
