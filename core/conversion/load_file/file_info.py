# load_file/file_info.py  
import os, json, sqlite3, openpyxl, pandas as pd
from pathlib import Path

# === IMPORT parse_sql_inserts từ sql_handler ===
from core.conversion.handlers.sql_handler import parse_sql_inserts  

def get_sample_rows(total_row: int) -> int:
    """
    Trả về số dòng mẫu đại diện (sample_rows) dựa vào tổng số dòng dữ liệu (total_row).
    Quy tắc:
    - Lấy khoảng 1% số dòng, nhưng không ít hơn 10 và không quá 500.
    - Giúp đảm bảo đủ tính đại diện mà vẫn xử lý nhanh.
    """
    sample_rows = int(total_row * 0.01)

    # Giới hạn dưới và trên
    sample_rows = max(sample_rows, 10)   # tối thiểu 10 dòng
    sample_rows = min(sample_rows, 500)  # tối đa 500 dòng

    return sample_rows

def get_file_info(path: str):
    p = Path(path)
    if not p.exists():
        return {"rows": 0, "cols": 0, "names": [], "mb": 0, "sample_df": None, "error": "File not found"}

    ext = p.suffix.lower()
    mb = round(p.stat().st_size / (1024*1024), 1)
    result = {"rows": 0, "cols": 0, "names": [], "mb": mb, "sample_df": None, "error": None}

    # ==================== ĐẾM DÒNG (RIÊNG) ====================
    if result["error"] is None:
        try:
            if ext in {'.xlsx', '.xls'}:
                wb = openpyxl.load_workbook(p, read_only=True)
                result["rows"] = wb.active.max_row - 1
                wb.close()
            elif ext == '.csv':
                with open(p, 'r', encoding='utf-8-sig') as f:
                    result["rows"] = sum(1 for _ in f) - 1
            elif ext == '.json':
                with open(p, 'r', encoding='utf-8') as f:
                    content = f.read().strip()
                    if content.startswith('['):
                        result["rows"] = len(json.loads(content))
                    else:
                        result["rows"] = sum(1 for ln in f if ln.strip() and not ln.strip().startswith('//'))
            elif ext in {'.db', '.sqlite', '.sqlite3'}:
                conn = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
                cur = conn.cursor()
                cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' LIMIT 1")
                table = cur.fetchone()
                if table:
                    result["rows"] = cur.execute(f"SELECT COUNT(*) FROM `{table[0]}`").fetchone()[0]
                conn.close()
            elif ext == '.sql':
                # === DÙNG parse_sql_inserts ĐỂ ĐẾM DÒNG TRONG .sql ===
                df_temp, _, _, _ = parse_sql_inserts(str(p))
                result["rows"] = len(df_temp) if df_temp is not None else 0
        except Exception as e:
            result["rows"] = -1
            result["error"] = result["error"] or f"Count error: {e}"

    # ==================== ĐỌC MẪU + HEADER ====================
    total_row = result["rows"] 
    sample_rows = get_sample_rows(total_row) if total_row > 0 else 0

    df_sample = None
    try:
        if ext in {'.xlsx', '.xls'}:
            wb = openpyxl.load_workbook(p, read_only=True, data_only=True)
            sheet = wb.active
            if sheet.max_row <= 1:
                result["error"] = "Excel trống hoặc chỉ có header"
            else:
                header = next(sheet.iter_rows(min_row=1, max_row=1, values_only=True))
                header = [str(h) if h is not None else f"col_{i}" for i, h in enumerate(header)]
                data_rows = list(sheet.iter_rows(min_row=2, max_row=min(1+sample_rows, sheet.max_row), values_only=True))
                df_sample = pd.DataFrame(data_rows, columns=header)
            wb.close()

        elif ext == '.csv':
            for encoding in ['utf-8-sig', 'utf-8', 'cp1252', 'latin1']:
                try:
                    df_sample = pd.read_csv(p, nrows=sample_rows, encoding=encoding)
                    break
                except:
                    continue
            else:
                result["error"] = "Không đọc được CSV (encoding)"

        elif ext == '.json':
            with open(p, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                if not content:
                    result["error"] = "JSON rỗng"
                elif content.startswith('['):
                    data = json.loads(content)[:sample_rows]
                else:
                    lines = [ln.strip() for ln in content.splitlines() if ln.strip() and not ln.strip().startswith('//')]
                    data = [json.loads(ln) for ln in lines[:sample_rows]]
                df_sample = pd.DataFrame(data)

        elif ext in {'.db', '.sqlite', '.sqlite3'}:
            conn = None
            try:
                conn = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
                cur = conn.cursor()
                cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' LIMIT 1")
                row = cur.fetchone()
                if not row:
                    result["error"] = "Không có bảng"
                else:
                    table = row[0]
                    df_sample = pd.read_sql_query(f"SELECT * FROM `{table}` LIMIT {sample_rows}", conn)
                conn.close()
            except Exception as e:
                if conn: conn.close()
                result["error"] = f"DB error: {e}"

        elif ext == '.sql':
            # === DÙNG parse_sql_inserts ĐỂ ĐỌC MẪU .sql ===
            df_full, table_name, columns, debug_lines = parse_sql_inserts(str(p))
            if df_full is not None and not df_full.empty:
                # Lấy mẫu
                df_sample = df_full.head(sample_rows).copy()
                # Đảm bảo cột là str
                df_sample.columns = [str(col) for col in df_sample.columns]
            else:
                result["error"] = "Không đọc được dữ liệu từ file SQL"

        else:
            result["error"] = f"Không hỗ trợ: {ext}"

    except Exception as e:
        result["error"] = f"Read error: {e}"

    # ==================== GÁN KẾT QUẢ ====================
    if df_sample is not None and not df_sample.empty:
        result["names"] = df_sample.columns.astype(str).tolist()
        result["cols"] = len(result["names"])
        result["sample_df"] = df_sample  # ĐÃ CHUẨN HÓA
    else:
        result["names"] = []
        result["cols"] = 0
        result["sample_df"] = pd.DataFrame()

    return result