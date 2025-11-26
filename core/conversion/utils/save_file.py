import os
import pandas as pd

from core.conversion.handlers.sql_handler import generate_sql_inserts


def save_excel_file(df: pd.DataFrame, output_file: str) -> None:
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    out_path = (
        output_file
        if output_file.lower().endswith('.xlsx')
        else f"{output_file.rsplit('.', 1)[0]}.xlsx"
    )

    try:
        df.to_excel(out_path, index=False, engine='openpyxl')
        print(f"Đã lưu file: {out_path}")
    except Exception as e:
        print(f"Lỗi lưu file: {e}")
        return False
    return True

def save_csv_file(df: pd.DataFrame, output_file: str) -> None:
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Đảm bảo output là .csv
    if not output_file.lower().endswith('.csv'):
        output_file = output_file.rsplit('.', 1)[0] + '.csv'
    
    try:
        df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"💾 Đã lưu CSV: {output_file}")
    except Exception as e:
        print(f"❌ Lỗi lưu file: {e}")
        return False

    return True

def save_json_file(df: pd.DataFrame, output_file: str) -> None:
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Đảm bảo output là .json
    if not output_file.lower().endswith('.json'):
        output_file = output_file.rsplit('.', 1)[0] + '.json'
    
    try:
        df.to_json(output_file, orient='records', indent=2, force_ascii=False)
        print(f"💾 Đã lưu JSON: {output_file}")
    except Exception as e:
        print(f"❌ Lỗi lưu file: {e}")
        return False
    
    return True

def save_sql_file(df: pd.DataFrame, output_file: str) -> None:
    
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    if not output_file.lower().endswith('.sql'):
        output_file = output_file.rsplit('.', 1)[0] + '.sql'
    
    try:
        if df is not None and not df.empty:
            table_name = "converted_addresses"
            sql_output = generate_sql_inserts(df, table_name, df.columns.tolist())
        else:
            sql_output = "-- Không có dữ liệu hợp lệ để tạo câu lệnh INSERT"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(sql_output)
        print(f"💾 Đã lưu SQL: {output_file}")
    except Exception as e:
        print(f"❌ Lỗi lưu file: {e}")
        return False
    
    return True

def save_file(df: pd.DataFrame, output_file: str) -> bool:
    """
    Lưu DataFrame vào file với định dạng dựa trên phần mở rộng của output_file.
    Hỗ trợ: .xlsx, .csv, .json, .sql
    Trả về True nếu lưu thành công, False nếu lỗi.
    """
    if 'statusState' in df.columns:
        df = df.drop(columns=['statusState'])
    if 'id' in df.columns:
        df = df.drop(columns=['id'])
        
    ext = os.path.splitext(output_file)[1].lower()
    if ext == '.xlsx' or ext == '.xls' :
        return save_excel_file(df, output_file)
    elif ext == '.csv':
        return save_csv_file(df, output_file)
    elif ext == '.json':
        return save_json_file(df, output_file)
    elif ext == '.sql':
        return save_sql_file(df, output_file)
    else:
        print(f"❌ Định dạng file không được hỗ trợ: {ext}")
        return False