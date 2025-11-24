import pandas as pd
import os
import re
from typing import Dict, Tuple, Optional, List
from core.conversion.handlers.common.main_code import process_df_with_suffix

def parse_sql_inserts(file_path: str) -> Tuple[Optional[pd.DataFrame], Optional[str], Optional[list], List[str]]:
    """
    Phân tích cú pháp các câu lệnh SQL INSERT để trích xuất tên bảng, cột và dữ liệu vào một DataFrame.
    Xử lý nhiều câu lệnh INSERT hoặc một lệnh INSERT duy nhất với nhiều hàng giá trị.
    Trả về (DataFrame, table_name, column_names, debug_lines) hoặc (None, None, None, []) nếu phân tích cú pháp không thành công.
    debug_lines chứa văn bản thô của các hàng không thể phân tích cú pháp để gỡ lỗi.
    """
    debug_lines = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Chuẩn hóa khoảng trắng và xóa chú thích
        sql_content = re.sub(r'--.*?\n|/\*.*?\*/', '', sql_content, flags=re.DOTALL)
        sql_content = re.sub(r'\s+', ' ', sql_content.strip())
        
        # Tìm tất cả các câu lệnh INSERT với regex được cải thiện để xử lý dấu chấm phẩy trong các giá trị
        insert_pattern = r'INSERT INTO\s+`?((?:[\w-]+)(?:\.(?:[\w-]+))?)`?\s*\(([\w\s`,]+)\)\s*VALUES\s*((?:\(.*?\)(?:,\s*\(.*?\))*));'
        
        matches = re.findall(insert_pattern, sql_content, re.IGNORECASE | re.DOTALL)
        
        if not matches:
            debug_lines.append("-- Không tìm thấy câu lệnh INSERT hợp lệ trong file SQL")
            print("❌ Không tìm thấy câu lệnh INSERT hợp lệ trong file SQL")
            return None, None, None, debug_lines
        
        all_values = []
        table_name = None
        columns = None
        
        for match in matches:
            current_table_name, columns_str, values_str = match
            
            # Đảm bảo tên bảng nhất quán
            if table_name is None:
                table_name = current_table_name
            elif table_name != current_table_name:
                debug_lines.append(f"-- Lỗi: Tìm thấy nhiều bảng khác nhau: {table_name} và {current_table_name}")
                print(f"❌ Tìm thấy nhiều bảng khác nhau: {table_name} và {current_table_name}")
                return None, None, None, debug_lines
            
           # Phân tích cú pháp các cột (chỉ một lần, giả sử tất cả các INSERT đều có cùng một cột)
            if columns is None:
                columns = [col.strip().strip('`') for col in columns_str.split(',')]
            
            # Phân tích giá trị
            values_str = values_str.strip()
            value_rows = []
            current_row = []
            current_value = ''
            in_quotes = False
            i = 0
            
            while i < len(values_str):
                char = values_str[i]
                
                if char == "'" and (i == 0 or values_str[i-1] != '\\'):
                    in_quotes = not in_quotes
                    current_value += char
                elif char == ',' and not in_quotes:
                    val = current_value.strip()
                    if val.lower() == 'null':
                        current_row.append(None)
                    elif val.startswith("'") and val.endswith("'"):
                        current_row.append(val[1:-1])
                    else:
                        try:
                            # Thử chuyển đổi sang float/int cho các giá trị số
                            current_row.append(float(val) if '.' in val else int(val))
                        except ValueError:
                            current_row.append(val)
                    current_value = ''
                elif char == '(' and not in_quotes:
                    current_value = ''
                    current_row = []
                elif char == ')' and not in_quotes:
                    val = current_value.strip()
                    if val.lower() == 'null':
                        current_row.append(None)
                    elif val.startswith("'") and val.endswith("'"):
                        current_row.append(val[1:-1])
                    else:
                        try:
                            current_row.append(float(val) if '.' in val else int(val))
                        except ValueError:
                            current_row.append(val)
                    if len(current_row) == len(columns):
                        value_rows.append(current_row)
                    else:
                        debug_lines.append(f"-- Bỏ qua dòng không khớp số cột ({len(current_row)} cột, cần {len(columns)}): ({values_str[max(0, i-50):i+50]})")
                        print(f"⚠️ Bỏ qua dòng không khớp số cột ({len(current_row)} cột, cần {len(columns)}): {current_row}")
                    current_value = ''
                    current_row = []
                else:
                    current_value += char
                i += 1
            
            all_values.extend(value_rows)
        
        if not all_values:
            debug_lines.append("-- Không thể phân tích dữ liệu từ câu lệnh INSERT")
            print("❌ Không thể phân tích dữ liệu từ câu lệnh INSERT")
            return None, None, None, debug_lines
        
        # Tạo DataFrame
        df = pd.DataFrame(all_values, columns=columns)
        print(f"📊 Đã đọc SQL: {len(df)} mẫu, {len(df.columns)} trường")
        return df, table_name, columns, debug_lines
    
    except Exception as e:
        debug_lines.append(f"-- Lỗi đọc file SQL: {str(e)}")
        print(f"❌ Không thể đọc SQL: {e}")
        return None, None, None, debug_lines

def generate_sql_inserts(df: pd.DataFrame, table_name: str, columns: list) -> str:
    """
    Sinh SQL INSERT từ DataFrame.
    """
    def format_value(val):
        if pd.isna(val) or val is None:
            return 'NULL'
        if isinstance(val, str):
            return f"'{val.replace('\'', '\\\'')}'"
        return str(val)
    
    inserts = [f"INSERT INTO {table_name} ({', '.join([f'{col}' for col in columns])}) VALUES"]
    values = [f"({', '.join(format_value(val) for val in row)})" for _, row in df[columns].iterrows()]
    inserts.append(',\n'.join(values) + ';')
    return '\n'.join(inserts)

def process_sql(input_file: str, 
                map_dict: Dict[Tuple[str, str, str], List[Tuple[str, str, str, str]]], 
                address_configs=None,
                pool=None) -> bool:
    """Xử lý SQL HOÀN CHỈNH (.sql) - DEBUG MAPPING CHI TIẾT"""
    
    # -------------------------------------------------
    # 1. KIỂM TRA FILE
    # -------------------------------------------------
    if not os.path.exists(input_file):
        print(f"❌ File SQL không tồn tại: {input_file}")
        return False
    
    # -------------------------------------------------
    # 2. ĐỌC SQL
    # -------------------------------------------------
    df, table_name, original_columns, debug_lines = parse_sql_inserts(input_file)
    
    if df is None or table_name is None or original_columns is None or len(df) == 0:
        print("❌ File SQL rỗng hoặc không đọc được")
    else:
        if 'Trạng thái chuyển đổi' not in df.columns:
            df.insert(len(df.columns), 'Trạng thái chuyển đổi', '')

        # -------------------------------------------------
        # 3. XỬ LÝ DATAFRAME
        # -------------------------------------------------
        for idx, (id_p, id_d, id_w, p, d, w) in enumerate(address_configs):
            suffix = f"_group{idx+1}"
            df = process_df_with_suffix(df, map_dict,
                                        id_province_col=id_p, 
                                        id_district_col=id_d, 
                                        id_ward_col=id_w,
                                        province_col=p, 
                                        district_col=d, 
                                        ward_col=w,
                                        suffix=suffix, 
                                        pool=pool)

    count_success = (df['Trạng thái chuyển đổi'] == 'Thành công').sum()
    count_fail = len(df) - count_success
    
    return {
        "success": True,
        "full_df": df.to_dict(orient="records"),
        "total_rows": len(df),
        "success_count": int(count_success),
        "fail_count": int(count_fail),
    }