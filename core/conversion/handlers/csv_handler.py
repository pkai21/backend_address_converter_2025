import pandas as pd
import os
from typing import Dict, Tuple, Optional, List
from core.conversion.handlers.common.main_code import process_df_with_suffix

def process_csv(input_file: str,
                map_dict: Dict[Tuple[str, str, str], List[Tuple[str, str, str, str]]], 
                address_groups=None,
                pool=None) -> bool:
    """Xử lý CSV HOÀN CHỈNH (.csv) - DEBUG MAPPING CHI TIẾT"""
    
    # -------------------------------------------------
    # 1. KIỂM TRA FILE
    # -------------------------------------------------
    if not os.path.exists(input_file):
        print(f"❌ File CSV không tồn tại: {input_file}")
        return False
    
    # -------------------------------------------------
    # 2. ĐỌC CSV
    # -------------------------------------------------
    df = None
    try:
        df = pd.read_csv(input_file)
        print(f"📊 Đã đọc CSV: {len(df)} mẫu, {len(df.columns)} trường")
    except Exception as e:
        print(f"❌ Không thể đọc CSV: {e}")
        return False
    
    if df is None or len(df) == 0:
        print("❌ File CSV rỗng hoặc không đọc được")
        return False

    if 'statusState' not in df.columns:
        df.insert(len(df.columns), 'statusState', '')

    # -------------------------------------------------
    # 3. XỬ LÝ DATAFRAME
    # -------------------------------------------------
    for idx, (id_p, id_d, id_w, p, d, w) in enumerate(address_groups):
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
        
    count_success = (df['statusState'] == 'Thành công').sum()
    count_fail = len(df) - count_success

    df.insert(0, 'id', df.index + 1)
    
    original_columns = df.columns.tolist()
    final_columns_order = [col for col in original_columns if col.lower() != "id"]
    
    return {
        "success": True,
        "full_df": df[original_columns].to_dict(orient="records"),
        "columns": final_columns_order,
        "total_rows": len(df),
        "success_count": int(count_success),
        "fail_count": int(count_fail),
    }