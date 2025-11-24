import pandas as pd
import os
import json
from typing import Dict, Tuple, Optional, List
from core.conversion.handlers.common.main_code import process_df_with_suffix

def process_json(input_file: str,
                 map_dict: Dict[Tuple[str, str, str], List[Tuple[str, str, str, str]]], 
                 address_configs=None,
                 pool=None) -> bool:
    """Xử lý JSON HOÀN CHỈNH (.json) - DEBUG MAPPING CHI TIẾT"""
    
    # -------------------------------------------------
    # 1. KIỂM TRA FILE
    # -------------------------------------------------
    if not os.path.exists(input_file):
        print(f"❌ File JSON không tồn tại: {input_file}")
        return False
    
    # -------------------------------------------------
    # 2. ĐỌC JSON
    # -------------------------------------------------
    df = None
    try:
        df = pd.read_json(input_file, orient='records')
        print(f"📊 Đã đọc JSON: {len(df)} mẫu, {len(df.columns)} trường")
    except Exception as e:
        print(f"❌ Không thể đọc JSON: {e}")
        return False
    
    if df is None or len(df) == 0:
        print("❌ File JSON rỗng hoặc không đọc được")
        return False

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