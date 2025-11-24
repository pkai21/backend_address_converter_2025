# utils/column_detector.py
from typing import Tuple, Optional, Dict, Set, List
import pandas as pd
import numpy as np
import Levenshtein
from core.conversion.utils.normalizer import normalize_place
from core.conversion.utils.vietnamese_code import vietnamese_normalize_text

def normalize_sample_value(value):
    """Chuẩn hóa giống hệt mapping_loader"""
    if pd.isna(value) or value == '':
        return ''
    s = str(value).strip()
    return vietnamese_normalize_text(normalize_place(s)).strip()

# Kiểm tra cột có thể chuyển về int an toàn không
def can_convert_to_numeric(series):
    try:
        # Dùng pd.to_numeric với errors='coerce' -> nếu toàn bộ thành NaN thì không được
        converted = pd.to_numeric(series, errors='coerce')
        return not converted.isna().all()  # Có ít nhất 1 giá trị hợp lệ
    except:
        return False

#Kiểm tra cho các cột id
def check_id_address(id_cand, numeric_dict):
    id_cand_new = []
    for val in id_cand:
        if val in numeric_dict and numeric_dict[val] == True:
            id_cand_new.append(val)
    return id_cand_new

#----------CHIA THÀNH CÁC CỤM ĐỊA CHỈ---------------
def similar(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return Levenshtein.jaro_winkler(a.lower(), b.lower())

def matrix_cand(cand1: List[str], cand2: List[str]):
    n = len(cand1)
    m = len(cand2)

    # Tạo ma trận rỗng n×m
    matrix = np.zeros((n, m))

    # Điền giá trị độ tương đồng
    for i in range(n):
        for j in range(m):
            matrix[i][j] = similar(cand1[i], cand2[j])

    return matrix

def find_matrix(matrix):
    max_pos = np.unravel_index(np.argmax(matrix), matrix.shape)
    return (max_pos[0], max_pos[1])

def reset_matrix (matrix,row, col):
    for j in range (len(matrix[0])):
        matrix[row][j] = 0
    for i in range (len(matrix)):
        matrix[i][col] = 0
    return matrix

def check_matrix(matrix):
    if np.all(matrix == 0):
        return False
    return True

def _group_address(id_p_cand, id_d_cand, id_w_cand,p_cand, d_cand, w_cand):
    if len(w_cand) == 0:
        return []
    
    result = [["" for _ in range(6)] for _ in range(len(w_cand))]
    w_cand_remaining = []
    id_p_cand_use = []
    id_d_cand_use = []
    id_w_cand_use = []
    p_cand_use = []
    d_cand_use = []

    for i in range(len(w_cand)):
        result[i][5] = w_cand[i]
    
    if (len(id_p_cand) <= 1 and len(id_d_cand) <= 1 and len(id_w_cand) <= 1 and
        len(p_cand) <= 1 and len(d_cand) <= 1 and len(w_cand) == 1):

        result[0][0] = id_p_cand[0] if id_p_cand else None
        result[0][1] = id_d_cand[0] if id_d_cand else None
        result[0][2] = id_w_cand[0] if id_w_cand else None
        result[0][3] = p_cand[0] if p_cand else None
        result[0][4] = d_cand[0] if d_cand else None
    
        return result
    

    matrix_w_idp = matrix_cand(w_cand,id_p_cand) if len(id_p_cand) > 0 else None
    matrix_w_idd = matrix_cand(w_cand,id_d_cand) if len(id_d_cand) > 0 else None
    matrix_w_idw = matrix_cand(w_cand,id_w_cand) if len(id_w_cand) > 0 else None
    matrix_w_p = matrix_cand(w_cand,p_cand) if len(p_cand) > 0 else None
    matrix_w_d = matrix_cand(w_cand,d_cand) if len(d_cand) > 0 else None

    while check_matrix(matrix_w_idp):
        row, col = find_matrix(matrix_w_idp)
        result[row][0] = id_p_cand[col]
        id_p_cand_use.append(id_p_cand[col])
        reset_matrix(matrix_w_idp,row, col)
    
    while check_matrix(matrix_w_idd):
        row, col = find_matrix(matrix_w_idd)
        result[row][1] = id_d_cand[col]
        id_d_cand_use.append(id_d_cand[col])
        reset_matrix(matrix_w_idd,row, col)
    
    while check_matrix(matrix_w_idw):
        row, col = find_matrix(matrix_w_idw)
        result[row][2] = id_w_cand[col]
        id_w_cand_use.append(id_w_cand[col])
        reset_matrix(matrix_w_idw,row, col)
    
    while check_matrix(matrix_w_p):
        row, col = find_matrix(matrix_w_p)
        result[row][3] = p_cand[col]
        p_cand_use.append(p_cand[col])
        reset_matrix(matrix_w_p,row, col)
    
    while check_matrix(matrix_w_d):
        row, col = find_matrix(matrix_w_d)
        result[row][4] = d_cand[col]
        d_cand_use.append(d_cand[col])
        reset_matrix(matrix_w_d,row, col)

    idx_list = []
    for i in range(len(w_cand)):
        if result[i][3] == '' and result[i][4] == '':
            if result[i][0] != '':
                id_p_cand_use.remove(result[i][0])
            if result[i][1] != '':
                id_d_cand_use.remove(result[i][1])
            if result[i][2] != '':
                id_w_cand_use.remove(result[i][2])
            w_cand_remaining.append(result[i][5])
            idx_list.append(i)
    
    if len(w_cand_remaining) > 0:
        id_p_cand_use = list(set(id_p_cand) - set(id_p_cand_use))
        id_d_cand_use = list(set(id_d_cand) - set(id_d_cand_use))
        id_w_cand_use = list(set(id_w_cand) - set(id_w_cand_use))
        p_cand_use = list(set(p_cand) - set(p_cand_use))
        d_cand_use = list(set(d_cand) - set(d_cand_use))

        for i in idx_list:
            result[i][0] = id_p_cand_use[0] if id_p_cand_use else None
            result[i][1] = id_d_cand_use[0] if id_d_cand_use else None
            result[i][2] = id_w_cand_use[0] if id_w_cand_use else None
            result[i][3] = p_cand_use[0] if p_cand_use else None
            result[i][4] = d_cand_use[0] if d_cand_use else None
    
    return result

#-----------LỌC CÁC CỘT ĐỊA CHỈ-----------------
def filter_candidates_by_keywords(candidates, keyword):
    if not candidates:
        return []
    if len(candidates) == 1:
        return candidates

    filtered = []
    for col in candidates:
        if any(kw in col.lower() for kw in keyword):
            filtered.append(col)
    return filtered if filtered else candidates

def identify_address_columns_smart(
    df_sample: pd.DataFrame,
    units: Dict[str, Set[str]],
) -> Tuple[list, int]:
    """
    Dùng units từ mapping để detect cột
    """
    if not units:
        return None, None, None, None, None, None
    
    id_provinces = units.get("id_provinces", set())
    id_districts = units.get("id_districts", set())
    id_wards = units.get("id_wards", set())
    provinces = units.get("provinces", set())
    districts = units.get("districts", set())
    wards = units.get("wards", set())

    # Tạo từ điển kết quả
    numeric_dict = {col: can_convert_to_numeric(df_sample[col])
                    for col in df_sample.columns }
    
    df_sample = df_sample.applymap(normalize_sample_value)

    flag = int(len(df_sample) / 100 * 96)

    id_p_candidates = []
    id_d_candidates = []
    id_w_candidates = []
    p_candidates = []
    d_candidates = []
    w_candidates = []
    
    for col in df_sample.columns:
        values = df_sample[col].astype(str)
        values = values[values != ""]
        if len(values) == 0: continue
        n = len(values)

        id_p_match = 0 
        id_d_match = 0
        id_w_match = 0
        p_match = 0
        d_match = 0
        w_match = 0

        for v in values:
            if v in id_provinces: id_p_match += 1
            if v in id_districts: id_d_match += 1
            if v in id_wards: id_w_match += 1
            if v in provinces: p_match += 1
            if v in districts: d_match += 1
            if v in wards: w_match += 1

        if id_p_match > flag:
            id_p_candidates.append(col)
        if id_d_match > flag:
            id_d_candidates.append(col)
        if id_w_match > flag:
            id_w_candidates.append(col)
        if p_match > flag:
            p_candidates.append(col)
        if d_match > flag:
            d_candidates.append(col)
        if w_match > flag:
            w_candidates.append(col)
            
    # Keywords mở rộng cho từng loại
    province_keywords = [
        'tỉnh','thành phố', 'tỉnh/thành phố', 'tỉnh thành',
        'province', 'provincename',
        'prov', 'city',
        'tinh','thanhpho', 'tinhthanh','thanh pho','tinh thanh'
    ]
    
    district_keywords = [
        'huyện','quận','thị xã','huyện/quận',
        'districtname','district', 
        'dist', 'town', 'township', 'town ship',
        'quan','huyen','thi xa','thixa', 'quanhuyen'
    ]
    
    ward_keywords = [
        'xã','phường','thị trấn', 'xã/phường',
        'wardname', 'communename',
        'commune','ward', 'townlet', 
        'xa','phuong','thi tran','thitran', 'phuongxa'
    ]

    id_p_candidates = check_id_address(filter_candidates_by_keywords(id_p_candidates, province_keywords), numeric_dict)
    id_d_candidates = check_id_address(filter_candidates_by_keywords(id_d_candidates, district_keywords), numeric_dict)
    id_w_candidates = check_id_address(filter_candidates_by_keywords(id_w_candidates, ward_keywords), numeric_dict)
    p_candidates = filter_candidates_by_keywords(p_candidates, province_keywords)
    d_candidates = filter_candidates_by_keywords(d_candidates, district_keywords)
    w_candidates = filter_candidates_by_keywords(w_candidates, ward_keywords)

    print ('----------------------------')
    print(id_p_candidates)
    print(id_d_candidates)
    print(id_w_candidates)
    print(p_candidates)
    print(d_candidates)
    print(w_candidates)
    print ('----------------------------')

    
    result = _group_address(id_p_candidates,id_d_candidates,id_w_candidates,p_candidates,d_candidates,w_candidates)

    return result, len(w_candidates)

def validate_columns(province_col: Optional[str], district_col: Optional[str], ward_col: Optional[str]) -> bool:
    """Kiểm tra linh hoạt: cần XÃ + (TỈNH hoặc HUYỆN)"""
    has_ward = ward_col is not None
    has_province_or_district = province_col is not None or district_col is not None
    return has_ward and has_province_or_district