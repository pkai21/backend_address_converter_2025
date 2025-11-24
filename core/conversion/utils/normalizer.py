import re
import pandas as pd
from typing import Tuple
from core.conversion.utils.vietnamese_code import vietnamese_normalize_text

def normalize_place(name: str) -> str:
    """Chuẩn hóa tên địa danh - loại bỏ tiền tố và ký tự thừa (không phân biệt hoa thường)"""
    if pd.isna(name) or not name:
        return ''
    
    # Chuyển về lowercase để xử lý tiền tố
    name_lower = str(name).strip().lower()
    
    # 🔥 TIỀN TỐ VIỆT NAM (case-insensitive)
    prefixes_vn = [
        r'tp\.', r'tx\.', r'tt\.', r'q\.', r'x\.', r'p\.', r't\.', r'h\.',  
        r'thành phố', r'tỉnh', r'tp', r'thủ đô', r'td',                      
        r'huyện', r'quận', r'thị xã',                                      
        r'xã', r'phường', r'thị trấn'                                 
    ]
    
    # 🔥 TIỀN TỐ TIẾNG ANH
    prefixes_en = [
        r'district of', r'dist of', r'county of', r'town of',
        r'ward of', r'commune of', r'township of'
    ]
    
    all_prefixes = prefixes_vn + prefixes_en

    # 🔥 HẬU TỐ TIẾNG ANH
    all_suffixes = [
        r'province', r'prov', 
        r'district', r'dist', r'county', r'town',
        r'ward', r'commune', r'township'
    ]
    
    # Loại bỏ tiền tố
    for prefix_pattern in all_prefixes:
        match = re.match(rf'^{prefix_pattern}\s*', name_lower)
        if match:
            name_lower = name_lower[match.end():].strip()
            break
    
    # Loại bỏ hậu tố
    for suffix_pattern in all_suffixes:
        match = re.search(rf'\s*{suffix_pattern}$', name_lower.lower())
        if match:
            name_lower = name_lower[:match.start()].strip()
            break

    # Xóa ký tự đặc biệt và khoảng trắng thừa
    name_lower = re.sub(r'[,\(\)\[\]\-\+]+', ' ', name_lower)
    name_lower = re.sub(r'[.,/\s]+$', '', name_lower)  # Xóa .,/,space ở cuối
    name_lower = re.sub(r'\s+', ' ', name_lower).strip()
    
    # Xóa số 0 ở đầu (nếu có)
    name_lower = re.sub(r'^0+', '', name_lower).strip()
    return name_lower

from typing import Tuple

def normalize_mapping_key(prov: str, dist: str, ward: str) -> Tuple[str, str, str]:
    """
    Chuẩn hóa 3 thành phần địa chỉ (tỉnh, huyện, xã) để tạo key tra cứu.
    Gồm 3 bước:
      1️ normalize_place(): loại bỏ tiền tố (tỉnh, huyện, xã, phường, thị trấn, ...)
      2️ vietnamese_normalize_text(): chuẩn hóa dấu tiếng Việt về cùng dạng
      3️ strip(): loại bỏ khoảng trắng thừa ở đầu và cuối
    """
    return tuple(
        vietnamese_normalize_text(normalize_place(x)).strip()  # thực hiện 3 bước chuẩn hóa
        for x in (prov, dist, ward)                             # áp dụng cho cả 3 phần: tỉnh, huyện, xã
    )

