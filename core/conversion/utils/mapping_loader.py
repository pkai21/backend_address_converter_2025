import json
import os
from typing import Dict, Tuple, List, Set
from config.settings import Settings
from .normalizer import normalize_mapping_key, normalize_place
from .vietnamese_code import vietnamese_normalize_text

def load_mapping_and_units(mapping_file: str = None) -> Dict[Tuple[str, str, str], List[Tuple[str, str, str, str]]]:
    """
    Load mapping và lưu tất cả value vào một list thay vì bỏ qua duplicate
    """
    if mapping_file is None:
        mapping_file = Settings.MAPPING_FILE
    
    if not os.path.exists(mapping_file):
        raise FileNotFoundError(f"❌ Mapping file không tồn tại: {mapping_file}")
    
    # Đọc JSON
    with open(mapping_file, 'r', encoding='utf-8') as f:
        raw_mappings = json.load(f)
    
    # TẠO BẢNG HASH
    mapping_table: Dict[Tuple[str, str, str], List[Tuple[str, str, str, str]]] = {}
    id_provinces_set: Set[str] = set()
    id_districts_set: Set[str] = set()
    id_wards_set: Set[str] = set()
    provinces_set: Set[str] = set()
    districts_set: Set[str] = set()
    wards_set: Set[str] = set()
    
    for i, raw_row in enumerate(raw_mappings):
        id_prov_old_raw = (str(raw_row.get('Mã I (CŨ)', '')).strip())
        id_dist_old_raw = (str(raw_row.get('Mã II (CŨ)', '')).strip())
        id_ward_old_raw = (str(raw_row.get('Mã III (CŨ)', '')).strip())
        prov_old_raw = (str(raw_row.get('Tỉnh (CŨ)', '')).strip())
        dist_old_raw = (str(raw_row.get('Huyện (CŨ)', '')).strip())
        ward_old_raw = (str(raw_row.get('Xã (CŨ)', '')).strip())
        
        prov_new_raw = str(raw_row.get('Tỉnh', '')).strip()
        ward_new_raw = str(raw_row.get('Xã', '')).strip()
        id_prov_new_raw = str(raw_row.get('Mã I', '')).strip()
        id_ward_new_raw = str(raw_row.get('Mã III', '')).strip()
        
        key = normalize_mapping_key(prov_old_raw, dist_old_raw, ward_old_raw)
        lower_key = tuple(k.lower() if k else '' for k in key)
        value = (prov_new_raw, ward_new_raw, id_prov_new_raw, id_ward_new_raw)
        
        if lower_key in mapping_table:
            mapping_table[lower_key].append(value)
        else:
            mapping_table[lower_key] = [value]

        # === 2. THU THẬP TÊN CHUẨN TỪ CŨ (đã chuẩn hóa) ===
        if id_prov_old_raw:
            id_provinces_set.add(id_prov_old_raw)
        if id_dist_old_raw:
            id_districts_set.add(id_dist_old_raw)
        if id_ward_old_raw:
            id_wards_set.add(id_ward_old_raw)
        if prov_old_raw:
            norm = vietnamese_normalize_text(normalize_place(prov_old_raw)).strip()
            if norm: provinces_set.add(norm)
        if dist_old_raw:
            norm = vietnamese_normalize_text(normalize_place(dist_old_raw)).strip()
            if norm: districts_set.add(norm)
        if ward_old_raw:
            norm = vietnamese_normalize_text(normalize_place(ward_old_raw)).strip()
            if norm: wards_set.add(norm)
    
    # BẢNG TỈNH, HUYỆN, XÃ cŨ
    units = {
        "id_provinces": id_provinces_set,
        "id_districts": id_districts_set,
        "id_wards": id_wards_set,
        "provinces": provinces_set,
        "districts": districts_set,
        "wards": wards_set
    }

    return mapping_table, units