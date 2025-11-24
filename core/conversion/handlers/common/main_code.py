import pandas as pd
from multiprocessing import Pool, cpu_count
from typing import Dict, Optional, Tuple, List
from core.conversion.utils.column_detector import validate_columns
from core.conversion.utils.normalizer import normalize_mapping_key

def find_mapping_key(mapping_table: Dict[Tuple[str, str, str], List[Tuple[str, str, str, str]]], address: tuple) -> str:
    """
    mapping_table: dict với key là tuple (name1, name2, id)
    address: tuple (name1, name2, id)
    Trả về key (tuple) nếu thỏa: address[2] == key[2] và (address[0] == key[0] or address[1] == key[1])
    Ngược lại trả về ''.
    """
    if address in mapping_table:
        return address
    else:
        countkey = 0
        keyfound = ''
        for key in mapping_table.keys():
            if address[2] == key[2] and (address[0] == key[0] or address[1] == key[1]):
                countkey += 1
                if countkey > 1:
                    return ''
                keyfound = key
        if countkey == 1:
            return keyfound
    return ''

# ------------------- HÀM XỬ LÝ TỪNG CHUNK  -------------------
def _process_chunk(args):
    chunk_idx, chunk_df, map_dict, province_col, district_col, ward_col, province_id_col_name, ward_id_col_name, suffix = args


    for idx, row in chunk_df.iterrows():
        province_raw = str(row.get(province_col, '')) if province_col else ''
        district_raw = str(row.get(district_col, '')) if district_col else ''
        ward_raw = str(row.get(ward_col, ''))

        key = normalize_mapping_key(province_raw, district_raw, ward_raw)
        lower_key = tuple(k.lower() if k else '' for k in key)

        # XỬ LÝ MATCHING
        key_found = find_mapping_key(map_dict, lower_key)
        if key_found:
            values = map_dict[key_found]
            if values:
                # Tuple đầu tiên:
                prov_new, ward_new, id_prov, id_ward = values[0]
                if province_col:
                    chunk_df.at[idx, province_col] = prov_new
                else:
                    chunk_df.at[idx, f'provinceName{suffix}'] = prov_new
                chunk_df.at[idx, ward_col] = ward_new
                chunk_df.at[idx, province_id_col_name] = id_prov
                chunk_df.at[idx, ward_id_col_name] = id_ward

                # Xử lý các option (từ tuple thứ 2 trở đi)
                for opt_num, val in enumerate(values[1:], start=2):
                    prov_new_opt, ward_new_opt, id_prov_opt, id_ward_opt = val

                    ward_id_col = f'{ward_id_col_name}_option_{opt_num}'
                    ward_name_col = f'{ward_col}_option_{opt_num}'

                    # Thêm cột nếu chưa có
                    if ward_id_col not in chunk_df.columns:
                        prev_col = f'{ward_col}_option_{opt_num-1}' if opt_num > 2 else ward_col
                        insert_pos = chunk_df.columns.get_loc(prev_col) + 1 
                        chunk_df.insert(insert_pos, ward_id_col, '')
                    chunk_df.at[idx, ward_id_col] = id_ward_opt

                    if ward_name_col not in chunk_df.columns:
                        insert_pos = chunk_df.columns.get_loc(ward_id_col) + 1
                        chunk_df.insert(insert_pos, ward_name_col, '')
                    chunk_df.at[idx, ward_name_col] = ward_new_opt
            if  chunk_df.at[idx, 'Trạng thái chuyển đổi'] == '':
                chunk_df.at[idx, 'Trạng thái chuyển đổi'] = 'Thành công'
        else:
            if chunk_df.at[idx, 'Trạng thái chuyển đổi'] == '' or chunk_df.at[idx, 'Trạng thái chuyển đổi'] == 'Thành công':
                chunk_df.at[idx, 'Trạng thái chuyển đổi'] = f'Lỗi {suffix}'
            else:
                chunk_df.at[idx, 'Trạng thái chuyển đổi'] += f';{suffix}'
    return chunk_df


# ------------------- HÀM CHÍNH process_df (song song) -------------------
def process_df_with_suffix(df: pd.DataFrame,
                           map_dict: Dict[Tuple[str, str, str], List[Tuple[str, str, str, str]]],
                           id_province_col: Optional[str] = None,
                           id_district_col: Optional[str] = None,
                           id_ward_col: Optional[str] = None,
                           province_col: Optional[str] = None,
                           district_col: Optional[str] = None,
                           ward_col: Optional[str] = None,
                           suffix: str = "",
                           pool=None) -> pd.DataFrame:
    """
    Xử lý 1 nhóm địa chỉ → thêm cột với suffix → trả về df mới.
    """
    if not validate_columns(province_col, district_col, ward_col):
        print("Cảnh báo: Thiếu cột địa chỉ cần thiết. Bỏ qua nhóm này.")
        return df

    total_rows = len(df)
    if total_rows == 0:
        return df

    # --- TẠO TÊN CỘT VỚI SUFFIX ---
    province_id_col_name = id_province_col if id_province_col else f"{'province_id'}{suffix}"
    ward_id_col_name = id_ward_col if id_ward_col else f"{'ward_id'}{suffix}"

    # Xóa cột cũ id nếu có 
    for col in [id_district_col, id_province_col, id_ward_col]:
        if col and col in df.columns:
            df = df.drop(columns=[col])

    # --- THÊM CỘT province_id ---
    if province_id_col_name not in df.columns:
        ref_col = province_col or ward_col
        if ref_col and ref_col in df.columns:
            pos = df.columns.get_loc(ref_col)
            df.insert(pos, province_id_col_name, '')
        else:
            df[province_id_col_name] = ''

    # --- THÊM CỘT ward_id ---
    if ward_id_col_name not in df.columns and ward_col in df.columns:
        pos = df.columns.get_loc(ward_col)
        df.insert(pos, ward_id_col_name, '')

    # --- THÊM provinceName nếu cần ---
    if not province_col and f'provinceName{suffix}' not in df.columns:
        pos = df.columns.get_loc(province_id_col_name) + 1
        df.insert(pos, f'provinceName{suffix}', '')

    # --- CHIA CHUNK & XỬ LÝ SONG SONG ---
    n_workers = pool._processes if pool else 1
    ideal_chunk_size = 10000
    chunk_size = max(ideal_chunk_size, total_rows // (n_workers * 2))
    chunks = [df[i:i + chunk_size] for i in range(0, total_rows, chunk_size)]

    chunk_args = [
        (i, chunk.copy(), map_dict, province_col, district_col, ward_col,
         province_id_col_name, ward_id_col_name, suffix)
        for i, chunk in enumerate(chunks)
    ]

    if pool:
        results = pool.map(_process_chunk, chunk_args)
    else:
        with Pool(n_workers) as temp_pool:
            results = temp_pool.map(_process_chunk, chunk_args)

    # --- GỘP KẾT QUẢ ---
    result_df = pd.concat(results, ignore_index=True, sort=False)

    # XÓA CỘT HUYỆN
    if district_col and district_col in result_df.columns:
        result_df = result_df.drop(columns=[district_col])

    return result_df