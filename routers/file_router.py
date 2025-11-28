# routers/file_router.py – BẢN HOÀN HẢO CUỐI CÙNG
import shutil
from fastapi import APIRouter, File, UploadFile, BackgroundTasks, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from core.database import engine
from core.models import TaskEdit
from fastapi.responses import StreamingResponse,FileResponse
import json
from pathlib import Path
import uuid
import pandas as pd
from nanoid import generate

from core.conversion.utils.save_file import save_file
from tasks.task_manager import apply_edits_to_result, create_task, get_merged_full_data, update_task, get_task
from core.conversion.engine import convert_file_blocking
from core.conversion.load_file.file_info import get_file_info
from core.conversion.utils.column_detector import identify_address_columns_smart
from core.conversion import mapping_table, units
from config.settings import Settings
from datetime import datetime

router = APIRouter()

UPLOAD_DIR = Path("uploads")
DOWNLOAD_DIR = Path("downloads")
UPLOAD_DIR.mkdir(exist_ok=True)
DOWNLOAD_DIR.mkdir(exist_ok=True)
SAMPLE_DATA_DIST = {}

# 1. TẢI FILE LÊN VÀ PHÁT HIỆN CỘT ĐỊA CHỈ
@router.post("/upload-and-detect")
async def upload_and_detect(file: UploadFile = File(...)):
    task_id = generate(size=14)
    ext = Path(file.filename).suffix.lower()
    input_path = UPLOAD_DIR / f"{task_id}{ext}"

    with open(input_path, "wb") as f:
        shutil.copyfileobj(file.file, f)


    info = get_file_info(str(input_path))
    if info["error"]:
        raise HTTPException(400, detail=info["error"])

    rows = info.get("rows", 0)
    mb = round(info.get("mb", 0), 1)
    suggested_workers = min(8, max(1, rows // 15000 + int(mb // 25) + 1))

    create_task(task_id, file.filename, input_path.stat().st_size, suggested_workers)

    global SAMPLE_DATA_DIST
    SAMPLE_DATA_DIST = info["sample_df"].head(5).to_dict(orient="records")

    configs, _ = identify_address_columns_smart(info["sample_df"], units)

    groups = []
    for g in configs:
        id_p, id_d, id_w, p, d, w = g
        groups.append({
            "id_province": id_p,
            "id_district": id_d,
            "id_ward": id_w,
            "province": p,
            "district": d,
            "ward": w
        })

    update_task(task_id, pending_groups=groups, step = 1)

    return {
        "data":{
            "task_id": task_id,
            "step": 1,
            "groups": groups,
            "all_columns": info["names"],
            "rows": rows,
            "mb": mb,
            "suggested_workers": suggested_workers
        }
    }

# 2.PREWIEW SAMPLE DỮ LIỆU
@router.get("/tasks/{task_id}/group-preview")
async def get_group_preview(
    task_id: str,
    col: list[str] = Query(..., description="Danh sách tên cột cần xem preview")
):

    # Lọc dữ liệu theo các cột hợp lệ
    global SAMPLE_DATA_DIST

    if not SAMPLE_DATA_DIST:
        raise HTTPException(400, detail="Chưa có dữ liệu sample. Hãy upload file trước.")

    # Lọc từng row
    filtered_data = [
        {c: row.get(c) for c in col if c in row}
        for row in SAMPLE_DATA_DIST
    ]

    return {
        "data": {
            "columns": col,        
            "sample_data": filtered_data,     
            "total_sample_rows": len(SAMPLE_DATA_DIST),
        }
    }

# 3. BẮT ĐẦU CHUYỂN ĐỔI VỚI CẤU HÌNH ĐÃ CHỌN
@router.post("/start-conversion/{task_id}")
async def start_conversion(task_id: str, payload: dict):
    groups = payload.get("groups", [])
    n_workers = payload.get("n_workers", 1)

    if not groups:
        raise HTTPException(400, detail="Chưa chọn nhóm địa chỉ nào!")

    # Cập nhật trạng thái đang xử lý
    update_task(
        task_id,
        selected_groups=groups,
        n_workers=n_workers,
        status="processing",
        progress=0,
        step = 1,
        message="Đang chuyển đổi (đồng bộ, vui lòng chờ)...",
    )

    # Chạy blocking (đồng bộ) – vì FE đang chờ response
    await convert_file_blocking(task_id)

    # Trả về luôn task đầy đủ → FE không cần gọi thêm gì nữa!
    task = get_task(task_id)

    return {
        "data": {
            "task": task,
            "message": "Chuyển đổi hoàn tất!",
        }
    }

# 4. LẤY TRẠNG THÁI TASK VÀ DỮ LIỆU ĐÃ XỬ LÝ 
@router.get("/tasks/{task_id}")
async def get_task_status(task_id: str):
    task = get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task không tồn tại")

    return {"data": {"task": task}}

# 5. CẬP NHẬT DÒNG THEO id 
@router.post("/tasks/{task_id}/row-by-id/{id}")
async def update_row_by_id(task_id: str, id: str, updated_row: dict):
    task = get_task(task_id)
    if not task or task.get("status") != "preview_ready":
        raise HTTPException(404, detail="Task không tồn tại hoặc chưa sẵn sàng")

    full_data = get_task(task_id)['result']['full_data']
    
    # Tìm dòng theo id (duy nhất)
    row_index = None
    original_row = None
    for idx, row in enumerate(full_data):
        if str(row.get("id")) == str(id):  
            row_index = idx
            original_row = row.copy()
            break

    if row_index is None:
        raise HTTPException(404, detail=f"Không tìm thấy dòng có id = {id}")

    updated_row = {
        **original_row,                   
        **updated_row,                    
        "statusState": "Thành công"
    }

    with Session(engine) as db:
        stmt = postgresql_insert(TaskEdit).values(
            task_id=task_id,
            row_index=row_index,
            original_row=original_row,
            edited_row=updated_row
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=['task_id', 'row_index'],
            set_={
                "edited_row": updated_row,
                "original_row": original_row,    
                "edited_at": func.now()
            }
        )
        db.execute(stmt)
        db.commit()

    # Tính lại thống kê
    merged_data = get_merged_full_data(task_id)
    new_success = sum(1 for r in merged_data if r.get("statusState") == "Thành công")
    new_fail = len(merged_data) - new_success
    new_progress = round(new_success / len(merged_data) * 100, 1) if merged_data else 100

    update_task(task_id, step=2, result={"success_count": new_success, "fail_count": new_fail, "full_data": merged_data})

    return {
        "data": {
            "message": "Đã lưu chỉnh sửa thành công",
            "id": id,
            "row_index": row_index,                   
            "updated_row": updated_row,
            "total_rows": len(merged_data),
            "success_count": new_success,
            "fail_count": new_fail,
            "progress": new_progress,
            "step": 2,
            "saved_at": datetime.now().isoformat()
        }
    }

# 6. TẢI FILE KẾT QUẢ ĐÃ CHUYỂN ĐỔI VỀ
@router.get("/download-and-save/{task_id}")
async def download_and_save(task_id: str):
    task = get_task(task_id)
    if not task or task.get("status") != "preview_ready":
        raise HTTPException(400, detail="Chưa sẵn sàng")

    # Áp dụng hết edit trước khi xuất
    apply_edits_to_result(task_id)

    # Sau đó lấy lại task mới nhất
    task = get_task(task_id)
    df = pd.DataFrame(task["result"]["full_data"])

    input_filename = task["filename"]
    pretty_name = Settings.get_output_filename(input_filename)
    safe_output_path = Settings._ensure_unique_path(str(DOWNLOAD_DIR / pretty_name))

    # Lưu file đúng định dạng
    success = save_file(df, safe_output_path)
    if not success:
        raise HTTPException(500, detail="Lỗi lưu file")

    update_task(task_id, step = 2)

    return FileResponse(
        path=safe_output_path,
        filename=Path(safe_output_path).name,
        media_type="application/octet-stream"
    )

# 7. LẤY DỮ LIỆU ĐÃ LỌC THEO TRẠNG THÁI (THÀNH CÔNG / LỖI)
@router.get("/tasks/{task_id}/filtered-data")
async def get_filtered_data(
    task_id: str,
    filter_status: str = "all"
):
    task = get_task(task_id)
    if not task or task.get("status") != "preview_ready":
        raise HTTPException(404, detail="Task không tồn tại")

    full_data = get_merged_full_data(task_id) 
    # Lọc
    if filter_status == "success":
        filtered = [r for r in full_data if r.get("statusState") == "Thành công"]
    elif filter_status == "error":
        filtered = [r for r in full_data if r.get("statusState") != "Thành công"]
    else:
        filtered = full_data

    update_task(task_id, step = 2, created_at = datetime.now().isoformat())

    task = get_task(task_id)

    return {
        "data": {
            "task": {
                "task_id": task_id,
                "filename":task['filename'],
                "filesize": task['filesize'],
                "status": "preview_ready",
                "progress": task['progress'],
                "message": "HOÀN THÀNH LỌC! Sẵn sàng xem kết quả và chỉnh sửa",
                "created_at": datetime.now().isoformat(),
                "suggested_workers": task['suggested_workers'],
                "n_workers": task['n_workers'],
                "pending_groups": task['pending_groups'],
                "selected_groups": task['selected_groups'],
                "columns": task['columns'] ,
                "step": 2,
                "result": {
                    "total_rows": len(full_data),
                    "fail_count": task['result']['fail_count'],
                    "success_count": task['result']['success_count'],
                    "full_data": filtered
                }
            },
            "message": "Lọc hoàn tất!"
        }
    }