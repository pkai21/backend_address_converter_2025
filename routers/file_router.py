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
from core.conversion.engine import convert_file_async, convert_file_blocking
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

    sample_df = info["sample_df"].head(5).fillna("")
    sample_preview = {
        "columns": sample_df.columns.tolist(),
        "data_rows": sample_df.to_dict(orient="records")
    }

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

    update_task(task_id, pending_groups=groups)

    return {
        "data":{
            "task_id": task_id,
            "groups": groups,
            "all_columns": info["names"],
            "rows": rows,
            "mb": mb,
            "suggested_workers": suggested_workers,
            "sample_preview": sample_preview
        }
    }

# 2. BẮT ĐẦU CHUYỂN ĐỔI VỚI CẤU HÌNH ĐÃ CHỌN
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
        message="Đang chuyển đổi (đồng bộ, vui lòng chờ)...",
    )

    # Chạy blocking (đồng bộ) – vì FE đang chờ response
    await convert_file_blocking(task_id)

    # Sau khi xong → lấy kết quả cuối cùng
    merged_data = get_merged_full_data(task_id)
    success_count = sum(1 for r in merged_data if r.get("Trạng thái chuyển đổi") == "Thành công")
    fail_count = len(merged_data) - success_count
    progress = round(success_count / len(merged_data) * 100, 1) if merged_data else 100

    # Cập nhật task thành công
    update_task(
        task_id,
        status="preview_ready",
        progress=100,
        message="HOÀN THÀNH! Sẵn sàng xem kết quả và chỉnh sửa",
        columns= [col for col in (merged_data[0].keys() if merged_data else []) if col != "id_VNA"],
        result={
            "total_rows": len(merged_data),
            "success_count": success_count,
            "fail_count": fail_count,
            "full_data": merged_data  
        }
    )

    # Trả về luôn task đầy đủ → FE không cần gọi thêm gì nữa!
    task = get_task(task_id)

    return {
        "data": {
            "task": task,
            "message": "Chuyển đổi hoàn tất!",
            "progress": 100,
            "status": "preview_ready"
        }
    }

# 3. LẤY TRẠNG THÁI TASK VÀ DỮ LIỆU ĐÃ XỬ LÝ (NẾU CÓ)
@router.get("/tasks/{task_id}")
async def get_task_status(task_id: str):
    task = get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task không tồn tại")

    return {"data": {"task": task}}

# 4. CẬP NHẬT DÒNG THEO id_VNA 
@router.patch("/tasks/{task_id}/row-by-id-vna/{id_vna}")
async def update_row_by_id_vna(task_id: str, id_vna: str, updated_row: dict):
    task = get_task(task_id)
    if not task or task.get("status") != "preview_ready":
        raise HTTPException(404, detail="Task không tồn tại hoặc chưa sẵn sàng")

    full_data = get_merged_full_data(task_id)
    
    # Tìm dòng theo id_VNA (duy nhất)
    row_index = None
    original_row = None
    for idx, row in enumerate(full_data):
        if str(row.get("id_VNA")) == str(id_vna):  
            row_index = idx
            original_row = row.copy()
            break

    if row_index is None:
        raise HTTPException(404, detail=f"Không tìm thấy dòng có id_VNA = {id_vna}")

    updated_row = {
        **original_row,                   
        **updated_row,                    
        "Trạng thái chuyển đổi": "Thành công"
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
    new_success = sum(1 for r in merged_data if r.get("Trạng thái chuyển đổi") == "Thành công")
    new_fail = len(merged_data) - new_success
    new_progress = round(new_success / len(merged_data) * 100, 1) if merged_data else 100

    return {
        "data": {
            "message": "Đã lưu chỉnh sửa thành công",
            "id_VNA": id_vna,
            "row_index": row_index,                   
            "updated_row": updated_row,
            "total_rows": len(merged_data),
            "success_count": new_success,
            "fail_count": new_fail,
            "progress": new_progress,
            "saved_at": datetime.now().isoformat()
        }
    }

# 5. TẢI FILE KẾT QUẢ ĐÃ CHUYỂN ĐỔI VỀ
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

    return FileResponse(
        path=safe_output_path,
        filename=Path(safe_output_path).name,
        media_type="application/octet-stream"
    )

# 6. LẤY DỮ LIỆU ĐÃ LỌC THEO TRẠNG THÁI (THÀNH CÔNG / LỖI)
@router.get("/tasks/{task_id}/filtered-data")
async def get_filtered_data(
    task_id: str,
    filter_status: str = "all",
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=10, le=200000)
):
    task = get_task(task_id)
    if not task or task.get("status") != "preview_ready":
        raise HTTPException(404, detail="Task không tồn tại")

    full_data = get_merged_full_data(task_id)

    # Lọc
    if filter_status == "success":
        filtered = [r for r in full_data if r.get("Trạng thái chuyển đổi") == "Thành công"]
    elif filter_status == "error":
        filtered = [r for r in full_data if r.get("Trạng thái chuyển đổi") != "Thành công"]
    else:
        filtered = full_data

    # Phân trang
    total = len(filtered)
    start = (page - 1) * page_size
    end = start + page_size
    paginated = filtered[start:end]
    
    return {
        "data": {
            "total_rows": total,
            "success_count": sum(1 for r in filtered if r.get("Trạng thái chuyển đổi") == "Thành công"),
            "fail_count": total - sum(1 for r in filtered if r.get("Trạng thái chuyển đổi") == "Thành công"),
            "page": page,
            "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size,
            "full_data": paginated,
            "task_status": task.get("status"),  
        }
    }