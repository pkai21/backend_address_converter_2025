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
from core.conversion.engine import convert_file_async
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
        "rows": sample_df.to_dict(orient="records")
    }

    groups, _ = identify_address_columns_smart(info["sample_df"], units)

    configs = []
    for g in groups:
        id_p, id_d, id_w, p, d, w = g
        configs.append({
            "id_province": id_p,
            "id_district": id_d,
            "id_ward": id_w,
            "province": p,
            "district": d,
            "ward": w
        })

    update_task(task_id, pending_configs=configs)

    return {
        "task_id": task_id,
        "groups": configs,
        "all_columns": info["names"],
        "rows": rows,
        "mb": mb,
        "suggested_workers": suggested_workers,
        "sample_preview": sample_preview
    }

# 2. BẮT ĐẦU CHUYỂN ĐỔI VỚI CẤU HÌNH ĐÃ CHỌN
@router.post("/start-conversion/{task_id}")
async def start_conversion(task_id: str, payload: dict, background_tasks: BackgroundTasks):
    configs = payload.get("configs", [])
    n_workers = payload.get("n_workers")

    if not configs:
        raise HTTPException(400, detail="Chưa chọn nhóm địa chỉ nào!")

    update_task(
        task_id,
        selected_configs=configs,
        n_workers=n_workers,
        status="processing",
        progress=10,
        message="Đang chuẩn bị xử lý song song..."
    )

    background_tasks.add_task(convert_file_async, task_id)

    return {"message": "Đã bắt đầu chuyển đổi!"}

# 3. LẤY TRẠNG THÁI TASK VÀ DỮ LIỆU ĐÃ XỬ LÝ (NẾU CÓ)
@router.get("/tasks/{task_id}")
async def get_task_status(task_id: str):
    task = get_task(task_id)  
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    def json_stream():
        yield "{"
        first = True
        for key, value in task.items():
            if not first:
                yield ","
            first = False

            if key == "result" and value and "full_data" in value:
                merged_data = get_merged_full_data(task_id)  # ← Dùng merged_data ở đây luôn!

                yield f'"{key}":{{'
                yield f'"total_rows": {value.get("total_rows", 0)},'
                # Cập nhật lại success_count thực tế sau khi đã merge edit
                actual_success = len([r for r in merged_data if r.get("Trạng thái chuyển đổi") == "Thành công"])
                yield f'"success_count": {actual_success},'
                yield f'"fail_count": {len(merged_data) - actual_success},'
                yield '"full_data":['
                for i, row in enumerate(merged_data):  # ← Dùng merged_data, không phải full_data!!!
                    if i > 0:
                        yield ","
                    yield json.dumps(row, ensure_ascii=False)
                yield "]}"
            else:
                # ← Fix lỗi Timestamp, numpy ở đây luôn
                safe_value = json.loads(json.dumps(value, default=str)) if value is not None else None
                yield f'"{key}": {json.dumps(safe_value, ensure_ascii=False)}'
        yield "}"
    return StreamingResponse(json_stream(), media_type="application/json")

# 4. CẬP NHẬT DÒNG ĐÃ CHỈNH SỬA TỪ FRONTEND
@router.patch("/tasks/{task_id}/row/{row_index}")
async def update_row(task_id: str, row_index: int, updated_row: dict):
    task = get_task(task_id)
    if not task or task.get("status") != "preview_ready":
        raise HTTPException(404, detail="Task không tồn tại hoặc chưa sẵn sàng")

    full_data = task["result"]["full_data"]
    if row_index < 0 or row_index >= len(full_data):
        raise HTTPException(400, detail="Dòng không hợp lệ")

    original_row = full_data[row_index].copy()

    # Đánh dấu thành công
    updated_row = {**updated_row, "Trạng thái chuyển đổi": "Thành công"}

    # Lưu vào bảng task_edits (upsert)
    with Session(engine) as db:
        stmt = postgresql_insert(TaskEdit).values(
            task_id=task_id,
            row_index=row_index,
            original_row=original_row,
            edited_row=updated_row
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=['task_id', 'row_index'],
            set_={"edited_row": updated_row, "edited_at": func.now()}
        )
        db.execute(stmt)
        db.commit()

    # Tính lại thống kê sau khi đã apply edit
    merged_data = get_merged_full_data(task_id)
    new_success = sum(1 for r in merged_data if r.get("Trạng thái chuyển đổi") == "Thành công")
    new_fail = len(merged_data) - new_success

    # Trả lại đủ thứ FE cần để update realtime
    return {
        "message": "Đã lưu chỉnh sửa thành công",
        "row_index": row_index,
        "updated_row": updated_row,                  
        "status": "Thành công",                      
        "total_rows": len(merged_data),
        "success_count": new_success,                
        "fail_count": new_fail,                      
        "progress": round(new_success / len(merged_data) * 100, 1) if merged_data else 100,
        "saved_at": datetime.now().isoformat()
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
        "total_rows": total,
        "success_count": sum(1 for r in filtered if r.get("Trạng thái chuyển đổi") == "Thành công"),
        "fail_count": total - sum(1 for r in filtered if r.get("Trạng thái chuyển đổi") == "Thành công"),
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size,
        "full_data": paginated  
    }