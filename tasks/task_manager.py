# tasks/task_manager.py – BẢN HOÀN HẢO SAU KHI FIX BUG MẤT DỮ LIỆU
import pandas as pd
from sqlalchemy.orm import Session
from core.models import Task, TaskEdit
from core.database import engine
import json
from sqlalchemy import update


def create_task(task_id: str, filename: str, filesize: int, suggested_workers: int = 1):
    with Session(engine) as db:
        task = Task(
            task_id=task_id,
            filename=filename,
            filesize=filesize,
            suggested_workers=suggested_workers,
            n_workers=suggested_workers,
            pending_configs=[],     
            selected_configs=[],
            status="pending",
            progress=0,
            message="Đang chờ xử lý...",
            created_at=None,         
            updated_at=None,
        )
        db.add(task)
        db.commit()
        db.refresh(task)

def to_serializable(obj):
    if isinstance(obj, dict):
        return {k: to_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [to_serializable(i) for i in obj]
    elif hasattr(obj, 'item'):
        return obj.item()
    elif hasattr(obj, 'tolist'):
        return obj.tolist()
    elif isinstance(obj, (pd.Timestamp, pd.NaT)):
        return obj.isoformat() if pd.notna(obj) else None
    else:
        return obj

def update_task(task_id: str, **kwargs):
    with Session(engine) as db:
        task = db.query(Task).filter(Task.task_id == task_id).first()
        if not task:
            return False

        for key, value in kwargs.items():
            if not hasattr(task, key):
                continue

            if key == "progress":
                value = min(100, max(0, int(value or 0)))

            if key in ("pending_configs", "selected_configs", "result") and value is not None:
                value = json.loads(json.dumps(value, default=str))

            setattr(task, key, value)

        db.commit()
        return True

def get_task(task_id: str) -> dict | None:
    with Session(engine) as db:
        task = db.query(Task).filter(Task.task_id == task_id).first()
        if not task:
            return None

        return {
            "task_id": task.task_id,
            "filename": task.filename,
            "filesize": task.filesize,
            "status": task.status,
            "progress": task.progress,
            "message": task.message or "",
            "created_at": task.created_at.isoformat() if task.created_at else None,
            "suggested_workers": task.suggested_workers or 1,
            "n_workers": task.n_workers or 1,
            "pending_configs": task.pending_configs or [],
            "selected_configs": task.selected_configs or [],
            "result": task.result or {}
        }

def get_merged_full_data(task_id: str):
    """Trả về full_data đã được MERGE (không ghi đè) các edit thủ công"""
    task = get_task(task_id)
    if not task or not task.get("result") or "full_data" not in task["result"]:
        return []

    full_data = task["result"]["full_data"][:]  # copy để an toàn

    with Session(engine) as db:
        edits = db.query(TaskEdit).filter(TaskEdit.task_id == task_id).order_by(TaskEdit.edited_at).all()
        for edit in edits:
            idx = edit.row_index
            if 0 <= idx < len(full_data):
                original_row = full_data[idx] or {}  # dòng gốc
                edited_row = edit.edited_row or {}   # dữ liệu người dùng sửa

                # MERGE: ưu tiên edited_row, giữ lại các field cũ nếu không có trong edit
                merged_row = {**original_row, **edited_row}
                full_data[idx] = merged_row

    return full_data

def apply_edits_to_result(task_id: str):
    """Dùng khi tải file: merge tất cả edit vào result.full_data để xuất file sạch 100%"""
    merged = get_merged_full_data(task_id)
    current_result = get_task(task_id).get("result", {})
    update_task(
        task_id,
        result={**current_result, "full_data": merged}
    )