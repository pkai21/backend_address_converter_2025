# tasks/task_manager.py – BẢN HOÀN HẢO SAU KHI FIX BUG MẤT DỮ LIỆU
import pandas as pd
from sqlalchemy.orm import Session
from core.models import Task, TaskEdit
from core.database import engine
import json
from sqlalchemy import update
import numpy as np
from datetime import datetime, date

def to_serializable(val):
    if pd.isna(val):
        return None
    if isinstance(val, (pd.Timestamp, datetime, date)):
        return val.isoformat()
    if isinstance(val, (np.integer, np.int64)):
        return int(val)
    if isinstance(val, (np.floating, np.float64)):
        return float(val)
    if isinstance(val, np.ndarray):
        return val.tolist()
    if isinstance(val, (bytes, bytearray, memoryview)):
        return None  # hoặc str(val, 'utf-8', errors='ignore') nếu muốn giữ
    try:
        # Thử ép kiểu về str nếu là kiểu lạ
        if hasattr(val, '__str__'):
            str_val = str(val)
            if str_val != "":  
                return str_val
    except:
        pass
    return val

def make_json_serializable(obj):
    if isinstance(obj, dict):
        return {k: make_json_serializable(v) for k, v in obj.items() if v is not None or True}
    elif isinstance(obj, list):
        return [make_json_serializable(i) for i in obj]
    elif pd.isna(obj):
        return None
    elif isinstance(obj, (datetime, date, pd.Timestamp)):
        return obj.isoformat()
    elif isinstance(obj, (bytes, bytearray, memoryview)):
        return None  # Bỏ qua hình ảnh, object trong Excel
    else:
        try:
            json.dumps(obj)  # Test xem có serializable không
            return obj
        except:
            return str(obj)  # Cuối cùng thì ép string

def create_task(task_id: str, filename: str, filesize: int, suggested_workers: int = 1):
    with Session(engine) as db:
        task = Task(
            task_id=task_id,
            filename=filename,
            filesize=filesize,
            suggested_workers=suggested_workers,
            n_workers=suggested_workers,
            pending_groups=[],     
            selected_groups=[],
            columns=[],
            step = 0,
            status="pending",
            progress=0,
            message="Đang chờ xử lý...",
            created_at=None,         
            updated_at=None,
        )
        db.add(task)
        db.commit()
        db.refresh(task)

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

            if key in ("pending_groups", "selected_groups", "columns", "step", "result", "created_at") and value is not None:
                value = make_json_serializable(value)

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
            "pending_groups": task.pending_groups or [],
            "selected_groups": task.selected_groups or [],
            "columns": task.columns or [],
            "step": task.step or 0,
            "result": task.result or {}
        }

def get_merged_full_data(task_id: str):
    """Trả về full_data đã được MERGE (không ghi đè) các edit thủ công"""
    task = get_task(task_id)
    if not task or not task.get("result") or "full_data" not in task["result"]:
        return []

    full_data = task["result"]["full_data"][:] 
    final_order = list(dict.fromkeys(task.get("columns", []) + ["id"]))

    with Session(engine) as db:
        edits = db.query(TaskEdit).filter(TaskEdit.task_id == task_id).order_by(TaskEdit.edited_at).all()
        for edit in edits:
            idx = edit.row_index
            if 0 <= idx < len(full_data):
                original_row = full_data[idx] or {}  
                edited_row = edit.edited_row or {}  

                merged_row = {**original_row, **edited_row}
                full_data[idx] = merged_row

    if final_order:
        full_data = [
            {col: row.get(col, "") for col in final_order} for row in full_data
        ]
    
    return full_data

def apply_edits_to_result(task_id: str):
    """Dùng khi tải file: merge tất cả edit vào result.full_data để xuất file sạch 100%"""
    merged = get_merged_full_data(task_id)
    current_result = get_task(task_id).get("result", {})
    update_task(
        task_id,
        result={**current_result, "full_data": merged}
    )