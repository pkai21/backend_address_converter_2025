import time
import multiprocessing
from pathlib import Path
from core.conversion.handlers import get_handler
from tasks.task_manager import update_task,get_task
from core.conversion import mapping_table, units
import asyncio
from typing import Any

def _run_conversion_sync(task_id: str) -> None:
    """
    Hàm blocking thật sự – chứa toàn bộ logic multiprocessing
    """
    try:
        current_task = get_task(task_id)
        if not current_task:
            raise Exception("Không tìm thấy task")

        filename = current_task["filename"]
        input_path = Path("uploads") / f"{task_id}{Path(filename).suffix}"
        str_input_path = str(input_path)

        n_workers = current_task.get("n_workers") or current_task.get("suggested_workers", 4)
        n_workers = int(n_workers)
        
        raw_groups = current_task.get("selected_groups", [])
        if not raw_groups:
            raise Exception("Không có nhóm địa chỉ nào được chọn!")

        address_groups = []
        for cfg in raw_groups:
            address_groups.append((
                cfg.get("id_province"),
                cfg.get("id_district"),
                cfg.get("id_ward"),
                cfg.get("province"),
                cfg.get("district"),
                cfg.get("ward")
            ))

        handler_func = get_handler(input_path.suffix)
        if not handler_func:
            raise Exception(f"Không hỗ trợ định dạng file: {input_path.suffix}")

        start_time = time.time()

        with multiprocessing.Pool(processes=n_workers) as pool:
            result = handler_func(
                input_file=str_input_path,
                map_dict=mapping_table,
                address_groups=address_groups,
                pool=pool
            )

        progress = round(result["success_count"] / result["total_rows"] * 100, 1) if result["total_rows"] > 0 else 0

        elapsed = time.time() - start_time

        if result.get("success"):
            update_task(task_id, 
                status = "preview_ready",
                progress = progress,
                message = f"HOÀN THÀNH trong {elapsed:.1f}s, Sẵn sàng xem kết quả và chỉnh sửa!",
                columns = result["columns"],
                step = 2,
                result={ 
                    "total_rows": result["total_rows"],
                    "success_count": result["success_count"],
                    "fail_count": result["fail_count"],
                    "full_data": result["full_df"]
                }
            )
        else:
            raise Exception("Handler xử lý thất bại")

    except Exception as e:
        update_task(task_id, status="failed", message=f"Lỗi: {str(e)}", progress=0)


# Hàm async
async def convert_file_blocking(task_id: str):
    """
    Chạy blocking function trong thread pool → an toàn với FastAPI + asyncio
    """
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _run_conversion_sync, task_id)