import time
import multiprocessing
from pathlib import Path
from core.conversion.handlers import get_handler
from tasks.task_manager import update_task,get_task
from core.conversion import mapping_table, units

def convert_file_async(task_id: str):
    """
    Xử lý file – trả về full data + thống kê, không lưu file ngay
    Chỉ lưu khi người dùng nhấn tải
    """
    try:
        update_task(task_id, status="processing", progress=5, message="Đang chuẩn bị xử lý song song...")

        current_task = get_task(task_id)
        if not current_task:
            raise Exception("Không tìm thấy task")

        filename = current_task["filename"]
        input_path = Path("uploads") / f"{task_id}{Path(filename).suffix}"
        str_input_path = str(input_path)

        # LẤY SỐ LUỒNG
        n_workers = current_task.get("n_workers") or current_task.get("suggested_workers", 4)
        n_workers = int(n_workers)
        worker_source = "người dùng chọn" if current_task.get("n_workers") else "đề xuất"
        update_task(task_id, progress=15, message=f"Sử dụng {n_workers} luồng ({worker_source})")

        # CHUYỂN CONFIG
        raw_configs = current_task.get("selected_configs", [])
        if not raw_configs:
            raise Exception("Không có nhóm địa chỉ nào được chọn!")

        address_configs = []
        for cfg in raw_configs:
            address_configs.append((
                cfg.get("id_province"),
                cfg.get("id_district"),
                cfg.get("id_ward"),
                cfg.get("province"),
                cfg.get("district"),
                cfg.get("ward")
            ))

        update_task(task_id, progress=25, message=f"Đang xử lý {len(address_configs)} nhóm địa chỉ...")

        handler_func = get_handler(input_path.suffix)
        if not handler_func:
            raise Exception(f"Không hỗ trợ định dạng file: {input_path.suffix}")

        start_time = time.time()

        # XỬ LÝ SONG SONG
        with multiprocessing.Pool(processes=n_workers) as pool:
            result = handler_func(
                input_file=str_input_path,
                map_dict=mapping_table,
                address_configs=address_configs,
                pool=pool
            )

        elapsed = time.time() - start_time

        if result.get("success"):
            update_task(task_id, **{
                "status": "preview_ready",
                "progress": 100,
                "message": f"HOÀN THÀNH trong {elapsed:.1f}s!",
                "result": {
                    "total_rows": result["total_rows"],
                    "success_count": result["success_count"],
                    "fail_count": result["fail_count"],
                    "full_data": result["full_df"]
                }
            })
        else:
            raise Exception("Handler xử lý thất bại")

    except Exception as e:
        update_task(task_id, status="failed", message=f"Lỗi: {str(e)}", progress=0)