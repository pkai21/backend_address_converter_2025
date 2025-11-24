# config/settings.py
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

class Settings:
    MAPPING_FILE = BASE_DIR / "core" / "data" / "mapping.json"
    DOWNLOAD_DIR = BASE_DIR / "downloads"

    @staticmethod
    def get_output_filename(input_filename: str) -> str:
        """
        Tạo tên file output 
        Ví dụ: 
        - input: "danh_sach_khach_hang.xlsx" → output: "danh_sach_khach_hang_convert.xlsx"
        """
        name, ext = os.path.splitext(input_filename)
    
        new_name = f"{name}_convert{ext}"      
        return new_name
    
    @classmethod
    def _ensure_unique_path(cls, file_path: str) -> str:
        """
        Nếu file_path đã tồn tại, thêm (1), (2)... vào sau tên file cho đến khi duy nhất
        """
        base, ext = os.path.splitext(file_path)
        counter = 1
        new_path = file_path
        while os.path.exists(new_path):
            new_path = f"{base}({counter}){ext}"
            counter += 1
        return new_path
