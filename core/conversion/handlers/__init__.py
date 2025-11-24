# core/conversion/handlers/__init__.py
from .csv_handler import process_csv
from .json_handler import process_json
from .excel_handler import process_excel
from .sql_handler import process_sql

def get_handler(ext: str):
    """
    Trả về hàm handler đúng định dạng file
    """
    handlers = {
        '.csv': process_csv,
        '.json': process_json,
        '.xlsx': process_excel,
        '.xls': process_excel,
        '.sql': process_sql
    }
    return handlers.get(ext.lower())