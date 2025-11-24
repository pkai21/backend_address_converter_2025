# core/database.py
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError, ProgrammingError
from dotenv import load_dotenv

load_dotenv()

# URL mặc định cho local dev
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL không được set! Vui lòng cấu hình trên Render.")

# Tạo engine chính
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=3600,
    echo=False  
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# ==========================================
# TỰ ĐỘNG TẠO DB + TABLE + INDEX KHI CHẠY LẦN ĐẦU
# ==========================================
def init_db():
    db_name = DATABASE_URL.split("/")[-1]
    # Engine kết nối đến database "postgres" để tạo DB mới nếu cần
    sys_url = DATABASE_URL.rsplit("/", 1)[0] + "/postgres"
    sys_engine = create_engine(sys_url, isolation_level="AUTOCOMMIT")

    # 1. Tạo database nếu chưa tồn tại
    try:
        with sys_engine.connect() as conn:
            conn.execute(text(f"CREATE DATABASE {db_name} ENCODING 'UTF8' LC_COLLATE 'en_US.UTF-8' LC_CTYPE 'en_US.UTF-8' TEMPLATE template0"))
            print(f"✅ Đã tự động tạo database: {db_name}")
    except ProgrammingError:
        # Đã tồn tại rồi → bình thường
        pass

    # 2. Tạo table + index nếu chưa có (dùng engine chính)
    with SessionLocal() as db:
        # Table chính lưu thông tin task
        db.execute(text("""
            CREATE TABLE IF NOT EXISTS tasks (
                task_id TEXT PRIMARY KEY,
                filename TEXT NOT NULL,
                filesize BIGINT,
                status TEXT DEFAULT 'pending',
                progress INTEGER DEFAULT 0,
                message TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW(),
                
                suggested_workers INTEGER,
                n_workers INTEGER,
                
                pending_configs JSONB DEFAULT '[]',
                selected_configs JSONB DEFAULT '[]',
                
                result JSONB,
                
                UNIQUE(task_id)
            );
        """))

        # Thêm cột full_data_blob nếu chưa có
        db.execute(text("ALTER TABLE tasks ADD COLUMN IF NOT EXISTS full_data_blob BYTEA;"))

        # Index nhanh
        db.execute(text("CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);"))
        db.execute(text("CREATE INDEX IF NOT EXISTS idx_tasks_created_at ON tasks(created_at DESC);"))
        db.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_result_full_data_status 
            ON tasks USING GIN ((result->'full_data') jsonb_path_ops);
        """))
            
        db.commit()
    
        # Table lưu chỉnh sửa từng dòng bởi người dùng
        db.execute(text("""
            CREATE TABLE IF NOT EXISTS task_edits (
                id SERIAL PRIMARY KEY,
                task_id TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
                row_index INTEGER NOT NULL,
                original_row JSONB,
                edited_row JSONB NOT NULL,
                edited_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(task_id, row_index)
            );
        """))
        
        db.execute(text("CREATE INDEX IF NOT EXISTS idx_task_edits_task_id ON task_edits(task_id);"))
        db.commit()
        
init_db()