# core/models.py
from sqlalchemy import Column, ForeignKey, String, Integer, BigInteger, Text, DateTime, UniqueConstraint
from sqlalchemy.sql import func
from core.database import engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import JSONB

Base = declarative_base()

class Task(Base):
    __tablename__ = "tasks"

    task_id = Column(String, primary_key=True)
    filename = Column(String, nullable=False)
    filesize = Column(BigInteger)
    status = Column(String, default="pending")
    progress = Column(Integer, default=0)
    message = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    suggested_workers = Column(Integer)
    n_workers = Column(Integer)

    pending_groups = Column(JSONB, default=list)
    selected_groups = Column(JSONB, default=list)
    columns = Column(JSONB, default=list)
    step = Column(Integer, default=0)
    result = Column(JSONB, nullable=True)

class TaskEdit(Base):
    __tablename__ = "task_edits"

    id = Column(Integer, primary_key=True)
    task_id = Column(String, ForeignKey("tasks.task_id", ondelete="CASCADE"), nullable=False, index=True)
    row_index = Column(Integer, nullable=False)
    
    original_row = Column(JSONB)                    
    edited_row = Column(JSONB, nullable=False)      
    
    edited_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (UniqueConstraint('task_id', 'row_index', name='uix_task_row'),)

Base.metadata.create_all(bind=engine)