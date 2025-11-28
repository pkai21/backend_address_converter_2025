from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import file_router
import uvicorn

app = FastAPI(
    title="Chuyển đổi địa chỉ hành chính Việt Nam 2025",
    description="Backend API - Cập nhật 01/07/2025",
    version="2.0.0"
)

# Cho phép Next.js gọi (CORS)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(file_router.router)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)