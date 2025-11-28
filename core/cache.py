# core/cache.py – phiên bản production
from aiocache import Cache
from aiocache.serializers import JsonSerializer

cache = Cache(
    Cache.REDIS,
    endpoint="redis",       
    port=6379,
    db=0,
    password="your_strong_password", 
    serializer=JsonSerializer(),
    namespace="grok_convert"
)

async def set_sample_preview(task_id: str, data):
    await cache.set(f"preview:{task_id}", data, ttl=3600)  

async def get_sample_preview(task_id: str):
    return await cache.get(f"preview:{task_id}")