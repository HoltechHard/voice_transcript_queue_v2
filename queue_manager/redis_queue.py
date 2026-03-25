import redis.asyncio as redis
import json
import uuid
import asyncio
from core.config import settings


class RedisQueue:

    def __init__(self):

        self.redis = redis.Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            db=settings.redis_db,
            username=settings.redis_user,
            password=settings.redis_password,
            decode_responses=True
        )

        self.queue = settings.redis_queue

    # -----------------------------

    async def push(self, audio_path: str):

        job = {
            "id": str(uuid.uuid4()),
            "audio_path": audio_path,
        }

        await self.redis.rpush(self.queue, json.dumps(job))
        return job["id"]

    # -----------------------------

    async def pop(self):
        """
        Async pop using BLPOP
        """
        try:
            item = await self.redis.blpop(self.queue, timeout=5)

            if item:
                # item is (key, value)
                return json.loads(item[1])
        except Exception as e:
            print(f"Queue Pop Error: {e}")

        return None
    