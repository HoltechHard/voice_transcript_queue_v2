import asyncio

from core.config import Settings
from queue_manager.redis_queue import RedisQueue
from core.grpc_client import WhisperGRPCClient
from core.storage import TranscriptStorage
from workers.async_worker import AsyncWorker
from core.transcription_service import WhisperTranscriber


class Application:
    """
    Main application orchestrator (OOP entrypoint)
    """

    def __init__(self):
        self.settings = Settings()

        # Infrastructure
        self.queue = RedisQueue()
        self.grpc_client = WhisperGRPCClient()
        self.storage = TranscriptStorage()

        # Services
        self.transcriber = WhisperTranscriber()

        # Workers pool
        self.workers = [
            AsyncWorker(
                self.queue,
                self.transcriber,
                self.storage,
                worker_id=i
            )
            for i in range(self.settings.max_workers)
        ]


    async def start_workers(self):
        print(f"Starting {len(self.workers)} workers...")

        for worker in self.workers:
            asyncio.create_task(worker.run())

    async def simulate_jobs(self):
        """
        Push jobs to queue for workers to pick up
        """
        print("Simulating jobs (pushing to queue)...")
        for i in range(10):
            job_id = await self.queue.push("voice/test.ogg")
            print(f"Queued job {i+1}: {job_id}")

    async def run(self):
        await self.start_workers()

        # simulate producers
        await self.simulate_jobs()

        # keep app alive
        print("System is running. Press Ctrl+C to stop.")
        while True:
            await asyncio.sleep(1)


async def main():
    app = Application()
    await app.run()


if __name__ == "__main__":
    asyncio.run(main())

