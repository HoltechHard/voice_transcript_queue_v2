import asyncio

from core.config import Settings
from queue_manager.redis_queue import RedisQueue
from core.grpc_client import WhisperGRPCClient
from core.storage import TranscriptStorage
from core.transcription_service import WhisperTranscriber
from workers.async_worker import AsyncWorker


class Application:
    """
    Main application orchestrator (OOP entrypoint)
    
    Architecture:
    ?????????????????????????????????????????
    Clients (HTTP/App/Users)
             ?
        Redis Job Queue
             ?
       Async Worker Pool
             ?
    WhisperTranscriber (orchestrates operations)
             ?
    WhisperGRPCClient (manages persistent connections) [Singleton]
             ?
      NVIDIA Whisper (Riva gRPC)
             ?
       transcripts.json
    
    Separation of Concerns:
    - WhisperGRPCClient: Low-level gRPC connection management
    - WhisperTranscriber: High-level transcription operation orchestration
    - AsyncWorker: Job consumer (unaware of gRPC details)
    """

    def __init__(self):
        self.settings = Settings()

        # Infrastructure layers
        self.queue = RedisQueue()
        
        # Persistent gRPC client singleton (connection pooling)
        self.grpc_client = WhisperGRPCClient()
        
        # Transcription service (holds gRPC client, orchestrates operations)
        self.transcriber = WhisperTranscriber(grpc_client=self.grpc_client)
        
        # Storage
        self.storage = TranscriptStorage()

        # Workers pool - each uses same transcriber instance (which holds same gRPC client)
        self.workers = [
            AsyncWorker(
                self.queue,
                self.transcriber,  # Workers get transcription service
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

