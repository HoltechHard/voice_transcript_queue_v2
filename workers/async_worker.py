# workers/async_worker.py

import asyncio
import sys


class AsyncWorker:
    """
    Worker that consumes transcription jobs from Redis queue.
    
    Responsibilities:
    - Pop jobs from Redis queue
    - Delegate transcription to WhisperTranscriber
    - Persist results to storage
    
    Decoupled from gRPC implementation details.
    WhisperTranscriber handles all transcription orchestration.
    """

    def __init__(self, queue, transcriber, storage, worker_id=0):
        """
        Args:
            queue: RedisQueue instance
            transcriber: WhisperTranscriber instance (holds persistent gRPC client)
            storage: TranscriptStorage instance
            worker_id: Worker identifier
        """
        self.queue = queue
        self.transcriber = transcriber
        self.storage = storage
        self.worker_id = worker_id

    async def run(self):
        print(
            f"[Worker-{self.worker_id}] Started and waiting for jobs "
            f"(using persistent gRPC client via transcriber)..."
        )

        while True:
            try:
                # 1. Pop from Redis (BLPOP with timeout)
                job = await self.queue.pop()

                if not job:
                    # Small sleep if blpop timeout occurs
                    await asyncio.sleep(0.1)
                    continue

                job_id = job.get("id")
                audio_path = job.get("audio_path")

                print(f"[Worker-{self.worker_id}] [{job_id}] Processing: {audio_path}")

                try:
                    # 2. Execute transcription via WhisperTranscriber
                    # WhisperTranscriber holds the persistent gRPC client instance
                    transcript = await self.transcriber.transcribe(audio_path)

                    # 3. Persist result
                    self.storage.save(job_id, transcript)

                    # Handle encoding safely for Windows console
                    encoding = sys.stdout.encoding or 'utf-8'
                    safe_transcript = transcript.encode(encoding, errors='replace').decode(encoding)
                    
                    print(f"[Worker-{self.worker_id}] [{job_id}] DONE") 
                    print(f"[Worker-{self.worker_id}] Transcript: {safe_transcript}")

                except Exception as e:
                    print(f"[Worker-{self.worker_id}] [{job_id}] Transcription Failed: {e}")

            except Exception as e:
                print(
                    f"[Worker-{self.worker_id}] Critical Error (Queue/Connection): {e}"
                )
                await asyncio.sleep(5)

