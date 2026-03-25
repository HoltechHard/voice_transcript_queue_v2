# workers/async_worker.py

import asyncio


class AsyncWorker:
    """
    Worker that consumes transcription jobs from Redis
    and uses WhisperTranscriber.
    """

    def __init__(self, queue, transcriber, storage, worker_id=0):
        self.queue = queue
        self.transcriber = transcriber
        self.storage = storage
        self.worker_id = worker_id

    async def run(self):
        print(
            f"[Worker-{self.worker_id}] Started and waiting for jobs on "
            f"{self.queue.queue}..."
        )

        while True:
            try:
                # 1. Pop from Redis (this uses BLPOP, which is blocking)
                job = await self.queue.pop()

                if not job:
                    # Small sleep if blpop timeout occurs (timeout=5 in RedisQueue)
                    await asyncio.sleep(0.1)
                    continue

                job_id = job.get("id")
                audio_path = job.get("audio_path")

                print(f"[Worker-{self.worker_id}] [{job_id}] Processing: {audio_path}")

                try:
                    # 2. Execute Transcription via WhisperTranscriber (subprocess)
                    transcript = await self.transcriber.transcribe(audio_path)

                    # 3. Persist result
                    self.storage.save(job_id, transcript)

                    # Use encode/decode with replace to avoid charmap errors on Windows console
                    import sys
                    encoding = sys.stdout.encoding or 'utf-8'
                    safe_transcript = transcript.encode(encoding, errors='replace').decode(encoding)
                    
                    print(f"[Worker-{self.worker_id}] [{job_id}] DONE") 
                    print(f"[Worker-{self.worker_id}] Transcription: {safe_transcript}")

                except Exception as proc_error:
                    print(
                        f"[Worker-{self.worker_id}] [{job_id}] Processing Failed: {proc_error}"
                    )

            except Exception as e:
                print(
                    f"[Worker-{self.worker_id}] Critical Error (Queue/Connection): {e}"
                )
                await asyncio.sleep(5)

