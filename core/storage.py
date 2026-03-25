# core/storage.py

import json
from pathlib import Path
from threading import Lock


class TranscriptStorage:
    """
    Persistent storage for transcription results.

    Stores transcripts inside:
        data/transcripts.json

    Thread-safe for multi-worker usage.
    """

    def __init__(self, filepath: str = "data/transcripts.json"):
        self.file = Path(filepath)
        self.lock = Lock()

        # ensure directory exists
        self.file.parent.mkdir(parents=True, exist_ok=True)

        # initialize file if missing
        if not self.file.exists():
            self.file.write_text(json.dumps({}, indent=2))
    

    def _read(self) -> dict:
        try:
            return json.loads(self.file.read_text())
        except Exception:
            return {}


    def _write(self, data: dict):
        self.file.write_text(json.dumps(data, indent=2))


    def save(self, job_id: str, text: str):
        """
        Save transcript result.
        Safe for concurrent workers.
        """

        with self.lock:
            data = self._read()
            data[job_id] = text
            self._write(data)


    def get(self, job_id: str) -> str | None:
        """Retrieve stored transcript."""
        data = self._read()
        return data.get(job_id)


    def all(self) -> dict:
        """Return all transcripts."""
        return self._read()
    