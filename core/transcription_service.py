import subprocess
import asyncio
import os
import re
import sys
from pathlib import Path

from core.config import settings


class WhisperTranscriber:
    """
    Adapter that executes the python-clients script via subprocess
    to ensure perfect integration while removing .bat dependency.
    """

    TRANSCRIPT_PATTERN = r"Final transcript:\s*(.+)"

    def __init__(self):
        # The user wants "pure python and GRPC persistent client with singleton"
        # We handle this as a singleton service in the app.
        pass

    async def transcribe(self, audio_path: str):
        """
        Async entry point for the worker
        """
        return await asyncio.to_thread(self._sync_transcribe, audio_path)

    def _sync_transcribe(self, audio_path: str):
        
        # 1. Prepare environment (as seen in user's example)
        env = os.environ.copy()
        
        script_path = settings.script_path
        if not script_path:
            raise RuntimeError("SCRIPT_PATH not defined in .env")

        # 2. Build command (mimicking run_whisper.bat)
        # Using sys.executable to ensure we use the same virtual environment
        cmd = [
            sys.executable,
            script_path,
            "--server", settings.whisper_server,
            "--use-ssl",
            "--metadata", "function-id", settings.function_id,
            "--metadata", "authorization", f"Bearer {settings.api_key}",
            "--language-code", settings.language,
            "--input-file", audio_path
        ]

        print(f">>> Executing: {' '.join(cmd)}")

        # 3. Running subprocess
        process = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace", # Use replace to avoid losing data on encoding issues
            env=env
        )


        # 4. Extracting transcript
        combined_output = (process.stdout or "") + "\n" + (process.stderr or "")

        if process.returncode != 0:
            print(f"!!! Script Error output: {combined_output}")
            raise RuntimeError(f"Script failed with exit code {process.returncode}")

        return self._extract_transcript(combined_output)

    def _extract_transcript(self, output: str):
        # normalize windows line endings
        output = output.replace("\r", "")
        
        # Find all matches (mimicking r.alternatives[0].transcript logic)
        # Taking the last occurrence or joining them depends on script output.
        # But usually offline_recognize returns one "Final transcript" line.
        match = re.search(self.TRANSCRIPT_PATTERN, output)

        if not match:
            # Maybe the local Riva failed silently or returned nothing
            if "ipv4:127.0.0.1:50051" in output and "connection refused" in output.lower():
                 print("!!! Local Riva UNAVAILABLE in script output. Script should handle fallback.")
            
            raise RuntimeError(
                "Transcript not found in script output.\n---- RAW OUTPUT ----\n"
                + output
            )

        return match.group(1).strip()
    