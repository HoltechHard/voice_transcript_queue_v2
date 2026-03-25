import asyncio
from pathlib import Path
from core.grpc_client import WhisperGRPCClient
from core.config import settings


class WhisperTranscriber:
    """
    High-level transcription service orchestrator.
    
    Responsibilities:
    - Holds instance of persistent gRPC client (singleton)
    - Orchestrates transcription operations (file reading, gRPC calls, result handling)
    - Provides async/await interface for workers
    - Uses language code from configuration
    
    Architecture:
    - WhisperGRPCClient: Low-level gRPC connection management
    - WhisperTranscriber: High-level transcription operation orchestration
    """

    def __init__(self, grpc_client: WhisperGRPCClient = None):
        """
        Initialize transcription service.
        
        Args:
            grpc_client: WhisperGRPCClient singleton instance.
                        If None, creates/gets singleton instance.
        """
        self.grpc_client = grpc_client or WhisperGRPCClient()
        self.default_language_code = settings.language

    async def transcribe(self, audio_path: str, language_code: str = None) -> str:
        """
        Async transcription operation using persistent gRPC client.
        
        This is the main entry point for workers.
        No subprocess overhead, no process spawning.
        Reuses persistent authenticated gRPC channels.
        
        Args:
            audio_path: Path to audio file
            language_code: Language code (e.g., 'en-US', 'es-US'). 
                          If None, uses LANGUAGE_CODE from .env
        
        Returns:
            Transcript text
        
        Raises:
            FileNotFoundError: If audio file doesn't exist
            RuntimeError: If transcription fails or no service available
        """
        # Use configured language if not specified
        lang_code = language_code or self.default_language_code
        
        # 1. Read audio file (blocking I/O in thread pool)
        audio_data = await asyncio.to_thread(self._read_audio_file, audio_path)
        
        # 2. Execute transcription via persistent gRPC client (blocking call in thread pool)
        result = await asyncio.to_thread(
            self.grpc_client.transcribe_bytes,
            audio_data,
            self.grpc_client.create_recognition_config(lang_code)
        )
        
        # 3. Extract and return transcript
        return self.grpc_client.extract_transcript_from_result(result)

    def _read_audio_file(self, audio_path: str) -> bytes:
        """
        Read audio file into bytes.
        
        (Blocking operation - runs in thread executor when called via asyncio.to_thread)
        """
        path = Path(audio_path)
        if not path.exists():
            raise FileNotFoundError(f"Audio file not found: {audio_path}")
        
        with open(path, 'rb') as f:
            return f.read()
    