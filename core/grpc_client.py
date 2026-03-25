import grpc
import riva.client
from core.config import settings


class WhisperGRPCClient:
    """
    Persistent authenticated gRPC client (Singleton).
    Supports:
      - NVIDIA Whisper Cloud (Primary/Fallback)
      - Local Riva (Primary/Fallback)
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance

    # --------------------------------

    def _initialize(self):
        self.asr_local = None
        self.asr_cloud = None

        self._setup_local()
        self._setup_cloud()

    def _setup_local(self):
        if settings.riva_server:
            try:
                # Use standard Auth for Local
                auth = riva.client.Auth(
                    uri=settings.riva_server,
                    use_ssl=False
                )
                self.asr_local = riva.client.ASRService(auth)
            except Exception:
                pass # Silent init

    def _setup_cloud(self):
        if settings.whisper_server and settings.api_key:
            try:
                # Use metadata_args for Cloud (matching python-clients logic)
                metadata = [
                    ("authorization", f"Bearer {settings.api_key}"),
                    ("function-id", settings.function_id),
                ]

                auth = riva.client.Auth(
                    uri=settings.whisper_server,
                    use_ssl=True,
                    metadata_args=metadata
                )
                self.asr_cloud = riva.client.ASRService(auth)
            except Exception:
                pass # Silent init

    # --------------------------------

    def transcribe_bytes(self, audio_data: bytes, config: riva.client.RecognitionConfig):
        """
        Transcription with dynamic fallback.
        Silent on UNAVAILABLE if a fallback exists.
        """
        
        # 1. Try Local Riva
        if self.asr_local:
            try:
                return self.asr_local.offline_recognize(audio_data, config)
            except grpc.RpcError as e:
                # Fallback if local is down
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    if not self.asr_cloud:
                        raise e # No fallback available
                else:
                    raise e

        # 2. Try NVIDIA Cloud
        if self.asr_cloud:
            try:
                print(">>> Attempting NVIDIA Whisper Cloud...")
                return self.asr_cloud.offline_recognize(audio_data, config)
            except Exception as e:
                print(f"!!! NVIDIA Cloud Failed: {e}")
                raise e

        raise RuntimeError("No transcription backend available")

    def get_asr(self):
        """Legacy compatibility"""
        return self.asr_local or self.asr_cloud

    # --------------------------------
    # PUBLIC METHODS FOR TRANSCRIPTION SERVICE
    # --------------------------------

    def transcribe_bytes(self, audio_data: bytes, config) -> str:
        """
        Core transcription method using persistent gRPC connection.
        
        This is a synchronous method that:
        - Reuses authenticated persistent channels (no new connections per call)
        - Handles automatic fallback (local Riva ? NVIDIA Cloud)
        - Returns gRPC result object
        
        Args:
            audio_data: Raw audio bytes
            config: gRPC RecognitionConfig object
        
        Returns:
            gRPC recognition result object
        
        Used by WhisperTranscriber in async context via asyncio.to_thread()
        """
        # 1. Try Local Riva
        if self.asr_local:
            try:
                return self.asr_local.offline_recognize(audio_data, config)
            except grpc.RpcError as e:
                # Fallback if local is down
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    if not self.asr_cloud:
                        raise e  # No fallback available
                    print(">>> Local Riva unavailable, falling back to NVIDIA Cloud...")
                else:
                    raise e

        # 2. Try NVIDIA Cloud
        if self.asr_cloud:
            try:
                print(">>> Attempting NVIDIA Whisper Cloud...")
                return self.asr_cloud.offline_recognize(audio_data, config)
            except Exception as e:
                print(f"!!! NVIDIA Cloud Failed: {e}")
                raise e

        raise RuntimeError("No transcription backend available")

    def create_recognition_config(self, language_code: str = "en-US"):
        """
        Create gRPC recognition configuration.
        
        Args:
            language_code: Language code (e.g., 'en-US', 'es-US')
        
        Returns:
            riva.client.RecognitionConfig object
        
        Note: This matches the API used in python-clients/scripts/asr/transcribe_file_offline.py
        """
        return riva.client.RecognitionConfig(
            language_code=language_code,
            max_alternatives=1,
        )

    def extract_transcript_from_result(self, result) -> str:
        """
        Extract transcript text from gRPC recognition result.
        
        Args:
            result: gRPC recognition result object from offline_recognize()
        
        Returns:
            Transcript string
        
        Raises:
            RuntimeError: If result is invalid or empty
        """
        if not result or not result.results:
            raise RuntimeError("No recognition results returned from service")
        
        # Get first (and usually only) result
        recognition_result = result.results[0]
        
        if not recognition_result.alternatives:
            raise RuntimeError("No alternatives in recognition result")
        
        # Get transcript from best alternative
        transcript = recognition_result.alternatives[0].transcript
        
        if not transcript or not transcript.strip():
            raise RuntimeError("Empty transcript returned from service")
        
        return transcript.strip()
    