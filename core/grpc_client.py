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
    