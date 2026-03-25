from dotenv import load_dotenv
import os


class Settings:

    def __init__(self):
        load_dotenv()

        self.api_key = os.getenv("NVIDIA_API_KEY")
        self.function_id = os.getenv("WHISPER_FUNCTION_ID")
        self.whisper_server = os.getenv("WHISPER_SERVER")
        self.language = os.getenv("LANGUAGE_CODE")

        self.redis_host = os.getenv("REDIS_HOST")        
        self.redis_port = int(os.getenv("REDIS_PORT"))
        self.redis_db = int(os.getenv("REDIS_DB"))
        self.redis_user = os.getenv("REDIS_USERNAME")
        self.redis_password = os.getenv("REDIS_PASSWORD")
        self.redis_queue = os.getenv("REDIS_QUEUE")

        self.max_workers = int(os.getenv("MAX_WORKERS"))
        self.riva_server = os.getenv("RIVA_LOCAL_URI")
        self.script_path = os.getenv("SCRIPT_PATH")

settings = Settings()
