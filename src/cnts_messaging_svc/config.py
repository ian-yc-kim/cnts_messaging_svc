import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///:memory:")
SERVICE_PORT = os.getenv("SERVICE_PORT", 8000)

# WebSocket inactivity configuration
# Default values chosen small to keep tests fast; override via env for production
WEBSOCKET_INACTIVITY_TIMEOUT_SECONDS = int(os.getenv("WEBSOCKET_INACTIVITY_TIMEOUT_SECONDS", "2"))
WEBSOCKET_INACTIVITY_CHECK_INTERVAL_SECONDS = int(os.getenv("WEBSOCKET_INACTIVITY_CHECK_INTERVAL_SECONDS", "1"))
