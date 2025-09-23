from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from cnts_messaging_svc.routers.messages import router as messages_router

app = FastAPI(
    title="Messaging Service API",
    description="REST and WebSocket API for real-time messaging and persistence.",
    version="1.0.0",
    debug=True  # Keep for now as it's already there
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for development, restrict in production
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods
    allow_headers=["*"],  # Allow all headers
)

# Include message router with API prefix
app.include_router(messages_router, prefix="/api/v1")
