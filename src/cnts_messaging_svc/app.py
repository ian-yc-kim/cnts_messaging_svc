from fastapi import FastAPI
from cnts_messaging_svc.routers.messages import router as messages_router

app = FastAPI(debug=True)

# Include message router with API prefix
app.include_router(messages_router, prefix="/api")
