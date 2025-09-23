from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import HTTPException as FastAPIHTTPException
from fastapi.responses import JSONResponse
from starlette.requests import Request
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette import status
import logging

from cnts_messaging_svc.routers.messages import router as messages_router
from cnts_messaging_svc.schemas.error import ErrorResponse

logger = logging.getLogger(__name__)

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

# Global exception handlers
@app.exception_handler(FastAPIHTTPException)
async def fastapi_http_exception_handler(request: Request, exc: FastAPIHTTPException):
    """Handle FastAPI HTTPExceptions with standardized error response."""
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(
            status_code=exc.status_code,
            code=f"HTTP_{exc.status_code}",
            message=exc.detail,
            details=None
        ).model_dump()
    )

@app.exception_handler(StarletteHTTPException)
async def starlette_http_exception_handler(request: Request, exc: StarletteHTTPException):
    """Handle Starlette HTTPExceptions (404, 405, etc.) with standardized error response."""
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(
            status_code=exc.status_code,
            code=f"HTTP_{exc.status_code}",
            message=exc.detail,
            details=None
        ).model_dump()
    )

@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    """Handle unhandled exceptions with standardized error response."""
    # Log the unhandled exception
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content=ErrorResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            code="INTERNAL_SERVER_ERROR",
            message="An unexpected internal server error occurred.",
            details=str(exc) if app.debug else None
        ).model_dump()
    )

# Include message router with API prefix
app.include_router(messages_router, prefix="/api/v1")
