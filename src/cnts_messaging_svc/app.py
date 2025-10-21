from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import HTTPException as FastAPIHTTPException
from fastapi.responses import JSONResponse
from starlette.requests import Request
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette import status
import logging
import asyncio
from datetime import datetime

from cnts_messaging_svc.routers.messages import router as messages_router
from cnts_messaging_svc.routers.websocket_router import websocket_router, manager
from cnts_messaging_svc.schemas.error import ErrorResponse
from cnts_messaging_svc import config

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

# Include routers with API prefix
app.include_router(messages_router, prefix="/api/v1")
app.include_router(websocket_router, prefix="/api/v1")

# Background cleanup task reference
app.state.ws_cleanup_task = None


async def stale_connection_cleanup_task():
    """Background task that periodically checks for stale websocket connections and cleans them up."""
    try:
        while True:
            # Snapshot current time and connections to avoid mutation during iteration
            now = datetime.utcnow()
            try:
                items = list(manager.active_connections.items())
            except Exception as e:
                logger.error(f"Failed to snapshot active connections: {e}", exc_info=True)
                items = []

            for client_id, conn_tuple in items:
                try:
                    # conn_tuple expected to be (websocket, last_activity_at)
                    if not isinstance(conn_tuple, tuple) or len(conn_tuple) != 2:
                        logger.warning(f"Unexpected connection tuple for client {client_id}: {conn_tuple}")
                        continue

                    websocket, last_activity_at = conn_tuple
                    try:
                        inactivity_seconds = (now - last_activity_at).total_seconds()
                    except Exception as e:
                        logger.error(f"Failed to compute inactivity for client {client_id}: {e}", exc_info=True)
                        continue

                    if inactivity_seconds > config.WEBSOCKET_INACTIVITY_TIMEOUT_SECONDS:
                        logger.warning(
                            f"Closing stale websocket for client {client_id} due to inactivity ({inactivity_seconds}s)"
                        )
                        try:
                            await websocket.close(code=1000, reason="Inactivity timeout")
                        except Exception as e:
                            logger.error(f"Error closing websocket for client {client_id}: {e}", exc_info=True)

                        try:
                            manager.disconnect(client_id)
                        except Exception as e:
                            logger.error(f"Failed to disconnect stale client {client_id}: {e}", exc_info=True)

                except Exception as e:
                    logger.error(f"Error checking client {client_id} for staleness: {e}", exc_info=True)

            # Sleep before next iteration
            await asyncio.sleep(config.WEBSOCKET_INACTIVITY_CHECK_INTERVAL_SECONDS)

    except asyncio.CancelledError:
        # Task cancellation requested, exit gracefully
        logger.info("stale_connection_cleanup_task cancelled")
    except Exception as e:
        logger.error(f"Unhandled error in stale_connection_cleanup_task: {e}", exc_info=True)
    finally:
        logger.info("stale_connection_cleanup_task exiting")


@app.on_event("startup")
async def startup_event():
    try:
        # Start the background cleanup task
        if app.state.ws_cleanup_task is None:
            app.state.ws_cleanup_task = asyncio.create_task(stale_connection_cleanup_task())
            logger.info("Started stale websocket cleanup task")
    except Exception as e:
        logger.error(f"Failed to start stale websocket cleanup task: {e}", exc_info=True)


@app.on_event("shutdown")
async def shutdown_event():
    try:
        task = app.state.ws_cleanup_task
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                logger.info("stale websocket cleanup task cancelled on shutdown")
            app.state.ws_cleanup_task = None
    except Exception as e:
        logger.error(f"Error during shutdown cleanup: {e}", exc_info=True)
