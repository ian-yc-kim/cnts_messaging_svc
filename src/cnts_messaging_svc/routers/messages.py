from fastapi import APIRouter, Body, Depends, HTTPException, status
from sqlalchemy.orm import Session
import logging
import asyncio

from cnts_messaging_svc.models.base import get_db
from cnts_messaging_svc.schemas.message import MessageCreate, MessageResponse
from cnts_messaging_svc.services.message_persistence import MessagePersistenceService, MessagePersistenceError
from cnts_messaging_svc.services.websocket_publisher import WebSocketPublisher
from cnts_messaging_svc.routers.websocket_router import manager

router = APIRouter()

# Instantiate module-level websocket publisher using global manager
websocket_publisher = WebSocketPublisher(manager)


@router.post("/messages", response_model=MessageResponse)
async def publish_message(
    message_data: MessageCreate = Body(...),
    db_session: Session = Depends(get_db)
):
    """
    Publish a new message to the messaging service.
    
    Args:
        message_data: The message data to persist
        db_session: Database session injected via dependency
        
    Returns:
        MessageResponse: The persisted message with auto-generated fields
        
    Raises:
        HTTPException: 500 for persistence errors, 422 for validation errors (handled by FastAPI)
    """
    try:
        print("[DEBUG] messages.publish_message: start", flush=True)
        # Instantiate the persistence service
        persistence_service = MessagePersistenceService()
        
        # Persist the message using the service
        result = persistence_service.persist_message(db_session=db_session, message_data=message_data)
        
        # Log successful publication
        logging.info(
            f"Successfully published message: topic_type={message_data.topic_type}, "
            f"topic_id={message_data.topic_id}, message_type={message_data.message_type}, "
            f"message_id={result.message_id}"
        )
        
        print("[DEBUG] messages.publish_message: persistence done, scheduling broadcast via WebSocketPublisher", flush=True)

        # Broadcast to websocket subscribers (best-effort) asynchronously to avoid deadlocks
        try:
            # Schedule broadcast without awaiting to prevent blocking the request/response cycle
            asyncio.create_task(websocket_publisher.publish_message(result))
            print("[DEBUG] messages.publish_message: broadcast task scheduled", flush=True)
        except Exception as e:
            logging.error(f"Failed to schedule broadcast to websockets: {e}", exc_info=True)
        
        print("[DEBUG] messages.publish_message: returning response", flush=True)
        # FastAPI will automatically convert the Message ORM object to MessageResponse
        return result
        
    except MessagePersistenceError as e:
        # Handle persistence-specific errors
        logging.error(f"Message persistence failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to persist message: {str(e)}"
        )
        
    except Exception as e:
        # Handle any unexpected errors
        logging.error(f"Unexpected error during message publishing: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while processing the message"
        )
