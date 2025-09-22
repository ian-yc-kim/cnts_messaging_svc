from fastapi import APIRouter, Body, Depends, HTTPException, status
from sqlalchemy.orm import Session
import logging

from cnts_messaging_svc.models.base import get_db
from cnts_messaging_svc.schemas.message import MessageCreate, MessageResponse
from cnts_messaging_svc.services.message_persistence import MessagePersistenceService, MessagePersistenceError

router = APIRouter()


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
