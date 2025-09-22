import logging
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from cnts_messaging_svc.models.message import Message
from cnts_messaging_svc.schemas.message import MessageCreate


class MessagePersistenceError(Exception):
    """Custom exception for message persistence failures."""
    pass


class MessagePersistenceService:
    """Service class responsible for persisting messages to the database."""
    
    def persist_message(self, db_session: Session, message_data: MessageCreate) -> Message:
        """
        Persist a message to the database with transaction management.
        
        Args:
            db_session: SQLAlchemy database session
            message_data: Validated message data from MessageCreate schema
            
        Returns:
            Message: The persisted Message ORM object with auto-generated fields
            
        Raises:
            MessagePersistenceError: If database persistence fails
        """
        try:
            # Create Message ORM object from MessageCreate data
            message_orm_obj = Message(
                topic_type=message_data.topic_type,
                topic_id=message_data.topic_id,
                message_type=message_data.message_type,
                sender_type=message_data.sender_type,
                sender_id=message_data.sender_id,
                content_type=message_data.content_type,
                content=message_data.content
                # message_id and created_at are handled by ORM/DB defaults
            )
            
            # Add to session and commit
            db_session.add(message_orm_obj)
            db_session.commit()
            
            # Refresh to ensure auto-generated fields are loaded
            db_session.refresh(message_orm_obj)
            
            # Log successful persistence
            logging.info(
                f"Successfully persisted message: topic_type={message_data.topic_type}, "
                f"topic_id={message_data.topic_id}, message_type={message_data.message_type}, "
                f"message_id={message_orm_obj.message_id}"
            )
            
            return message_orm_obj
            
        except IntegrityError as e:
            # Handle database constraint violations
            db_session.rollback()
            logging.error(f"Database integrity error during message persistence: {e}", exc_info=True)
            raise MessagePersistenceError(f"Failed to persist message due to database constraint violation: {e}")
        
        except Exception as e:
            # Handle any other database-related errors
            db_session.rollback()
            logging.error(f"Unexpected error during message persistence: {e}", exc_info=True)
            raise MessagePersistenceError(f"Failed to persist message due to unexpected error: {e}")
