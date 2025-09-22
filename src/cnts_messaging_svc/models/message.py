from sqlalchemy import Column, String, BigInteger, Text, TIMESTAMP, PrimaryKeyConstraint, func, event, text
from .base import Base
import logging


class Message(Base):
    __tablename__ = 'messages'

    topic_type = Column(String(255), nullable=False)
    topic_id = Column(String(255), nullable=False)
    message_type = Column(String(255), nullable=False)
    message_id = Column(BigInteger, nullable=False)
    sender_type = Column(String(255), nullable=False)
    sender_id = Column(String(255), nullable=False)
    content_type = Column(String(255), nullable=False)
    content = Column(Text, nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (
        PrimaryKeyConstraint('topic_type', 'topic_id', 'message_type', 'message_id', name='pk_messages'),
    )

    def __repr__(self):
        return f"<Message(topic_type='{self.topic_type}', topic_id='{self.topic_id}', message_type='{self.message_type}', message_id={self.message_id})>"


def generate_message_id_listener(mapper, connection, target):
    """Auto-generate message_id if not provided for SQLite and development environments.
    
    This event listener executes before insert operations on the Message model.
    If message_id is None, it queries the database to find the next sequential
    message_id within the scope of (topic_type, topic_id, message_type).
    
    Args:
        mapper: SQLAlchemy mapper for the Message model
        connection: Database connection being used
        target: The Message instance being inserted
    """
    if target.message_id is None:
        try:
            # Query for the maximum message_id in the current scope
            result = connection.execute(
                text("""
                SELECT COALESCE(MAX(message_id), 0) + 1 
                FROM messages 
                WHERE topic_type = :topic_type 
                AND topic_id = :topic_id 
                AND message_type = :message_type
                """),
                {
                    "topic_type": target.topic_type,
                    "topic_id": target.topic_id,
                    "message_type": target.message_type
                }
            )
            target.message_id = result.scalar()
        except Exception as e:
            logging.error(f"Failed to generate message_id: {e}", exc_info=True)
            raise


# Register the event listener
event.listen(Message, 'before_insert', generate_message_id_listener)
