from sqlalchemy import Column, String, BigInteger, Text, TIMESTAMP, PrimaryKeyConstraint, func
from .base import Base


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
