from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime


class MessageCreate(BaseModel):
    """Pydantic model for validating incoming message data."""
    topic_type: str = Field(min_length=1, max_length=255)
    topic_id: str = Field(min_length=1, max_length=255)
    message_type: str = Field(min_length=1, max_length=255)
    sender_type: str = Field(min_length=1, max_length=255)
    sender_id: str = Field(min_length=1, max_length=255)
    content_type: str = Field(min_length=1, max_length=255)
    content: str = Field(min_length=1)


class MessageResponse(BaseModel):
    """Pydantic model for API responses from Message SQLAlchemy model."""
    topic_type: str
    topic_id: str
    message_type: str
    message_id: int
    sender_type: str
    sender_id: str
    content_type: str
    content: str
    created_at: datetime
    
    model_config = ConfigDict(from_attributes=True)
