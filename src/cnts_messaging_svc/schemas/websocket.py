from typing import Literal
from pydantic import BaseModel, Field
from cnts_messaging_svc.schemas.message import MessageResponse


class WebSocketMessage(BaseModel):
    """Base model for all WebSocket messages."""
    type: str = Field(..., min_length=1, description="Message type identifier")


class SubscribeRequest(WebSocketMessage):
    """WebSocket message for subscribing to a topic."""
    type: Literal["subscribe"] = "subscribe"
    topic_type: str = Field(..., min_length=1, max_length=255, description="Type of topic to subscribe to")
    topic_id: str = Field(..., min_length=1, max_length=255, description="ID of topic to subscribe to")


class UnsubscribeRequest(WebSocketMessage):
    """WebSocket message for unsubscribing from a topic."""
    type: Literal["unsubscribe"] = "unsubscribe"
    topic_type: str = Field(..., min_length=1, max_length=255, description="Type of topic to unsubscribe from")
    topic_id: str = Field(..., min_length=1, max_length=255, description="ID of topic to unsubscribe from")


class Acknowledgement(WebSocketMessage):
    """WebSocket message for acknowledging requests."""
    type: Literal["ack"] = "ack"
    request_id: str = Field(..., min_length=1, max_length=255, description="ID of the request being acknowledged")
    status: Literal["success", "error"] = Field(..., description="Status of the acknowledged request")


class ErrorMessage(WebSocketMessage):
    """WebSocket message for error notifications."""
    type: Literal["error"] = "error"
    error: str = Field(..., min_length=1, description="Error message description")


class MessageDelivery(WebSocketMessage):
    """WebSocket message for delivering published messages to subscribers."""
    type: Literal["message"] = "message"
    message: MessageResponse = Field(..., description="The message being delivered")
