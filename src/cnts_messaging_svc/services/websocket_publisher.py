import logging
from typing import List
from fastapi import WebSocket, WebSocketDisconnect

from cnts_messaging_svc.schemas.websocket import MessageDelivery
from cnts_messaging_svc.schemas.message import MessageResponse
from cnts_messaging_svc.connection_manager import WebSocketConnectionManager

logger = logging.getLogger(__name__)


class WebSocketPublisher:
    """Publish persisted messages to all active websocket subscribers for a topic.

    This is a best-effort broadcaster. Individual client failures do not stop delivery
    to other subscribers.
    """

    def __init__(self, manager: WebSocketConnectionManager):
        self._manager = manager

    async def publish_message(self, message: MessageResponse) -> None:
        """Publish a message to all subscribers of its topic.

        Args:
            message: MessageResponse instance

        Notes:
            - Any WebSocketDisconnect from a client is logged and ignored.
            - Any other exception during send is logged with exc_info=True and ignored.
        """
        try:
            # Ensure we have a MessageResponse instance for serialization
            if isinstance(message, MessageResponse):
                msg_response = message
            else:
                # Use pydantic model validation from attributes to build a MessageResponse
                try:
                    msg_response = MessageResponse.model_validate(message)
                except Exception as e:
                    logger.error(f"Failed to model_validate message for delivery: {e}", exc_info=True)
                    return

            topic_type = msg_response.topic_type
            topic_id = msg_response.topic_id

            # Retrieve active subscriber WebSocket objects
            subscribers: List[WebSocket] = []
            try:
                subscribers = self._manager.get_subscribers(topic_type, topic_id)
            except Exception as e:
                logger.error(f"Failed to get subscribers for topic {topic_type}:{topic_id}: {e}", exc_info=True)
                return

            # Send MessageDelivery to each subscriber
            for websocket in subscribers:
                try:
                    message_delivery = MessageDelivery(message=msg_response)
                    # Use json-mode dump to ensure datetimes are serialized
                    payload = message_delivery.model_dump(mode="json")
                    await websocket.send_json(payload)
                except WebSocketDisconnect:
                    # Log and continue to next subscriber
                    try:
                        client_id = self._manager.get_client_id(websocket)
                    except Exception:
                        client_id = None
                    logger.info(
                        f"WebSocketDisconnect when sending to client {client_id} "
                        f"for topic {topic_type}:{topic_id}"
                    )
                    # Attempt to remove stale connection if we can find client_id
                    try:
                        if client_id:
                            self._manager.disconnect(client_id)
                    except Exception:
                        logger.error("Failed to clean up disconnected client", exc_info=True)
                    continue
                except Exception as e:
                    logger.error(f"Error sending message to a subscriber: {e}", exc_info=True)
                    continue

        except Exception as e:
            logger.error(f"Unexpected error in WebSocketPublisher.publish_message: {e}", exc_info=True)
            # Swallow exceptions to keep publish best-effort
            return
