from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from cnts_messaging_svc.connection_manager import WebSocketConnectionManager
from cnts_messaging_svc.schemas.websocket import (
    SubscribeRequest, 
    UnsubscribeRequest, 
    WebSocketMessage, 
    Acknowledgement, 
    ErrorMessage
)
import logging

logger = logging.getLogger(__name__)

websocket_router = APIRouter()
manager = WebSocketConnectionManager()


@websocket_router.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str):
    """WebSocket endpoint for real-time client connections.
    
    Args:
        websocket: WebSocket connection object
        client_id: Unique identifier for the client
    """
    try:
        # Accept the WebSocket connection
        await websocket.accept()
        logger.info(f"WebSocket connection accepted for client {client_id}")
    except Exception as e:
        logger.error(f"Failed to accept WebSocket connection for client {client_id}: {e}", exc_info=True)
        return
    
    try:
        # Register the connection with the manager
        manager.connect(websocket, client_id)
        logger.info(f"Client {client_id} connected to WebSocket manager")
        
        # Message handling loop
        while True:
            try:
                # Receive text data from the client
                data = await websocket.receive_text()
                logger.debug(f"Received message from client {client_id}: {data}")
                
                try:
                    # Parse the incoming message
                    msg = WebSocketMessage.model_validate_json(data)
                    
                    if msg.type == "subscribe":
                        # Handle subscription request
                        subscribe_req = SubscribeRequest.model_validate_json(data)
                        manager.subscribe(client_id, subscribe_req.topic_type, subscribe_req.topic_id)
                        
                        # Send acknowledgement
                        ack = Acknowledgement(request_id="subscribe", status="success")
                        await websocket.send_json(ack.model_dump())
                        
                        logger.info(
                            f"Client {client_id} subscribed to topic {subscribe_req.topic_type}:{subscribe_req.topic_id}"
                        )
                        
                    elif msg.type == "unsubscribe":
                        # Handle unsubscription request
                        unsubscribe_req = UnsubscribeRequest.model_validate_json(data)
                        manager.unsubscribe(client_id, unsubscribe_req.topic_type, unsubscribe_req.topic_id)
                        
                        # Send acknowledgement
                        ack = Acknowledgement(request_id="unsubscribe", status="success")
                        await websocket.send_json(ack.model_dump())
                        
                        logger.info(
                            f"Client {client_id} unsubscribed from topic {unsubscribe_req.topic_type}:{unsubscribe_req.topic_id}"
                        )
                        
                    else:
                        # Handle unknown message type
                        logger.warning(f"Unknown WebSocket message type received from client {client_id}: {msg.type}")
                        error_msg = ErrorMessage(error=f"Unknown message type: {msg.type}")
                        await websocket.send_json(error_msg.model_dump())
                        
                except Exception as e:
                    # Handle message processing errors
                    logger.error(f"Error processing WebSocket message from {client_id}: {e}", exc_info=True)
                    error_msg = ErrorMessage(error=f"Failed to process message: {str(e)}")
                    try:
                        await websocket.send_json(error_msg.model_dump())
                    except Exception as send_error:
                        logger.error(f"Failed to send error message to client {client_id}: {send_error}", exc_info=True)
                        break
                        
            except WebSocketDisconnect:
                # Client disconnected normally
                logger.info(f"Client {client_id} disconnected normally")
                break
                
            except Exception as e:
                # Handle other unexpected errors in message loop
                logger.error(f"Unexpected error in message loop for client {client_id}: {e}", exc_info=True)
                break
                
    except WebSocketDisconnect:
        # Handle disconnect during connection setup
        logger.info(f"Client {client_id} disconnected during setup")
        
    except Exception as e:
        # Handle any other unexpected errors
        logger.error(f"Unexpected error in WebSocket connection for {client_id}: {e}", exc_info=True)
        
    finally:
        # Ensure client is disconnected from manager
        try:
            manager.disconnect(client_id)
            logger.info(f"Client {client_id} cleaned up from connection manager")
        except Exception as e:
            logger.error(f"Error cleaning up client {client_id} from manager: {e}", exc_info=True)
