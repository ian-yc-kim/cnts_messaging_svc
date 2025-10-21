from typing import Dict, Set, Tuple, List, Optional
from fastapi import WebSocket
import logging
from datetime import datetime


class WebSocketConnectionManager:
    """Manages active WebSocket connections and their topic subscriptions.

    Stores active_connections mapping client_id -> (WebSocket, last_activity_at)
    """

    def __init__(self):
        # Mapping client_id -> (WebSocket object, last_activity_at)
        self.active_connections: Dict[str, Tuple[WebSocket, datetime]] = {}
        # Mapping (topic_type, topic_id) -> Set of client_ids
        self.subscriptions: Dict[Tuple[str, str], Set[str]] = {}
        # Mapping client_id -> Set of (topic_type, topic_id) tuples
        self.client_topics: Dict[str, Set[Tuple[str, str]]] = {}

    def connect(self, websocket: WebSocket, client_id: str) -> None:
        """Add a new WebSocket connection and initialize its last activity timestamp.

        Args:
            websocket: The WebSocket connection object
            client_id: Unique identifier for the client
        """
        try:
            # If client already exists, we'll replace the connection
            if client_id in self.active_connections:
                logging.warning(f"Client {client_id} already connected, replacing connection")
                self.disconnect(client_id)

            self.active_connections[client_id] = (websocket, datetime.utcnow())
            self.client_topics[client_id] = set()

            logging.info(f"Client {client_id} connected successfully")

        except Exception as e:
            logging.error(f"Failed to connect client {client_id}: {e}", exc_info=True)
            raise

    def update_activity(self, client_id: str) -> None:
        """Update last_activity_at for a given client to current UTC time.

        Args:
            client_id: Unique identifier for the client
        """
        try:
            if client_id not in self.active_connections:
                logging.warning(f"Attempted to update activity for unknown client {client_id}")
                return
            websocket, _ = self.active_connections[client_id]
            self.active_connections[client_id] = (websocket, datetime.utcnow())
            logging.debug(f"Updated activity for client {client_id}")
        except Exception as e:
            logging.error(f"Failed to update activity for client {client_id}: {e}", exc_info=True)
            raise

    def disconnect(self, client_id: str) -> None:
        """Remove a WebSocket connection and all its associated subscriptions.

        Args:
            client_id: Unique identifier for the client to disconnect
        """
        try:
            if client_id not in self.active_connections:
                logging.warning(f"Attempting to disconnect non-existent client: {client_id}")
                return

            # Remove the connection
            del self.active_connections[client_id]

            # Clean up all subscriptions for this client
            if client_id in self.client_topics:
                topics_to_unsubscribe = self.client_topics[client_id].copy()
                for topic_type, topic_id in topics_to_unsubscribe:
                    self._remove_subscription(client_id, topic_type, topic_id)

                # Remove client from client_topics
                del self.client_topics[client_id]

            logging.info(f"Client {client_id} disconnected and cleaned up successfully")

        except Exception as e:
            logging.error(f"Failed to disconnect client {client_id}: {e}", exc_info=True)
            raise

    def subscribe(self, client_id: str, topic_type: str, topic_id: str) -> None:
        """Add a subscription for a client to a topic.

        Args:
            client_id: Unique identifier for the client
            topic_type: Type of the topic to subscribe to
            topic_id: ID of the topic to subscribe to
        """
        try:
            if client_id not in self.active_connections:
                raise ValueError(f"Client {client_id} is not connected")

            topic_key = (topic_type, topic_id)

            # Add to subscriptions mapping
            if topic_key not in self.subscriptions:
                self.subscriptions[topic_key] = set()
            self.subscriptions[topic_key].add(client_id)

            # Add to client_topics mapping
            if client_id not in self.client_topics:
                self.client_topics[client_id] = set()
            self.client_topics[client_id].add(topic_key)

            logging.info(f"Client {client_id} subscribed to topic {topic_type}:{topic_id}")

        except Exception as e:
            logging.error(
                f"Failed to subscribe client {client_id} to topic {topic_type}:{topic_id}: {e}", 
                exc_info=True
            )
            raise

    def unsubscribe(self, client_id: str, topic_type: str, topic_id: str) -> None:
        """Remove a specific subscription for a client.

        Args:
            client_id: Unique identifier for the client
            topic_type: Type of the topic to unsubscribe from
            topic_id: ID of the topic to unsubscribe from
        """
        try:
            if client_id not in self.active_connections:
                logging.warning(f"Attempting to unsubscribe non-connected client: {client_id}")
                return

            self._remove_subscription(client_id, topic_type, topic_id)
            logging.info(f"Client {client_id} unsubscribed from topic {topic_type}:{topic_id}")

        except Exception as e:
            logging.error(
                f"Failed to unsubscribe client {client_id} from topic {topic_type}:{topic_id}: {e}", 
                exc_info=True
            )
            raise

    def _remove_subscription(self, client_id: str, topic_type: str, topic_id: str) -> None:
        """Internal method to remove a subscription and clean up empty sets.

        Args:
            client_id: Unique identifier for the client
            topic_type: Type of the topic
            topic_id: ID of the topic
        """
        topic_key = (topic_type, topic_id)

        # Remove from subscriptions mapping
        if topic_key in self.subscriptions:
            self.subscriptions[topic_key].discard(client_id)
            # Clean up empty subscription sets to prevent memory leaks
            if not self.subscriptions[topic_key]:
                del self.subscriptions[topic_key]

        # Remove from client_topics mapping
        if client_id in self.client_topics:
            self.client_topics[client_id].discard(topic_key)

    def get_subscribers(self, topic_type: str, topic_id: str) -> List[WebSocket]:
        """Return all active WebSocket objects subscribed to the given topic.

        Args:
            topic_type: Type of the topic
            topic_id: ID of the topic

        Returns:
            List of WebSocket objects for clients subscribed to the topic
        """
        try:
            topic_key = (topic_type, topic_id)

            if topic_key not in self.subscriptions:
                return []

            subscriber_websockets: List[WebSocket] = []
            client_ids_to_remove: List[str] = []

            for client_id in list(self.subscriptions[topic_key]):
                if client_id in self.active_connections:
                    websocket, _ = self.active_connections[client_id]
                    subscriber_websockets.append(websocket)
                else:
                    # Mark stale client_ids for removal
                    client_ids_to_remove.append(client_id)

            # Clean up stale references
            for stale_client_id in client_ids_to_remove:
                self._remove_subscription(stale_client_id, topic_type, topic_id)
                logging.warning(
                    f"Removed stale subscription for client {stale_client_id} "
                    f"from topic {topic_type}:{topic_id}"
                )

            return subscriber_websockets

        except Exception as e:
            logging.error(
                f"Failed to get subscribers for topic {topic_type}:{topic_id}: {e}", 
                exc_info=True
            )
            return []

    def get_client_id(self, websocket: WebSocket) -> Optional[str]:
        """Helper to retrieve client_id from a websocket object.

        Args:
            websocket: The WebSocket object to lookup

        Returns:
            The client_id associated with the WebSocket, or None if not found
        """
        try:
            for client_id, (ws, _) in self.active_connections.items():
                if ws is websocket:
                    return client_id
            return None

        except Exception as e:
            logging.error(f"Failed to get client_id for websocket: {e}", exc_info=True)
            return None

    def get_connection_count(self) -> int:
        """Get the number of active connections.

        Returns:
            Number of active WebSocket connections
        """
        return len(self.active_connections)

    def get_subscription_count(self) -> int:
        """Get the total number of active subscriptions.

        Returns:
            Total number of active subscriptions across all topics
        """
        return sum(len(subscribers) for subscribers in self.subscriptions.values())

    def get_client_subscriptions(self, client_id: str) -> List[Tuple[str, str]]:
        """Get all topics a client is subscribed to.

        Args:
            client_id: Unique identifier for the client

        Returns:
            List of (topic_type, topic_id) tuples the client is subscribed to
        """
        if client_id not in self.client_topics:
            return []
        return list(self.client_topics[client_id])
