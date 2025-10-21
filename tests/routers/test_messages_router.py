import pytest
from fastapi import status
from unittest.mock import patch
from sqlalchemy import select
from datetime import datetime, timezone
import threading
import queue
import time

from cnts_messaging_svc.models.message import Message
from cnts_messaging_svc.schemas.message import MessageCreate
from cnts_messaging_svc.services.message_persistence import MessagePersistenceService, MessagePersistenceError
from cnts_messaging_svc.routers.websocket_router import manager


class TestMessagesRouter:
    """Integration tests for the messages router."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.valid_message_data = {
            "topic_type": "project",
            "topic_id": "123",
            "message_type": "status_update",
            "sender_type": "user",
            "sender_id": "user123",
            "content_type": "text/plain",
            "content": "Test message content"
        }
        self.endpoint = "/api/v1/messages"

    # ... existing tests remain unchanged ...

    def test_publish_message_broadcasts_to_subscribed_clients(self, client):
        """Test that publishing a message broadcasts only to subscribers of that topic."""
        # Clear manager state to ensure isolation
        manager.active_connections.clear()
        manager.subscriptions.clear()
        manager.client_topics.clear()

        # Connect three websocket clients: two subscribe to topicA, one to topicB
        client_a1 = "a_client_1"
        client_a2 = "a_client_2"
        client_b1 = "b_client_1"

        subscribe_a = {"type": "subscribe", "topic_type": "project", "topic_id": "123"}
        subscribe_b = {"type": "subscribe", "topic_type": "task", "topic_id": "999"}

        with client.websocket_connect(f"/api/v1/ws/{client_a1}") as ws_a1, \
             client.websocket_connect(f"/api/v1/ws/{client_a2}") as ws_a2, \
             client.websocket_connect(f"/api/v1/ws/{client_b1}") as ws_b1:

            # Subscribe clients
            ws_a1.send_json(subscribe_a)
            ws_a1.receive_json()  # ack
            ws_a2.send_json(subscribe_a)
            ws_a2.receive_json()  # ack
            ws_b1.send_json(subscribe_b)
            ws_b1.receive_json()  # ack

            # Publish a message to project:123
            response = client.post(self.endpoint, json=self.valid_message_data)
            assert response.status_code == status.HTTP_200_OK

            # a1 and a2 should receive the MessageDelivery
            delivery_a1 = ws_a1.receive_json()
            delivery_a2 = ws_a2.receive_json()

            assert delivery_a1["type"] == "message"
            assert delivery_a1["message"]["content"] == self.valid_message_data["content"]
            assert delivery_a1["message"]["topic_type"] == self.valid_message_data["topic_type"]

            assert delivery_a2["type"] == "message"
            assert delivery_a2["message"]["sender_id"] == self.valid_message_data["sender_id"]

            # b1 should NOT receive any message for project:123
            # Attempt to receive in a background thread with timeout
            q = queue.Queue()
            def try_receive(ws, q):
                try:
                    msg = ws.receive_json()
                    q.put(msg)
                except Exception as e:
                    q.put(e)

            t = threading.Thread(target=try_receive, args=(ws_b1, q), daemon=True)
            t.start()
            t.join(timeout=0.2)

            # If thread is still alive, no message arrived (expected). If queue has item, it's an unexpected message.
            if not q.empty():
                item = q.get()
                # If we received something, that's a failure
                pytest.fail(f"Unexpected message received by non-subscriber b1: {item}")

    def test_publish_message_with_one_disconnected_subscriber(self, client):
        """Test broadcasting when one subscriber disconnects before broadcast."""
        # Clear manager state
        manager.active_connections.clear()
        manager.subscriptions.clear()
        manager.client_topics.clear()

        client_live = "live_client"
        client_dead = "dead_client"

        subscribe = {"type": "subscribe", "topic_type": "project", "topic_id": "123"}

        # Connect two clients and subscribe both
        ws_live = client.websocket_connect(f"/api/v1/ws/{client_live}")
        conn_live = ws_live.__enter__()
        conn_live.send_json(subscribe)
        conn_live.receive_json()

        ws_dead = client.websocket_connect(f"/api/v1/ws/{client_dead}")
        conn_dead = ws_dead.__enter__()
        conn_dead.send_json(subscribe)
        conn_dead.receive_json()

        # Now explicitly close dead client to simulate disconnect
        ws_dead.__exit__(None, None, None)

        # Publish message
        response = client.post(self.endpoint, json=self.valid_message_data)
        assert response.status_code == status.HTTP_200_OK

        # live client should still receive message
        delivery = conn_live.receive_json()
        assert delivery["type"] == "message"
        assert delivery["message"]["content"] == self.valid_message_data["content"]

        # cleanup live
        ws_live.__exit__(None, None, None)
