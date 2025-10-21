import pytest
import json
from fastapi import status
from unittest.mock import patch
import time

from cnts_messaging_svc.routers.websocket_router import manager
from cnts_messaging_svc import config


class TestWebSocketRouter:
    """Integration tests for the WebSocket router."""

    def setup_method(self):
        """Set up test fixtures and clear manager state."""
        # Clear manager state before each test to ensure isolation
        manager.active_connections.clear()
        manager.subscriptions.clear()
        manager.client_topics.clear()

    def test_websocket_connect_success(self, client):
        """Test successful WebSocket connection to /api/v1/ws/{client_id}."""
        client_id = "test_client_1"

        with client.websocket_connect(f"/api/v1/ws/{client_id}") as websocket:
            # Connection established successfully if no exception is raised
            assert websocket is not None

            # Verify client is registered in manager
            assert client_id in manager.active_connections
            assert manager.get_connection_count() == 1

    def test_subscribe_acknowledgement(self, client):
        """Test sending SubscribeRequest and receiving success acknowledgement."""
        client_id = "test_client_2"
        subscribe_message = {
            "type": "subscribe",
            "topic_type": "chat",
            "topic_id": "room1"
        }

        with client.websocket_connect(f"/api/v1/ws/{client_id}") as websocket:
            # Send subscribe request
            websocket.send_json(subscribe_message)

            # Receive acknowledgement
            response = websocket.receive_json()

            # Verify acknowledgement format
            assert response["type"] == "ack"
            assert response["request_id"] == "subscribe"
            assert response["status"] == "success"

            # Verify subscription was registered
            subscriptions = manager.get_client_subscriptions(client_id)
            assert len(subscriptions) == 1
            assert ("chat", "room1") in subscriptions

    def test_unsubscribe_acknowledgement(self, client):
        """Test sending UnsubscribeRequest and receiving success acknowledgement."""
        client_id = "test_client_3"

        # First subscribe to a topic
        subscribe_message = {
            "type": "subscribe",
            "topic_type": "notification",
            "topic_id": "user123"
        }

        unsubscribe_message = {
            "type": "unsubscribe",
            "topic_type": "notification",
            "topic_id": "user123"
        }

        with client.websocket_connect(f"/api/v1/ws/{client_id}") as websocket:
            # Subscribe first
            websocket.send_json(subscribe_message)
            subscribe_ack = websocket.receive_json()
            assert subscribe_ack["status"] == "success"

            # Verify subscription exists
            subscriptions = manager.get_client_subscriptions(client_id)
            assert len(subscriptions) == 1

            # Send unsubscribe request
            websocket.send_json(unsubscribe_message)

            # Receive acknowledgement
            response = websocket.receive_json()

            # Verify acknowledgement format
            assert response["type"] == "ack"
            assert response["request_id"] == "unsubscribe"
            assert response["status"] == "success"

            # Verify subscription was removed
            subscriptions = manager.get_client_subscriptions(client_id)
            assert len(subscriptions) == 0

    def test_unknown_message_type_error(self, client):
        """Test sending unknown message type and receiving error message."""
        client_id = "test_client_4"
        unknown_message = {
            "type": "unknown_action",
            "data": "some data"
        }

        with client.websocket_connect(f"/api/v1/ws/{client_id}") as websocket:
            # Send unknown message type
            websocket.send_json(unknown_message)

            # Receive error message
            response = websocket.receive_json()

            # Verify error message format
            assert response["type"] == "error"
            assert "error" in response
            assert "Unknown message type: unknown_action" in response["error"]

    def test_invalid_json_message_error(self, client):
        """Test sending invalid JSON and receiving error message."""
        client_id = "test_client_5"

        with client.websocket_connect(f"/api/v1/ws/{client_id}") as websocket:
            # Send invalid JSON as text
            websocket.send_text("{invalid json}")

            # Receive error message
            response = websocket.receive_json()

            # Verify error message format
            assert response["type"] == "error"
            assert "error" in response
            assert "Failed to process message" in response["error"]

    def test_missing_required_fields_error(self, client):
        """Test sending message with missing required fields and receiving error."""
        client_id = "test_client_6"
        invalid_subscribe = {
            "type": "subscribe",
            "topic_type": "chat"
            # Missing topic_id
        }

        with client.websocket_connect(f"/api/v1/ws/{client_id}") as websocket:
            # Send invalid subscribe request
            websocket.send_json(invalid_subscribe)

            # Receive error message
            response = websocket.receive_json()

            # Verify error message format
            assert response["type"] == "error"
            assert "error" in response
            assert "Failed to process message" in response["error"]

    def test_disconnect_cleans_up_manager(self, client):
        """Test that WebSocket disconnect properly cleans up client from manager."""
        client_id = "test_client_7"

        # Connect and subscribe to multiple topics
        subscribe_messages = [
            {"type": "subscribe", "topic_type": "chat", "topic_id": "room1"},
            {"type": "subscribe", "topic_type": "notification", "topic_id": "user1"}
        ]

        with client.websocket_connect(f"/api/v1/ws/{client_id}") as websocket:
            # Send multiple subscribe requests
            for msg in subscribe_messages:
                websocket.send_json(msg)
                ack = websocket.receive_json()
                assert ack["status"] == "success"

            # Verify subscriptions exist
            subscriptions = manager.get_client_subscriptions(client_id)
            assert len(subscriptions) == 2
            assert manager.get_connection_count() == 1
            assert manager.get_subscription_count() == 2

        # After context exit (disconnect), verify cleanup
        # Give a small moment for cleanup to complete
        time.sleep(0.1)

        # Verify client and subscriptions are cleaned up
        assert manager.get_connection_count() == 0
        assert manager.get_subscription_count() == 0
        assert manager.get_client_subscriptions(client_id) == []
        assert client_id not in manager.active_connections

    def test_multiple_clients_different_topics(self, client):
        """Test multiple clients subscribing to different topics simultaneously."""
        client1_id = "test_client_8a"
        client2_id = "test_client_8b"

        client1_subscribe = {"type": "subscribe", "topic_type": "chat", "topic_id": "room1"}
        client2_subscribe = {"type": "subscribe", "topic_type": "task", "topic_id": "project1"}

        with client.websocket_connect(f"/api/v1/ws/{client1_id}") as ws1, \
             client.websocket_connect(f"/api/v1/ws/{client2_id}") as ws2:

            # Both clients subscribe to different topics
            ws1.send_json(client1_subscribe)
            ws2.send_json(client2_subscribe)

            # Receive acknowledgements
            ack1 = ws1.receive_json()
            ack2 = ws2.receive_json()

            assert ack1["status"] == "success"
            assert ack2["status"] == "success"

            # Verify both connections and subscriptions exist
            assert manager.get_connection_count() == 2
            assert manager.get_subscription_count() == 2

            # Verify individual client subscriptions
            client1_subs = manager.get_client_subscriptions(client1_id)
            client2_subs = manager.get_client_subscriptions(client2_id)

            assert len(client1_subs) == 1
            assert len(client2_subs) == 1
            assert ("chat", "room1") in client1_subs
            assert ("task", "project1") in client2_subs

    def test_client_multiple_subscriptions(self, client):
        """Test single client subscribing to multiple topics."""
        client_id = "test_client_9"

        subscribe_messages = [
            {"type": "subscribe", "topic_type": "chat", "topic_id": "room1"},
            {"type": "subscribe", "topic_type": "chat", "topic_id": "room2"},
            {"type": "subscribe", "topic_type": "notification", "topic_id": "user1"}
        ]

        with client.websocket_connect(f"/api/v1/ws/{client_id}") as websocket:
            # Send multiple subscribe requests
            for msg in subscribe_messages:
                websocket.send_json(msg)
                ack = websocket.receive_json()
                assert ack["status"] == "success"

            # Verify all subscriptions registered
            subscriptions = manager.get_client_subscriptions(client_id)
            assert len(subscriptions) == 3
            assert ("chat", "room1") in subscriptions
            assert ("chat", "room2") in subscriptions
            assert ("notification", "user1") in subscriptions

    def test_subscribe_same_topic_multiple_times(self, client):
        """Test subscribing to the same topic multiple times (should not create duplicates)."""
        client_id = "test_client_10"
        subscribe_message = {"type": "subscribe", "topic_type": "chat", "topic_id": "room1"}

        with client.websocket_connect(f"/api/v1/ws/{client_id}") as websocket:
            # Subscribe to same topic multiple times
            for _ in range(3):
                websocket.send_json(subscribe_message)
                ack = websocket.receive_json()
                assert ack["status"] == "success"

            # Should still only have one subscription
            subscriptions = manager.get_client_subscriptions(client_id)
            assert len(subscriptions) == 1
            assert ("chat", "room1") in subscriptions
            assert manager.get_subscription_count() == 1

    def test_unsubscribe_non_existing_subscription(self, client):
        """Test unsubscribing from a topic not subscribed to (should still succeed)."""
        client_id = "test_client_11"
        unsubscribe_message = {"type": "unsubscribe", "topic_type": "chat", "topic_id": "room1"}

        with client.websocket_connect(f"/api/v1/ws/{client_id}") as websocket:
            # Unsubscribe without subscribing first
            websocket.send_json(unsubscribe_message)

            # Should still receive success acknowledgement
            response = websocket.receive_json()
            assert response["type"] == "ack"
            assert response["request_id"] == "unsubscribe"
            assert response["status"] == "success"

    def test_websocket_endpoint_path_routing(self, client):
        """Test that WebSocket endpoint is correctly routed at /api/v1/ws/{client_id}."""
        # Test correct path
        client_id = "test_client_12"
        with client.websocket_connect(f"/api/v1/ws/{client_id}") as websocket:
            # Connection successful - no exception raised
            assert websocket is not None

        # Test that incorrect paths fail
        incorrect_paths = [
            f"/ws/{client_id}",  # Missing /api/v1 prefix
            f"/api/ws/{client_id}",  # Missing version
            f"/api/v1/websocket/{client_id}",  # Wrong endpoint name
        ]

        for incorrect_path in incorrect_paths:
            try:
                with client.websocket_connect(incorrect_path):
                    pytest.fail(f"Connection should have failed for path: {incorrect_path}")
            except Exception:
                # Expected to fail
                pass

    def test_empty_string_topic_fields_validation(self, client):
        """Test that empty string topic fields are properly validated."""
        client_id = "test_client_13"

        invalid_messages = [
            {"type": "subscribe", "topic_type": "", "topic_id": "room1"},  # Empty topic_type
            {"type": "subscribe", "topic_type": "chat", "topic_id": ""},  # Empty topic_id
            {"type": "unsubscribe", "topic_type": "", "topic_id": "room1"},  # Empty topic_type
            {"type": "unsubscribe", "topic_type": "chat", "topic_id": ""}  # Empty topic_id
        ]

        with client.websocket_connect(f"/api/v1/ws/{client_id}") as websocket:
            for invalid_msg in invalid_messages:
                websocket.send_json(invalid_msg)

                # Should receive error message
                response = websocket.receive_json()
                assert response["type"] == "error"
                assert "Failed to process message" in response["error"]

    @patch('cnts_messaging_svc.routers.websocket_router.manager')
    def test_manager_subscribe_error_handling(self, mock_manager, client):
        """Test handling of manager.subscribe() errors."""
        # Configure mock to raise exception on subscribe
        mock_manager.connect.return_value = None
        mock_manager.subscribe.side_effect = Exception("Manager error")

        client_id = "test_client_14"
        subscribe_message = {"type": "subscribe", "topic_type": "chat", "topic_id": "room1"}

        with client.websocket_connect(f"/api/v1/ws/{client_id}") as websocket:
            websocket.send_json(subscribe_message)

            # Should receive error message
            response = websocket.receive_json()
            assert response["type"] == "error"
            assert "Failed to process message" in response["error"]

    @patch('cnts_messaging_svc.routers.websocket_router.manager')
    def test_manager_unsubscribe_error_handling(self, mock_manager, client):
        """Test handling of manager.unsubscribe() errors."""
        # Configure mock to raise exception on unsubscribe
        mock_manager.connect.return_value = None
        mock_manager.unsubscribe.side_effect = Exception("Manager error")

        client_id = "test_client_15"
        unsubscribe_message = {"type": "unsubscribe", "topic_type": "chat", "topic_id": "room1"}

        with client.websocket_connect(f"/api/v1/ws/{client_id}") as websocket:
            websocket.send_json(unsubscribe_message)

            # Should receive error message
            response = websocket.receive_json()
            assert response["type"] == "error"
            assert "Failed to process message" in response["error"]

    def test_client_id_special_characters(self, client):
        """Test WebSocket connection with special characters in client_id."""
        special_client_ids = [
            "client-with-dashes",
            "client_with_underscores",
            "client.with.dots",
            "client123",
            "Client_Mix3d-Ch4rs"
        ]

        for client_id in special_client_ids:
            with client.websocket_connect(f"/api/v1/ws/{client_id}") as websocket:
                # Connection should succeed
                assert websocket is not None

                # Test basic functionality with special client_id
                subscribe_message = {"type": "subscribe", "topic_type": "test", "topic_id": "123"}
                websocket.send_json(subscribe_message)

                response = websocket.receive_json()
                assert response["status"] == "success"

                # Verify client is properly registered
                assert manager.get_connection_count() == 1
                assert len(manager.get_client_subscriptions(client_id)) == 1
            
            # Verify cleanup after disconnect
            time.sleep(0.1)
            assert manager.get_connection_count() == 0

    def test_inactivity_cleanup_disconnects_stale_client(self, client):
        """Test that a client gets disconnected after inactivity timeout."""
        client_id = "stale_client"

        timeout = config.WEBSOCKET_INACTIVITY_TIMEOUT_SECONDS
        check_interval = config.WEBSOCKET_INACTIVITY_CHECK_INTERVAL_SECONDS

        with client.websocket_connect(f"/api/v1/ws/{client_id}") as websocket:
            assert client_id in manager.active_connections
            # Sleep less than timeout -> should still be connected
            time.sleep(max(0.1, timeout - 0.5))
            assert client_id in manager.active_connections

            # Now wait long enough for cleanup task to run and remove stale connection
            time.sleep(timeout + check_interval + 0.5)

        # After context exit and cleanup wait, client should be removed
        # Allow small extra time for server cleanup
        time.sleep(0.1)
        assert client_id not in manager.active_connections
        assert manager.get_connection_count() == 0

    def test_active_client_remains_connected_with_activity(self, client):
        """Test that an actively messaging client remains connected despite timeout."""
        client_id = "active_client"
        timeout = config.WEBSOCKET_INACTIVITY_TIMEOUT_SECONDS
        check_interval = config.WEBSOCKET_INACTIVITY_CHECK_INTERVAL_SECONDS

        # We'll send periodic messages every (timeout/2) seconds to keep activity updated
        send_interval = max(0.1, timeout / 2.0)
        total_duration = timeout * 3  # run longer than a single timeout

        with client.websocket_connect(f"/api/v1/ws/{client_id}") as websocket:
            start = time.time()
            while time.time() - start < total_duration:
                # Send a benign subscribe/unsubscribe to update activity
                websocket.send_json({"type": "subscribe", "topic_type": "keepalive", "topic_id": "1"})
                _ = websocket.receive_json()
                time.sleep(send_interval)

            # After active messaging, client should still be connected
            assert client_id in manager.active_connections
            assert manager.get_connection_count() == 1

        # Cleanup after context exit
        time.sleep(0.1)
        assert manager.get_connection_count() == 0
