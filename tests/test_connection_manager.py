import pytest
from fastapi import WebSocket
from unittest.mock import Mock

from cnts_messaging_svc.connection_manager import WebSocketConnectionManager


class TestWebSocketConnectionManager:
    """Test suite for WebSocketConnectionManager class."""
    
    @pytest.fixture
    def connection_manager(self):
        """Create a fresh connection manager for each test."""
        return WebSocketConnectionManager()
    
    @pytest.fixture
    def mock_websocket(self):
        """Create a mock WebSocket object."""
        return Mock(spec=WebSocket)
    
    @pytest.fixture
    def mock_websocket2(self):
        """Create a second mock WebSocket object."""
        return Mock(spec=WebSocket)
    
    def test_initial_state(self, connection_manager):
        """Test initial state of connection manager."""
        assert len(connection_manager.active_connections) == 0
        assert len(connection_manager.subscriptions) == 0
        assert len(connection_manager.client_topics) == 0
        assert connection_manager.get_connection_count() == 0
        assert connection_manager.get_subscription_count() == 0
    
    def test_connect_new_client(self, connection_manager, mock_websocket):
        """Test connecting a new client."""
        client_id = "client1"
        
        connection_manager.connect(mock_websocket, client_id)
        
        assert client_id in connection_manager.active_connections
        assert connection_manager.active_connections[client_id] is mock_websocket
        assert client_id in connection_manager.client_topics
        assert len(connection_manager.client_topics[client_id]) == 0
        assert connection_manager.get_connection_count() == 1
    
    def test_connect_duplicate_client(self, connection_manager, mock_websocket, mock_websocket2):
        """Test connecting a client with duplicate ID replaces the connection."""
        client_id = "client1"
        
        # Connect first websocket
        connection_manager.connect(mock_websocket, client_id)
        assert connection_manager.active_connections[client_id] is mock_websocket
        
        # Connect second websocket with same client_id
        connection_manager.connect(mock_websocket2, client_id)
        assert connection_manager.active_connections[client_id] is mock_websocket2
        assert connection_manager.get_connection_count() == 1
    
    def test_disconnect_existing_client(self, connection_manager, mock_websocket):
        """Test disconnecting an existing client."""
        client_id = "client1"
        
        # Connect and then disconnect
        connection_manager.connect(mock_websocket, client_id)
        connection_manager.disconnect(client_id)
        
        assert client_id not in connection_manager.active_connections
        assert client_id not in connection_manager.client_topics
        assert connection_manager.get_connection_count() == 0
    
    def test_disconnect_non_existing_client(self, connection_manager):
        """Test disconnecting a non-existing client (should not raise error)."""
        # Should not raise an exception
        connection_manager.disconnect("non_existing_client")
        assert connection_manager.get_connection_count() == 0
    
    def test_subscribe_connected_client(self, connection_manager, mock_websocket):
        """Test subscribing a connected client to a topic."""
        client_id = "client1"
        topic_type = "chat"
        topic_id = "room1"
        topic_key = (topic_type, topic_id)
        
        # Connect client first
        connection_manager.connect(mock_websocket, client_id)
        
        # Subscribe to topic
        connection_manager.subscribe(client_id, topic_type, topic_id)
        
        # Verify subscription mappings
        assert topic_key in connection_manager.subscriptions
        assert client_id in connection_manager.subscriptions[topic_key]
        assert topic_key in connection_manager.client_topics[client_id]
        assert connection_manager.get_subscription_count() == 1
    
    def test_subscribe_non_connected_client(self, connection_manager):
        """Test subscribing a non-connected client raises ValueError."""
        with pytest.raises(ValueError, match="Client non_connected is not connected"):
            connection_manager.subscribe("non_connected", "chat", "room1")
    
    def test_multiple_clients_same_topic(self, connection_manager, mock_websocket, mock_websocket2):
        """Test multiple clients subscribing to the same topic."""
        client1_id = "client1"
        client2_id = "client2"
        topic_type = "chat"
        topic_id = "room1"
        topic_key = (topic_type, topic_id)
        
        # Connect both clients
        connection_manager.connect(mock_websocket, client1_id)
        connection_manager.connect(mock_websocket2, client2_id)
        
        # Subscribe both to same topic
        connection_manager.subscribe(client1_id, topic_type, topic_id)
        connection_manager.subscribe(client2_id, topic_type, topic_id)
        
        # Verify both are subscribed
        assert len(connection_manager.subscriptions[topic_key]) == 2
        assert client1_id in connection_manager.subscriptions[topic_key]
        assert client2_id in connection_manager.subscriptions[topic_key]
        assert connection_manager.get_subscription_count() == 2
    
    def test_client_multiple_topics(self, connection_manager, mock_websocket):
        """Test a single client subscribing to multiple topics."""
        client_id = "client1"
        topic1_key = ("chat", "room1")
        topic2_key = ("notification", "user1")
        
        # Connect client
        connection_manager.connect(mock_websocket, client_id)
        
        # Subscribe to multiple topics
        connection_manager.subscribe(client_id, "chat", "room1")
        connection_manager.subscribe(client_id, "notification", "user1")
        
        # Verify subscriptions
        assert len(connection_manager.client_topics[client_id]) == 2
        assert topic1_key in connection_manager.client_topics[client_id]
        assert topic2_key in connection_manager.client_topics[client_id]
        assert connection_manager.get_subscription_count() == 2
    
    def test_unsubscribe_existing_subscription(self, connection_manager, mock_websocket):
        """Test unsubscribing from an existing subscription."""
        client_id = "client1"
        topic_type = "chat"
        topic_id = "room1"
        topic_key = (topic_type, topic_id)
        
        # Connect and subscribe
        connection_manager.connect(mock_websocket, client_id)
        connection_manager.subscribe(client_id, topic_type, topic_id)
        
        # Unsubscribe
        connection_manager.unsubscribe(client_id, topic_type, topic_id)
        
        # Verify subscription removed
        assert topic_key not in connection_manager.subscriptions
        assert topic_key not in connection_manager.client_topics[client_id]
        assert connection_manager.get_subscription_count() == 0
    
    def test_unsubscribe_non_existing_subscription(self, connection_manager, mock_websocket):
        """Test unsubscribing from a non-existing subscription (should not raise error)."""
        client_id = "client1"
        
        # Connect client
        connection_manager.connect(mock_websocket, client_id)
        
        # Unsubscribe from non-existing subscription (should not raise error)
        connection_manager.unsubscribe(client_id, "chat", "room1")
        
        assert connection_manager.get_subscription_count() == 0
    
    def test_unsubscribe_non_connected_client(self, connection_manager):
        """Test unsubscribing a non-connected client (should not raise error)."""
        # Should not raise an exception
        connection_manager.unsubscribe("non_connected", "chat", "room1")
        assert connection_manager.get_subscription_count() == 0
    
    def test_get_subscribers_existing_topic(self, connection_manager, mock_websocket, mock_websocket2):
        """Test getting subscribers for an existing topic."""
        client1_id = "client1"
        client2_id = "client2"
        topic_type = "chat"
        topic_id = "room1"
        
        # Connect and subscribe both clients
        connection_manager.connect(mock_websocket, client1_id)
        connection_manager.connect(mock_websocket2, client2_id)
        connection_manager.subscribe(client1_id, topic_type, topic_id)
        connection_manager.subscribe(client2_id, topic_type, topic_id)
        
        # Get subscribers
        subscribers = connection_manager.get_subscribers(topic_type, topic_id)
        
        assert len(subscribers) == 2
        assert mock_websocket in subscribers
        assert mock_websocket2 in subscribers
    
    def test_get_subscribers_non_existing_topic(self, connection_manager):
        """Test getting subscribers for a non-existing topic."""
        subscribers = connection_manager.get_subscribers("chat", "room1")
        assert subscribers == []
    
    def test_get_subscribers_with_stale_references(self, connection_manager, mock_websocket, mock_websocket2):
        """Test getting subscribers handles stale references correctly."""
        client1_id = "client1"
        client2_id = "client2"
        topic_type = "chat"
        topic_id = "room1"
        
        # Connect and subscribe both clients
        connection_manager.connect(mock_websocket, client1_id)
        connection_manager.connect(mock_websocket2, client2_id)
        connection_manager.subscribe(client1_id, topic_type, topic_id)
        connection_manager.subscribe(client2_id, topic_type, topic_id)
        
        # Manually remove one client from active_connections to simulate stale reference
        del connection_manager.active_connections[client1_id]
        
        # Get subscribers should clean up stale reference and return only active one
        subscribers = connection_manager.get_subscribers(topic_type, topic_id)
        
        assert len(subscribers) == 1
        assert mock_websocket2 in subscribers
        assert mock_websocket not in subscribers
    
    def test_get_client_id_existing_websocket(self, connection_manager, mock_websocket):
        """Test getting client_id for an existing websocket."""
        client_id = "client1"
        
        connection_manager.connect(mock_websocket, client_id)
        
        found_client_id = connection_manager.get_client_id(mock_websocket)
        assert found_client_id == client_id
    
    def test_get_client_id_non_existing_websocket(self, connection_manager, mock_websocket):
        """Test getting client_id for a non-existing websocket."""
        found_client_id = connection_manager.get_client_id(mock_websocket)
        assert found_client_id is None
    
    def test_disconnect_with_multiple_subscriptions(self, connection_manager, mock_websocket):
        """Test that disconnecting a client cleans up all its subscriptions."""
        client_id = "client1"
        
        # Connect and subscribe to multiple topics
        connection_manager.connect(mock_websocket, client_id)
        connection_manager.subscribe(client_id, "chat", "room1")
        connection_manager.subscribe(client_id, "chat", "room2")
        connection_manager.subscribe(client_id, "notification", "user1")
        
        # Verify subscriptions exist
        assert connection_manager.get_subscription_count() == 3
        
        # Disconnect client
        connection_manager.disconnect(client_id)
        
        # Verify all subscriptions are cleaned up
        assert connection_manager.get_subscription_count() == 0
        assert len(connection_manager.subscriptions) == 0
        assert client_id not in connection_manager.client_topics
    
    def test_subscribe_same_topic_multiple_times(self, connection_manager, mock_websocket):
        """Test subscribing to the same topic multiple times (should not create duplicates)."""
        client_id = "client1"
        topic_type = "chat"
        topic_id = "room1"
        topic_key = (topic_type, topic_id)
        
        # Connect and subscribe multiple times to same topic
        connection_manager.connect(mock_websocket, client_id)
        connection_manager.subscribe(client_id, topic_type, topic_id)
        connection_manager.subscribe(client_id, topic_type, topic_id)
        connection_manager.subscribe(client_id, topic_type, topic_id)
        
        # Should still only have one subscription
        assert len(connection_manager.subscriptions[topic_key]) == 1
        assert len(connection_manager.client_topics[client_id]) == 1
        assert connection_manager.get_subscription_count() == 1
    
    def test_get_client_subscriptions(self, connection_manager, mock_websocket):
        """Test getting all subscriptions for a client."""
        client_id = "client1"
        
        # Connect and subscribe to multiple topics
        connection_manager.connect(mock_websocket, client_id)
        connection_manager.subscribe(client_id, "chat", "room1")
        connection_manager.subscribe(client_id, "notification", "user1")
        
        subscriptions = connection_manager.get_client_subscriptions(client_id)
        
        assert len(subscriptions) == 2
        assert ("chat", "room1") in subscriptions
        assert ("notification", "user1") in subscriptions
    
    def test_get_client_subscriptions_non_existing_client(self, connection_manager):
        """Test getting subscriptions for a non-existing client."""
        subscriptions = connection_manager.get_client_subscriptions("non_existing")
        assert subscriptions == []
    
    def test_memory_cleanup_empty_subscription_sets(self, connection_manager, mock_websocket):
        """Test that empty subscription sets are cleaned up to prevent memory leaks."""
        client_id = "client1"
        topic_type = "chat"
        topic_id = "room1"
        topic_key = (topic_type, topic_id)
        
        # Connect, subscribe, then unsubscribe
        connection_manager.connect(mock_websocket, client_id)
        connection_manager.subscribe(client_id, topic_type, topic_id)
        
        # Verify subscription exists
        assert topic_key in connection_manager.subscriptions
        
        # Unsubscribe
        connection_manager.unsubscribe(client_id, topic_type, topic_id)
        
        # Verify empty subscription set is removed
        assert topic_key not in connection_manager.subscriptions
        assert connection_manager.get_subscription_count() == 0
