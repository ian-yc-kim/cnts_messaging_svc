import pytest
from pydantic import ValidationError
from datetime import datetime

from cnts_messaging_svc.schemas.websocket import (
    WebSocketMessage,
    SubscribeRequest,
    UnsubscribeRequest,
    Acknowledgement,
    ErrorMessage,
    MessageDelivery
)
from cnts_messaging_svc.schemas.message import MessageResponse


class TestWebSocketMessage:
    """Test suite for WebSocketMessage base class."""
    
    def test_websocket_message_valid(self):
        """Test creating a valid WebSocketMessage."""
        message = WebSocketMessage(type="test")
        assert message.type == "test"
    
    def test_websocket_message_empty_type(self):
        """Test WebSocketMessage with empty type fails validation."""
        with pytest.raises(ValidationError):
            WebSocketMessage(type="")
    
    def test_websocket_message_missing_type(self):
        """Test WebSocketMessage without type fails validation."""
        with pytest.raises(ValidationError):
            WebSocketMessage()


class TestSubscribeRequest:
    """Test suite for SubscribeRequest schema."""
    
    def test_subscribe_request_valid(self):
        """Test creating a valid SubscribeRequest."""
        request = SubscribeRequest(
            topic_type="chat",
            topic_id="room1"
        )
        assert request.type == "subscribe"
        assert request.topic_type == "chat"
        assert request.topic_id == "room1"
    
    def test_subscribe_request_json_serialization(self):
        """Test JSON serialization of SubscribeRequest."""
        request = SubscribeRequest(
            topic_type="chat",
            topic_id="room1"
        )
        json_str = request.model_dump_json()
        assert '"type":"subscribe"' in json_str
        assert '"topic_type":"chat"' in json_str
        assert '"topic_id":"room1"' in json_str
    
    def test_subscribe_request_from_dict(self):
        """Test creating SubscribeRequest from dictionary."""
        data = {
            "type": "subscribe",
            "topic_type": "notification",
            "topic_id": "user123"
        }
        request = SubscribeRequest(**data)
        assert request.topic_type == "notification"
        assert request.topic_id == "user123"
    
    def test_subscribe_request_empty_topic_type(self):
        """Test SubscribeRequest with empty topic_type fails validation."""
        with pytest.raises(ValidationError):
            SubscribeRequest(
                topic_type="",
                topic_id="room1"
            )
    
    def test_subscribe_request_empty_topic_id(self):
        """Test SubscribeRequest with empty topic_id fails validation."""
        with pytest.raises(ValidationError):
            SubscribeRequest(
                topic_type="chat",
                topic_id=""
            )
    
    def test_subscribe_request_missing_topic_type(self):
        """Test SubscribeRequest without topic_type fails validation."""
        with pytest.raises(ValidationError):
            SubscribeRequest(topic_id="room1")
    
    def test_subscribe_request_missing_topic_id(self):
        """Test SubscribeRequest without topic_id fails validation."""
        with pytest.raises(ValidationError):
            SubscribeRequest(topic_type="chat")
    
    def test_subscribe_request_max_length_topic_type(self):
        """Test SubscribeRequest with topic_type exceeding max length fails validation."""
        long_topic_type = "x" * 256  # exceeds 255 max_length
        with pytest.raises(ValidationError):
            SubscribeRequest(
                topic_type=long_topic_type,
                topic_id="room1"
            )
    
    def test_subscribe_request_max_length_topic_id(self):
        """Test SubscribeRequest with topic_id exceeding max length fails validation."""
        long_topic_id = "x" * 256  # exceeds 255 max_length
        with pytest.raises(ValidationError):
            SubscribeRequest(
                topic_type="chat",
                topic_id=long_topic_id
            )
    
    def test_subscribe_request_boundary_length_valid(self):
        """Test SubscribeRequest with boundary length values passes validation."""
        # Test exactly 255 characters (max allowed)
        topic_type_255 = "x" * 255
        topic_id_255 = "y" * 255
        
        request = SubscribeRequest(
            topic_type=topic_type_255,
            topic_id=topic_id_255
        )
        assert len(request.topic_type) == 255
        assert len(request.topic_id) == 255


class TestUnsubscribeRequest:
    """Test suite for UnsubscribeRequest schema."""
    
    def test_unsubscribe_request_valid(self):
        """Test creating a valid UnsubscribeRequest."""
        request = UnsubscribeRequest(
            topic_type="chat",
            topic_id="room1"
        )
        assert request.type == "unsubscribe"
        assert request.topic_type == "chat"
        assert request.topic_id == "room1"
    
    def test_unsubscribe_request_json_serialization(self):
        """Test JSON serialization of UnsubscribeRequest."""
        request = UnsubscribeRequest(
            topic_type="notification",
            topic_id="user456"
        )
        json_str = request.model_dump_json()
        assert '"type":"unsubscribe"' in json_str
        assert '"topic_type":"notification"' in json_str
        assert '"topic_id":"user456"' in json_str
    
    def test_unsubscribe_request_validation_same_as_subscribe(self):
        """Test UnsubscribeRequest has same validation as SubscribeRequest."""
        # Empty topic_type should fail
        with pytest.raises(ValidationError):
            UnsubscribeRequest(topic_type="", topic_id="room1")
        
        # Empty topic_id should fail
        with pytest.raises(ValidationError):
            UnsubscribeRequest(topic_type="chat", topic_id="")
        
        # Too long topic_type should fail
        long_topic_type = "x" * 256
        with pytest.raises(ValidationError):
            UnsubscribeRequest(topic_type=long_topic_type, topic_id="room1")
        
        # Too long topic_id should fail
        long_topic_id = "x" * 256
        with pytest.raises(ValidationError):
            UnsubscribeRequest(topic_type="chat", topic_id=long_topic_id)


class TestAcknowledgement:
    """Test suite for Acknowledgement schema."""
    
    def test_acknowledgement_success_valid(self):
        """Test creating a valid success Acknowledgement."""
        ack = Acknowledgement(
            request_id="req123",
            status="success"
        )
        assert ack.type == "ack"
        assert ack.request_id == "req123"
        assert ack.status == "success"
    
    def test_acknowledgement_error_valid(self):
        """Test creating a valid error Acknowledgement."""
        ack = Acknowledgement(
            request_id="req456",
            status="error"
        )
        assert ack.type == "ack"
        assert ack.request_id == "req456"
        assert ack.status == "error"
    
    def test_acknowledgement_json_serialization(self):
        """Test JSON serialization of Acknowledgement."""
        ack = Acknowledgement(
            request_id="req789",
            status="success"
        )
        json_str = ack.model_dump_json()
        assert '"type":"ack"' in json_str
        assert '"request_id":"req789"' in json_str
        assert '"status":"success"' in json_str
    
    def test_acknowledgement_empty_request_id(self):
        """Test Acknowledgement with empty request_id fails validation."""
        with pytest.raises(ValidationError):
            Acknowledgement(
                request_id="",
                status="success"
            )
    
    def test_acknowledgement_missing_request_id(self):
        """Test Acknowledgement without request_id fails validation."""
        with pytest.raises(ValidationError):
            Acknowledgement(status="success")
    
    def test_acknowledgement_missing_status(self):
        """Test Acknowledgement without status fails validation."""
        with pytest.raises(ValidationError):
            Acknowledgement(request_id="req123")
    
    def test_acknowledgement_invalid_status(self):
        """Test Acknowledgement with invalid status fails validation."""
        with pytest.raises(ValidationError):
            Acknowledgement(
                request_id="req123",
                status="invalid_status"
            )
    
    def test_acknowledgement_max_length_request_id(self):
        """Test Acknowledgement with request_id exceeding max length fails validation."""
        long_request_id = "x" * 256  # exceeds 255 max_length
        with pytest.raises(ValidationError):
            Acknowledgement(
                request_id=long_request_id,
                status="success"
            )


class TestErrorMessage:
    """Test suite for ErrorMessage schema."""
    
    def test_error_message_valid(self):
        """Test creating a valid ErrorMessage."""
        error_msg = ErrorMessage(
            error="Connection failed"
        )
        assert error_msg.type == "error"
        assert error_msg.error == "Connection failed"
    
    def test_error_message_json_serialization(self):
        """Test JSON serialization of ErrorMessage."""
        error_msg = ErrorMessage(
            error="Invalid topic type"
        )
        json_str = error_msg.model_dump_json()
        assert '"type":"error"' in json_str
        assert '"error":"Invalid topic type"' in json_str
    
    def test_error_message_empty_error(self):
        """Test ErrorMessage with empty error fails validation."""
        with pytest.raises(ValidationError):
            ErrorMessage(error="")
    
    def test_error_message_missing_error(self):
        """Test ErrorMessage without error fails validation."""
        with pytest.raises(ValidationError):
            ErrorMessage()
    
    def test_error_message_long_error(self):
        """Test ErrorMessage with very long error message."""
        # Test with a reasonably long error message (no max_length constraint)
        long_error = "This is a very long error message: " + "x" * 1000
        error_msg = ErrorMessage(error=long_error)
        assert error_msg.error == long_error


class TestMessageDelivery:
    """Test suite for MessageDelivery schema."""
    
    @pytest.fixture
    def sample_message_response(self):
        """Create a sample MessageResponse for testing."""
        return MessageResponse(
            topic_type="chat",
            topic_id="room1",
            message_type="text",
            message_id=123,
            sender_type="user",
            sender_id="user456",
            content_type="text/plain",
            content="Hello, world!",
            created_at=datetime.now()
        )
    
    def test_message_delivery_valid(self, sample_message_response):
        """Test creating a valid MessageDelivery."""
        delivery = MessageDelivery(
            message=sample_message_response
        )
        assert delivery.type == "message"
        assert delivery.message == sample_message_response
        assert delivery.message.topic_type == "chat"
        assert delivery.message.topic_id == "room1"
        assert delivery.message.message_id == 123
    
    def test_message_delivery_json_serialization(self, sample_message_response):
        """Test JSON serialization of MessageDelivery."""
        delivery = MessageDelivery(
            message=sample_message_response
        )
        json_str = delivery.model_dump_json()
        assert '"type":"message"' in json_str
        assert '"topic_type":"chat"' in json_str
        assert '"topic_id":"room1"' in json_str
        assert '"message_id":123' in json_str
        assert '"content":"Hello, world!"' in json_str
    
    def test_message_delivery_missing_message(self):
        """Test MessageDelivery without message fails validation."""
        with pytest.raises(ValidationError):
            MessageDelivery()
    
    def test_message_delivery_from_dict(self):
        """Test creating MessageDelivery from dictionary with nested message."""
        message_data = {
            "topic_type": "notification",
            "topic_id": "user123",
            "message_type": "alert",
            "message_id": 456,
            "sender_type": "system",
            "sender_id": "system_bot",
            "content_type": "application/json",
            "content": '{"alert": "New message"}',
            "created_at": "2023-01-01T10:00:00"
        }
        
        delivery_data = {
            "type": "message",
            "message": message_data
        }
        
        delivery = MessageDelivery(**delivery_data)
        assert delivery.message.topic_type == "notification"
        assert delivery.message.message_id == 456
        assert delivery.message.sender_type == "system"
    
    def test_message_delivery_invalid_message_data(self):
        """Test MessageDelivery with invalid MessageResponse data fails validation."""
        # Use missing required field to cause validation failure
        invalid_message_data = {
            "topic_type": "chat",
            "topic_id": "user123",
            "message_type": "alert",
            "message_id": 456,
            "sender_type": "system",
            # Missing required field: sender_id
            "content_type": "application/json",
            "content": '{"alert": "New message"}',
            "created_at": "2023-01-01T10:00:00"
        }
        
        with pytest.raises(ValidationError):
            MessageDelivery(message=invalid_message_data)


class TestWebSocketMessageTypeDiscrimination:
    """Test discriminated union behavior with type field."""
    
    def test_type_field_discrimination(self):
        """Test that type field correctly discriminates between message types."""
        # Test that each schema has the correct literal type
        subscribe = SubscribeRequest(topic_type="chat", topic_id="room1")
        unsubscribe = UnsubscribeRequest(topic_type="chat", topic_id="room1")
        ack = Acknowledgement(request_id="req123", status="success")
        error = ErrorMessage(error="Test error")
        
        assert subscribe.type == "subscribe"
        assert unsubscribe.type == "unsubscribe"
        assert ack.type == "ack"
        assert error.type == "error"
        # MessageDelivery type is tested in TestMessageDelivery
    
    def test_type_field_immutable(self):
        """Test that type field cannot be overridden."""
        # The type field should be set by the Literal constraint
        # and not changeable through constructor
        subscribe = SubscribeRequest(
            topic_type="chat",
            topic_id="room1"
            # Note: not passing type parameter
        )
        assert subscribe.type == "subscribe"
        
        # Test serialization maintains correct type
        data = subscribe.model_dump()
        assert data["type"] == "subscribe"
