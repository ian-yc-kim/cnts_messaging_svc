import pytest
from fastapi import status
from unittest.mock import patch
from sqlalchemy import select
from datetime import datetime, timezone

from cnts_messaging_svc.models.message import Message
from cnts_messaging_svc.schemas.message import MessageCreate
from cnts_messaging_svc.services.message_persistence import MessagePersistenceService, MessagePersistenceError


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
    
    def test_publish_message_success(self, client, db_session):
        """Test successful message publishing via API endpoint."""
        # Act
        response = client.post(self.endpoint, json=self.valid_message_data)
        
        # Assert response
        assert response.status_code == status.HTTP_200_OK
        
        response_data = response.json()
        
        # Verify response format matches MessageResponse schema
        expected_fields = {
            "topic_type", "topic_id", "message_type", "message_id",
            "sender_type", "sender_id", "content_type", "content", "created_at"
        }
        assert set(response_data.keys()) == expected_fields
        
        # Verify response content matches input
        assert response_data["topic_type"] == self.valid_message_data["topic_type"]
        assert response_data["topic_id"] == self.valid_message_data["topic_id"]
        assert response_data["message_type"] == self.valid_message_data["message_type"]
        assert response_data["sender_type"] == self.valid_message_data["sender_type"]
        assert response_data["sender_id"] == self.valid_message_data["sender_id"]
        assert response_data["content_type"] == self.valid_message_data["content_type"]
        assert response_data["content"] == self.valid_message_data["content"]
        
        # Verify auto-generated fields are present
        assert response_data["message_id"] is not None
        assert isinstance(response_data["message_id"], int)
        assert response_data["message_id"] == 1  # First message in scope
        assert response_data["created_at"] is not None
        
        # Verify message was actually persisted in database
        stmt = select(Message).where(
            Message.topic_type == self.valid_message_data["topic_type"],
            Message.topic_id == self.valid_message_data["topic_id"],
            Message.message_type == self.valid_message_data["message_type"],
            Message.message_id == response_data["message_id"]
        )
        db_result = db_session.execute(stmt)
        persisted_message = db_result.scalar_one_or_none()
        
        assert persisted_message is not None
        assert persisted_message.content == self.valid_message_data["content"]
        assert persisted_message.sender_id == self.valid_message_data["sender_id"]
    
    def test_publish_message_sequential_ids(self, client, db_session):
        """Test that sequential message IDs are generated correctly via API."""
        # Create first message
        response1 = client.post(self.endpoint, json=self.valid_message_data)
        assert response1.status_code == status.HTTP_200_OK
        assert response1.json()["message_id"] == 1
        
        # Create second message in same scope
        message_data2 = self.valid_message_data.copy()
        message_data2["sender_id"] = "user456"
        message_data2["content"] = "Second message"
        
        response2 = client.post(self.endpoint, json=message_data2)
        assert response2.status_code == status.HTTP_200_OK
        assert response2.json()["message_id"] == 2
        
        # Create message in different scope - should reset to 1
        message_data3 = self.valid_message_data.copy()
        message_data3["topic_type"] = "task"  # Different scope
        message_data3["content"] = "Different scope message"
        
        response3 = client.post(self.endpoint, json=message_data3)
        assert response3.status_code == status.HTTP_200_OK
        assert response3.json()["message_id"] == 1  # New scope starts at 1
    
    def test_publish_message_empty_topic_type(self, client):
        """Test validation error for empty topic_type."""
        invalid_data = self.valid_message_data.copy()
        invalid_data["topic_type"] = ""
        
        response = client.post(self.endpoint, json=invalid_data)
        
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
        response_data = response.json()
        assert "detail" in response_data
        
        # Verify error message mentions topic_type and length constraint
        error_details = str(response_data["detail"])
        assert "topic_type" in error_details
        assert "at least 1 character" in error_details.lower()
    
    def test_publish_message_too_long_topic_type(self, client):
        """Test validation error for topic_type exceeding max length."""
        invalid_data = self.valid_message_data.copy()
        invalid_data["topic_type"] = "a" * 256  # Exceeds max_length=255
        
        response = client.post(self.endpoint, json=invalid_data)
        
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
        response_data = response.json()
        assert "detail" in response_data
        
        # Verify error message mentions topic_type and length constraint
        error_details = str(response_data["detail"])
        assert "topic_type" in error_details
        assert "at most 255 characters" in error_details.lower()
    
    def test_publish_message_empty_content(self, client):
        """Test validation error for empty content."""
        invalid_data = self.valid_message_data.copy()
        invalid_data["content"] = ""
        
        response = client.post(self.endpoint, json=invalid_data)
        
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
        response_data = response.json()
        assert "detail" in response_data
        
        # Verify error message mentions content
        error_details = str(response_data["detail"])
        assert "content" in error_details
        assert "at least 1 character" in error_details.lower()
    
    def test_publish_message_missing_required_field(self, client):
        """Test validation error for missing required fields."""
        incomplete_data = self.valid_message_data.copy()
        del incomplete_data["sender_id"]  # Remove required field
        
        response = client.post(self.endpoint, json=incomplete_data)
        
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
        response_data = response.json()
        assert "detail" in response_data
        
        # Verify error message mentions missing field
        error_details = str(response_data["detail"])
        assert "sender_id" in error_details
        assert "required" in error_details.lower()
    
    def test_publish_message_multiple_validation_errors(self, client):
        """Test that multiple validation errors are reported together."""
        invalid_data = {
            "topic_type": "",  # Empty (invalid)
            "topic_id": "123",
            "message_type": "",  # Empty (invalid)
            "sender_type": "user",
            "sender_id": "user123",
            "content_type": "text/plain",
            "content": ""  # Empty (invalid)
        }
        
        response = client.post(self.endpoint, json=invalid_data)
        
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
        response_data = response.json()
        assert "detail" in response_data
        
        # Verify multiple fields are mentioned in error
        error_details = str(response_data["detail"])
        assert "topic_type" in error_details
        assert "message_type" in error_details
        assert "content" in error_details
    
    def test_publish_message_invalid_json(self, client):
        """Test handling of invalid JSON in request body."""
        response = client.post(
            self.endpoint, 
            data="{invalid json}",  # Invalid JSON
            headers={"Content-Type": "application/json"}
        )
        
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    
    def test_publish_message_wrong_data_types(self, client):
        """Test validation error for wrong data types."""
        invalid_data = self.valid_message_data.copy()
        invalid_data["topic_type"] = 123  # Should be string
        
        response = client.post(self.endpoint, json=invalid_data)
        
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
        response_data = response.json()
        assert "detail" in response_data
    
    @patch('cnts_messaging_svc.routers.messages.MessagePersistenceService')
    def test_publish_message_persistence_error(self, mock_service_class, client):
        """Test handling of MessagePersistenceError from service."""
        # Configure mock to raise MessagePersistenceError
        mock_service_instance = mock_service_class.return_value
        mock_service_instance.persist_message.side_effect = MessagePersistenceError("Database error")
        
        response = client.post(self.endpoint, json=self.valid_message_data)
        
        assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        response_data = response.json()
        
        # Assert response matches ErrorResponse format
        expected_fields = {"status_code", "code", "message", "details"}
        assert set(response_data.keys()) == expected_fields
        
        # Assert specific field values for ErrorResponse
        assert response_data["status_code"] == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert response_data["code"] == "HTTP_500"
        assert "Failed to persist message" in response_data["message"]
        assert "Database error" in response_data["message"]
        assert response_data["details"] is None
    
    @patch('cnts_messaging_svc.routers.messages.MessagePersistenceService')
    def test_publish_message_generic_error(self, mock_service_class, client):
        """Test handling of generic exceptions from service."""
        # Configure mock to raise generic exception
        mock_service_instance = mock_service_class.return_value
        mock_service_instance.persist_message.side_effect = Exception("Unexpected error")
        
        response = client.post(self.endpoint, json=self.valid_message_data)
        
        assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        response_data = response.json()
        
        # Assert response matches ErrorResponse format
        expected_fields = {"status_code", "code", "message", "details"}
        assert set(response_data.keys()) == expected_fields
        
        # Assert specific field values for ErrorResponse
        assert response_data["status_code"] == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert response_data["code"] == "HTTP_500"
        assert response_data["message"] == "An unexpected error occurred while processing the message"
        assert response_data["details"] is None
    
    def test_publish_message_no_partial_persistence_on_validation_error(self, client, db_session):
        """Test that validation errors don't result in partial data persistence."""
        # Attempt to create message with validation error
        invalid_data = self.valid_message_data.copy()
        invalid_data["content"] = ""  # Invalid
        
        response = client.post(self.endpoint, json=invalid_data)
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
        
        # Verify no message was persisted
        stmt = select(Message).where(
            Message.topic_type == self.valid_message_data["topic_type"],
            Message.topic_id == self.valid_message_data["topic_id"],
            Message.message_type == self.valid_message_data["message_type"]
        )
        db_result = db_session.execute(stmt)
        messages = db_result.scalars().all()
        assert len(messages) == 0
    
    @patch('cnts_messaging_svc.routers.messages.MessagePersistenceService')
    def test_publish_message_no_partial_persistence_on_service_error(self, mock_service_class, client, db_session):
        """Test that service errors don't result in partial data persistence."""
        # Configure mock to raise error after some processing
        mock_service_instance = mock_service_class.return_value
        mock_service_instance.persist_message.side_effect = MessagePersistenceError("Service error")
        
        response = client.post(self.endpoint, json=self.valid_message_data)
        assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        
        # Verify no message was persisted (service error should prevent persistence)
        stmt = select(Message).where(
            Message.topic_type == self.valid_message_data["topic_type"],
            Message.topic_id == self.valid_message_data["topic_id"],
            Message.message_type == self.valid_message_data["message_type"]
        )
        db_result = db_session.execute(stmt)
        messages = db_result.scalars().all()
        assert len(messages) == 0
    
    def test_publish_message_request_response_format_validation(self, client):
        """Test various valid MessageCreate payloads and response format."""
        test_cases = [
            # Basic ASCII content
            {
                "topic_type": "project",
                "topic_id": "test1",
                "message_type": "status",
                "sender_type": "user",
                "sender_id": "user1",
                "content_type": "text/plain",
                "content": "Basic message"
            },
            # Unicode content
            {
                "topic_type": "task",
                "topic_id": "test2",
                "message_type": "comment",
                "sender_type": "bot",
                "sender_id": "bot1",
                "content_type": "text/markdown",
                "content": "Message with unicode: 🚀 测试"
            },
            # Long content
            {
                "topic_type": "discussion",
                "topic_id": "test3",
                "message_type": "reply",
                "sender_type": "admin",
                "sender_id": "admin1",
                "content_type": "text/html",
                "content": "Very long message content " * 100  # Long content
            },
            # Max length string fields
            {
                "topic_type": "a" * 255,  # Max length
                "topic_id": "b" * 255,
                "message_type": "c" * 255,
                "sender_type": "d" * 255,
                "sender_id": "e" * 255,
                "content_type": "f" * 255,
                "content": "Valid content for max length test"
            }
        ]
        
        for i, test_data in enumerate(test_cases):
            response = client.post(self.endpoint, json=test_data)
            
            assert response.status_code == status.HTTP_200_OK, f"Test case {i+1} failed"
            
            response_data = response.json()
            
            # Verify all required fields in response
            expected_fields = {
                "topic_type", "topic_id", "message_type", "message_id",
                "sender_type", "sender_id", "content_type", "content", "created_at"
            }
            assert set(response_data.keys()) == expected_fields
            
            # Verify input data is preserved in response
            for field in ["topic_type", "topic_id", "message_type", "sender_type", "sender_id", "content_type", "content"]:
                assert response_data[field] == test_data[field]
            
            # Verify auto-generated fields
            assert isinstance(response_data["message_id"], int)
            assert response_data["message_id"] > 0
            assert response_data["created_at"] is not None
            
            # Verify created_at is a valid ISO datetime string
            try:
                datetime.fromisoformat(response_data["created_at"].replace('Z', '+00:00'))
            except ValueError:
                pytest.fail(f"Invalid datetime format in response: {response_data['created_at']}")
    
    def test_publish_message_concurrent_requests(self, client):
        """Test handling of multiple concurrent requests to the same scope."""
        # Create multiple messages in the same scope rapidly
        messages = []
        for i in range(5):
            message_data = self.valid_message_data.copy()
            message_data["sender_id"] = f"user{i}"
            message_data["content"] = f"Message {i+1}"
            
            response = client.post(self.endpoint, json=message_data)
            assert response.status_code == status.HTTP_200_OK
            
            messages.append(response.json())
        
        # Verify sequential message IDs
        message_ids = [msg["message_id"] for msg in messages]
        expected_ids = list(range(1, 6))
        assert message_ids == expected_ids
        
        # Verify all messages have unique content
        contents = [msg["content"] for msg in messages]
        assert len(set(contents)) == 5  # All unique
    
    def test_publish_message_endpoint_path(self, client):
        """Test that the endpoint is correctly mapped to /api/v1/messages."""
        # Test correct path
        response = client.post("/api/v1/messages", json=self.valid_message_data)
        assert response.status_code == status.HTTP_200_OK
        
        # Test that trailing slash also works (FastAPI handles this)
        response = client.post("/api/v1/messages/", json=self.valid_message_data)
        assert response.status_code == status.HTTP_200_OK
        
        # Test incorrect paths should return 404
        incorrect_paths = [
            "/messages",  # Missing /api/v1 prefix
            "/api/messages",  # Old API version
            "/api/v1/message",  # Singular instead of plural
            "/api/v1/Messages",  # Wrong case
        ]
        
        for path in incorrect_paths:
            response = client.post(path, json=self.valid_message_data)
            assert response.status_code == status.HTTP_404_NOT_FOUND
    
    def test_publish_message_http_methods(self, client):
        """Test that only POST method is allowed on the endpoint."""
        # POST should work
        response = client.post(self.endpoint, json=self.valid_message_data)
        assert response.status_code == status.HTTP_200_OK
        
        # Other HTTP methods should return 405
        disallowed_methods = [
            client.get,
            client.put,
            client.patch,
            client.delete,
        ]
        
        for method in disallowed_methods:
            response = method(self.endpoint)
            assert response.status_code == status.HTTP_405_METHOD_NOT_ALLOWED
