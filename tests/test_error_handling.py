"""Tests for global exception handlers and standardized error responses."""

import pytest
from fastapi import status
from unittest.mock import patch

from cnts_messaging_svc.app import app


class TestGlobalExceptionHandlers:
    """Test global exception handlers for standardized error responses."""
    
    def test_404_nonexistent_url_returns_standard_error(self, client):
        """Test that requesting a non-existent URL returns standardized 404 error response."""
        # Act - request a non-existent endpoint
        response = client.get("/api/v1/nonexistent")
        
        # Assert - check status code
        assert response.status_code == status.HTTP_404_NOT_FOUND
        
        # Assert - check response format matches ErrorResponse schema
        response_data = response.json()
        expected_fields = {"status_code", "code", "message", "details"}
        assert set(response_data.keys()) == expected_fields
        
        # Assert - check specific field values
        assert response_data["status_code"] == status.HTTP_404_NOT_FOUND
        assert response_data["code"] == "HTTP_404"
        assert "Not Found" in response_data["message"]
        assert response_data["details"] is None
    
    def test_405_method_not_allowed_returns_standard_error(self, client):
        """Test that disallowed HTTP method returns standardized 405 error response."""
        # Act - use GET method on POST-only endpoint
        response = client.get("/api/v1/messages")
        
        # Assert - check status code
        assert response.status_code == status.HTTP_405_METHOD_NOT_ALLOWED
        
        # Assert - check response format matches ErrorResponse schema
        response_data = response.json()
        expected_fields = {"status_code", "code", "message", "details"}
        assert set(response_data.keys()) == expected_fields
        
        # Assert - check specific field values
        assert response_data["status_code"] == status.HTTP_405_METHOD_NOT_ALLOWED
        assert response_data["code"] == "HTTP_405"
        assert "Method Not Allowed" in response_data["message"]
        assert response_data["details"] is None
    
    @patch('cnts_messaging_svc.services.message_persistence.MessagePersistenceService.persist_message')
    def test_generic_exception_returns_standard_error(self, mock_persist_message, client):
        """Test that unhandled exceptions return standardized 500 error response."""
        # Configure mock to raise a generic exception (not MessagePersistenceError)
        # This will be caught by the messages router and re-raised as HTTPException
        mock_persist_message.side_effect = RuntimeError("Test exception for error handling")
        
        # Valid message data for the request
        valid_message_data = {
            "topic_type": "project",
            "topic_id": "123",
            "message_type": "status_update",
            "sender_type": "user",
            "sender_id": "user123",
            "content_type": "text/plain",
            "content": "Test message content"
        }
        
        # Act - request the endpoint that will trigger the exception
        response = client.post("/api/v1/messages", json=valid_message_data)
        
        # Assert - check status code
        assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        
        # Assert - check response format matches ErrorResponse schema
        response_data = response.json()
        expected_fields = {"status_code", "code", "message", "details"}
        assert set(response_data.keys()) == expected_fields
        
        # Assert - check specific field values
        # Note: Generic exceptions caught by router are re-raised as HTTPException,
        # so they get "HTTP_500" code, not "INTERNAL_SERVER_ERROR"
        assert response_data["status_code"] == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert response_data["code"] == "HTTP_500"
        assert "An unexpected error occurred while processing the message" == response_data["message"]
        assert response_data["details"] is None
    
    @patch('cnts_messaging_svc.services.message_persistence.MessagePersistenceService.persist_message')
    def test_generic_exception_hides_details_when_debug_false(self, mock_persist_message, client):
        """Test that exception details are hidden when app.debug is False."""
        # Configure mock to raise a generic exception (not MessagePersistenceError)
        mock_persist_message.side_effect = RuntimeError("Sensitive error information")
        
        # Valid message data for the request
        valid_message_data = {
            "topic_type": "project",
            "topic_id": "123",
            "message_type": "status_update",
            "sender_type": "user",
            "sender_id": "user123",
            "content_type": "text/plain",
            "content": "Test message content"
        }
        
        # Temporarily set debug to False
        original_debug = app.debug
        app.debug = False
        
        try:
            # Act - request the endpoint that will trigger the exception
            response = client.post("/api/v1/messages", json=valid_message_data)
            
            # Assert - check status code
            assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
            
            # Assert - check response format matches ErrorResponse schema
            response_data = response.json()
            expected_fields = {"status_code", "code", "message", "details"}
            assert set(response_data.keys()) == expected_fields
            
            # Assert - check specific field values
            # Note: Generic exceptions caught by router are re-raised as HTTPException,
            # so they get "HTTP_500" code, not "INTERNAL_SERVER_ERROR"
            assert response_data["status_code"] == status.HTTP_500_INTERNAL_SERVER_ERROR
            assert response_data["code"] == "HTTP_500"
            assert "An unexpected error occurred while processing the message" == response_data["message"]
            assert response_data["details"] is None
            
        finally:
            # Restore original debug setting
            app.debug = original_debug
    
    def test_validation_errors_remain_unchanged(self, client):
        """Test that 422 validation errors are not affected by our global handlers."""
        # Act - send invalid data to trigger validation error
        invalid_data = {
            "topic_type": "",  # Empty string should trigger validation error
            "topic_id": "123",
            "message_type": "test",
            "sender_type": "user",
            "sender_id": "user123",
            "content_type": "text/plain",
            "content": "Test content"
        }
        
        response = client.post("/api/v1/messages", json=invalid_data)
        
        # Assert - check status code
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
        
        # Assert - check that the response still has the default FastAPI validation format
        # (This should NOT be transformed by our global handlers)
        response_data = response.json()
        assert "detail" in response_data  # FastAPI default validation error format
        
        # The response should NOT match our ErrorResponse format
        error_response_fields = {"status_code", "code", "message", "details"}
        assert set(response_data.keys()) != error_response_fields
