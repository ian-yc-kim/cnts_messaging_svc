import pytest
from fastapi import status


class TestAppMetadataAndCors:
    """Tests for FastAPI application metadata and CORS middleware functionality."""
    
    def test_docs_endpoint_accessible(self, client):
        """Test that /docs endpoint is accessible and returns 200 status."""
        response = client.get("/docs")
        assert response.status_code == status.HTTP_200_OK
        assert "text/html" in response.headers.get("content-type", "")
    
    def test_redoc_endpoint_accessible(self, client):
        """Test that /redoc endpoint is accessible and returns 200 status."""
        response = client.get("/redoc")
        assert response.status_code == status.HTTP_200_OK
        assert "text/html" in response.headers.get("content-type", "")
    
    def test_openapi_metadata(self, client):
        """Test that OpenAPI metadata contains correct title, description, and version."""
        response = client.get("/openapi.json")
        assert response.status_code == status.HTTP_200_OK
        
        openapi_data = response.json()
        
        # Verify OpenAPI structure and metadata
        assert "info" in openapi_data
        info = openapi_data["info"]
        
        assert info["title"] == "Messaging Service API"
        assert info["description"] == "REST and WebSocket API for real-time messaging and persistence."
        assert info["version"] == "1.0.0"
    
    def test_cors_preflight_request(self, client):
        """Test CORS preflight OPTIONS request returns correct headers."""
        # Send preflight OPTIONS request with CORS headers
        response = client.options(
            "/api/v1/messages",
            headers={
                "Origin": "http://example.com",
                "Access-Control-Request-Method": "POST",
                "Access-Control-Request-Headers": "Content-Type"
            }
        )
        
        # FastAPI/Starlette typically returns 200 for successful preflight
        assert response.status_code in [status.HTTP_200_OK, status.HTTP_204_NO_CONTENT]
        
        # Check CORS headers are present
        response_headers = response.headers
        
        # Access-Control-Allow-Origin should be either "*" or echo the origin
        allow_origin = response_headers.get("access-control-allow-origin")
        assert allow_origin is not None
        assert allow_origin in ["*", "http://example.com"]
        
        # Access-Control-Allow-Methods should include POST
        allow_methods = response_headers.get("access-control-allow-methods", "")
        assert "POST" in allow_methods.upper()
        
        # Access-Control-Allow-Headers should include content-type
        allow_headers = response_headers.get("access-control-allow-headers", "")
        assert "content-type" in allow_headers.lower()
    
    def test_cors_actual_request(self, client):
        """Test that actual POST request includes CORS headers in response."""
        # Valid message data for POST request
        valid_message_data = {
            "topic_type": "project",
            "topic_id": "123",
            "message_type": "status_update",
            "sender_type": "user",
            "sender_id": "user123",
            "content_type": "text/plain",
            "content": "Test message content"
        }
        
        # Send POST request with Origin header
        response = client.post(
            "/api/v1/messages",
            json=valid_message_data,
            headers={"Origin": "http://example.com"}
        )
        
        # Request should succeed
        assert response.status_code == status.HTTP_200_OK
        
        # Check CORS headers are present in response
        response_headers = response.headers
        
        # Access-Control-Allow-Origin should be either "*" or echo the origin
        allow_origin = response_headers.get("access-control-allow-origin")
        assert allow_origin is not None
        assert allow_origin in ["*", "http://example.com"]
        
        # Access-Control-Allow-Credentials should be present when credentials are allowed
        allow_credentials = response_headers.get("access-control-allow-credentials")
        if allow_credentials is not None:
            assert allow_credentials.lower() == "true"
    
    def test_cors_multiple_origins(self, client):
        """Test CORS behavior with multiple different origins."""
        valid_message_data = {
            "topic_type": "project",
            "topic_id": "cors_test",
            "message_type": "status_update",
            "sender_type": "user",
            "sender_id": "user123",
            "content_type": "text/plain",
            "content": "CORS test message"
        }
        
        test_origins = [
            "http://localhost:3000",
            "https://example.com",
            "http://192.168.1.100:8080",
            "https://app.mydomain.com"
        ]
        
        for origin in test_origins:
            # Test preflight request
            preflight_response = client.options(
                "/api/v1/messages",
                headers={
                    "Origin": origin,
                    "Access-Control-Request-Method": "POST",
                    "Access-Control-Request-Headers": "Content-Type"
                }
            )
            
            assert preflight_response.status_code in [status.HTTP_200_OK, status.HTTP_204_NO_CONTENT]
            
            allow_origin = preflight_response.headers.get("access-control-allow-origin")
            assert allow_origin is not None
            assert allow_origin in ["*", origin]
            
            # Test actual request
            actual_response = client.post(
                "/api/v1/messages",
                json=valid_message_data,
                headers={"Origin": origin}
            )
            
            assert actual_response.status_code == status.HTTP_200_OK
            
            allow_origin_actual = actual_response.headers.get("access-control-allow-origin")
            assert allow_origin_actual is not None
            assert allow_origin_actual in ["*", origin]
    
    def test_cors_without_origin_header(self, client):
        """Test that requests without Origin header still work (not CORS requests)."""
        valid_message_data = {
            "topic_type": "project",
            "topic_id": "no_cors_test",
            "message_type": "status_update",
            "sender_type": "user",
            "sender_id": "user123",
            "content_type": "text/plain",
            "content": "Non-CORS test message"
        }
        
        # Send POST request without Origin header (not a CORS request)
        response = client.post("/api/v1/messages", json=valid_message_data)
        
        # Request should succeed
        assert response.status_code == status.HTTP_200_OK
        
        # Response should still be valid JSON with expected structure
        response_data = response.json()
        assert "message_id" in response_data
        assert response_data["content"] == valid_message_data["content"]
    
    def test_api_versioning_prefix(self, client):
        """Test that API endpoints are correctly prefixed with /api/v1."""
        valid_message_data = {
            "topic_type": "project",
            "topic_id": "version_test",
            "message_type": "status_update",
            "sender_type": "user",
            "sender_id": "user123",
            "content_type": "text/plain",
            "content": "API version test message"
        }
        
        # Correct versioned path should work
        response = client.post("/api/v1/messages", json=valid_message_data)
        assert response.status_code == status.HTTP_200_OK
        
        # Old unversioned path should return 404
        response = client.post("/api/messages", json=valid_message_data)
        assert response.status_code == status.HTTP_404_NOT_FOUND
        
        # Direct path without prefix should return 404
        response = client.post("/messages", json=valid_message_data)
        assert response.status_code == status.HTTP_404_NOT_FOUND
