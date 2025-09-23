from pydantic import BaseModel, Field
from typing import Optional


class ErrorResponse(BaseModel):
    """Standardized error response model for the messaging service API."""
    
    status_code: int = Field(..., description="HTTP status code")
    code: str = Field(..., description="Application-specific error code")
    message: str = Field(..., description="Human-readable error message")
    details: Optional[str] = Field(None, description="Additional details about the error")
