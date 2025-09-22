import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy import select
from unittest.mock import Mock, patch
from pydantic import ValidationError
from datetime import datetime, timezone, timedelta

from cnts_messaging_svc.services.message_persistence import MessagePersistenceService, MessagePersistenceError
from cnts_messaging_svc.schemas.message import MessageCreate
from cnts_messaging_svc.models.message import Message


class TestMessagePersistenceService:
    """Test suite for MessagePersistenceService."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.service = MessagePersistenceService()
        self.valid_message_data = MessageCreate(
            topic_type="project",
            topic_id="123",
            message_type="status_update",
            sender_type="user",
            sender_id="user123",
            content_type="text/plain",
            content="Test message content"
        )
    
    def test_successful_message_persistence(self, db_session):
        """Test successful message persistence with valid MessageCreate data."""
        # Act
        result = self.service.persist_message(db_session, self.valid_message_data)
        
        # Assert
        assert isinstance(result, Message)
        assert result.topic_type == self.valid_message_data.topic_type
        assert result.topic_id == self.valid_message_data.topic_id
        assert result.message_type == self.valid_message_data.message_type
        assert result.sender_type == self.valid_message_data.sender_type
        assert result.sender_id == self.valid_message_data.sender_id
        assert result.content_type == self.valid_message_data.content_type
        assert result.content == self.valid_message_data.content
        
        # Verify auto-generated fields
        assert result.message_id is not None
        assert result.message_id == 1  # First message in this scope
        assert result.created_at is not None
        
        # Verify message was actually persisted to database
        stmt = select(Message).where(
            Message.topic_type == self.valid_message_data.topic_type,
            Message.topic_id == self.valid_message_data.topic_id,
            Message.message_type == self.valid_message_data.message_type,
            Message.message_id == result.message_id
        )
        db_result = db_session.execute(stmt)
        retrieved = db_result.scalar_one_or_none()
        
        assert retrieved is not None
        assert retrieved.content == self.valid_message_data.content
    
    def test_message_id_auto_generation(self, db_session):
        """Test that message_id is auto-generated correctly."""
        # Create first message
        result1 = self.service.persist_message(db_session, self.valid_message_data)
        assert result1.message_id == 1
        
        # Create second message in same scope
        message_data2 = MessageCreate(
            topic_type="project",
            topic_id="123",
            message_type="status_update",
            sender_type="user",
            sender_id="user456",
            content_type="text/plain",
            content="Second test message"
        )
        result2 = self.service.persist_message(db_session, message_data2)
        assert result2.message_id == 2
        
        # Create message in different scope - should reset to 1
        message_data3 = MessageCreate(
            topic_type="task",  # Different topic_type
            topic_id="123",
            message_type="status_update",
            sender_type="user",
            sender_id="user789",
            content_type="text/plain",
            content="Different scope message"
        )
        result3 = self.service.persist_message(db_session, message_data3)
        assert result3.message_id == 1  # New scope starts at 1
    
    def test_created_at_auto_population(self, db_session):
        """Test that created_at is populated after commit."""
        # Record time before persistence (with buffer for timestamp precision)
        before_time = datetime.now(timezone.utc).replace(microsecond=0)  # Remove microseconds for comparison
        
        # Act
        result = self.service.persist_message(db_session, self.valid_message_data)
        
        # Record time after persistence (with buffer)
        after_time = datetime.now(timezone.utc).replace(microsecond=0) + timedelta(seconds=1)
        
        # Assert created_at is populated and within reasonable time range
        assert result.created_at is not None
        
        # Convert created_at to UTC for comparison (handle both timezone-aware and naive datetimes)
        if result.created_at.tzinfo is None:
            created_at_utc = result.created_at.replace(tzinfo=timezone.utc)
        else:
            created_at_utc = result.created_at.astimezone(timezone.utc)
        
        # Remove microseconds for fair comparison since DB may not store them
        created_at_utc = created_at_utc.replace(microsecond=0)
        
        # Verify timestamp is reasonable (within a few seconds)
        time_diff = abs((created_at_utc - before_time).total_seconds())
        assert time_diff < 5, f"Timestamp difference too large: {time_diff} seconds"
    
    def test_integrity_error_handling_duplicate_key(self, db_session):
        """Test IntegrityError handling for duplicate composite primary key."""
        # Create first message
        result1 = self.service.persist_message(db_session, self.valid_message_data)
        assert result1.message_id == 1
        
        # Try to create another message with same composite key by manually setting message_id
        # This simulates a scenario where the auto-increment fails or is bypassed
        message_with_duplicate_key = Message(
            topic_type=self.valid_message_data.topic_type,
            topic_id=self.valid_message_data.topic_id,
            message_type=self.valid_message_data.message_type,
            message_id=1,  # Same as first message
            sender_type="user",
            sender_id="different_user",
            content_type="text/plain",
            content="Duplicate key message"
        )
        
        # Manually add to session to bypass service logic and trigger IntegrityError
        db_session.add(message_with_duplicate_key)
        
        with pytest.raises(IntegrityError):
            db_session.commit()
        
        # Reset session for clean state
        db_session.rollback()
    
    def test_service_error_handling_with_mock_integrity_error(self, db_session):
        """Test MessagePersistenceError is raised when IntegrityError occurs."""
        # Mock db_session.commit to raise IntegrityError
        with patch.object(db_session, 'commit', side_effect=IntegrityError("", "", "")):
            with pytest.raises(MessagePersistenceError) as exc_info:
                self.service.persist_message(db_session, self.valid_message_data)
            
            # Verify error message contains useful information
            assert "Failed to persist message due to database constraint violation" in str(exc_info.value)
    
    def test_service_error_handling_with_mock_generic_exception(self, db_session):
        """Test MessagePersistenceError is raised for generic database errors."""
        # Mock db_session.commit to raise a generic exception
        with patch.object(db_session, 'commit', side_effect=Exception("Database connection lost")):
            with pytest.raises(MessagePersistenceError) as exc_info:
                self.service.persist_message(db_session, self.valid_message_data)
            
            # Verify error message contains useful information
            assert "Failed to persist message due to unexpected error" in str(exc_info.value)
    
    def test_transaction_rollback_on_error(self, db_session):
        """Test that session rollback is called on errors."""
        # Mock rollback to track if it's called
        rollback_mock = Mock()
        db_session.rollback = rollback_mock
        
        # Mock commit to raise an exception
        with patch.object(db_session, 'commit', side_effect=IntegrityError("", "", "")):
            with pytest.raises(MessagePersistenceError):
                self.service.persist_message(db_session, self.valid_message_data)
        
        # Verify rollback was called
        rollback_mock.assert_called_once()
    
    def test_refresh_called_after_commit(self, db_session):
        """Test that refresh is called to load auto-generated fields."""
        # Mock refresh to track if it's called
        refresh_mock = Mock()
        db_session.refresh = refresh_mock
        
        # Act
        result = self.service.persist_message(db_session, self.valid_message_data)
        
        # Verify refresh was called with the message object
        refresh_mock.assert_called_once_with(result)
    
    @patch('cnts_messaging_svc.services.message_persistence.logging')
    def test_successful_persistence_logging(self, mock_logging, db_session):
        """Test that successful persistence is logged at INFO level."""
        # Act
        result = self.service.persist_message(db_session, self.valid_message_data)
        
        # Verify INFO log was called
        mock_logging.info.assert_called_once()
        log_message = mock_logging.info.call_args[0][0]
        assert "Successfully persisted message" in log_message
        assert self.valid_message_data.topic_type in log_message
        assert self.valid_message_data.topic_id in log_message
        assert self.valid_message_data.message_type in log_message
        assert str(result.message_id) in log_message
    
    @patch('cnts_messaging_svc.services.message_persistence.logging')
    def test_error_logging_on_integrity_error(self, mock_logging, db_session):
        """Test that IntegrityError is logged at ERROR level with exc_info."""
        # Mock commit to raise IntegrityError
        with patch.object(db_session, 'commit', side_effect=IntegrityError("", "", "")):
            with pytest.raises(MessagePersistenceError):
                self.service.persist_message(db_session, self.valid_message_data)
        
        # Verify ERROR log was called with exc_info=True
        mock_logging.error.assert_called()
        args, kwargs = mock_logging.error.call_args
        assert "Database integrity error during message persistence" in args[0]
        assert kwargs.get('exc_info') is True
    
    @patch('cnts_messaging_svc.services.message_persistence.logging')
    def test_error_logging_on_generic_error(self, mock_logging, db_session):
        """Test that generic errors are logged at ERROR level with exc_info."""
        # Mock commit to raise generic exception
        with patch.object(db_session, 'commit', side_effect=Exception("Test error")):
            with pytest.raises(MessagePersistenceError):
                self.service.persist_message(db_session, self.valid_message_data)
        
        # Verify ERROR log was called with exc_info=True
        mock_logging.error.assert_called()
        args, kwargs = mock_logging.error.call_args
        assert "Unexpected error during message persistence" in args[0]
        assert kwargs.get('exc_info') is True
    
    def test_multiple_messages_same_topic_scope(self, db_session):
        """Test persisting multiple messages in the same topic scope."""
        messages = []
        
        # Create 5 messages in same scope
        for i in range(5):
            message_data = MessageCreate(
                topic_type="project",
                topic_id="test_scope",
                message_type="batch_test",
                sender_type="user",
                sender_id=f"user{i}",
                content_type="text/plain",
                content=f"Batch message {i+1}"
            )
            result = self.service.persist_message(db_session, message_data)
            messages.append(result)
        
        # Verify sequential message_id generation
        for i, message in enumerate(messages):
            assert message.message_id == i + 1
            assert message.content == f"Batch message {i+1}"
        
        # Verify all messages are persisted in database
        stmt = select(Message).where(
            Message.topic_type == "project",
            Message.topic_id == "test_scope",
            Message.message_type == "batch_test"
        ).order_by(Message.message_id)
        
        db_result = db_session.execute(stmt)
        persisted_messages = db_result.scalars().all()
        
        assert len(persisted_messages) == 5
        for i, message in enumerate(persisted_messages):
            assert message.message_id == i + 1
            assert message.content == f"Batch message {i+1}"
    
    def test_message_scope_isolation_integration(self, db_session):
        """Test that message_id generation is properly isolated across different scopes."""
        # Create messages in different scopes
        scope_combinations = [
            ("project", "123", "status"),
            ("project", "456", "status"),  # Different topic_id
            ("task", "123", "status"),     # Different topic_type
            ("project", "123", "comment"), # Different message_type
        ]
        
        results = []
        for topic_type, topic_id, message_type in scope_combinations:
            message_data = MessageCreate(
                topic_type=topic_type,
                topic_id=topic_id,
                message_type=message_type,
                sender_type="user",
                sender_id="test_user",
                content_type="text/plain",
                content=f"Message in {topic_type}-{topic_id}-{message_type}"
            )
            result = self.service.persist_message(db_session, message_data)
            results.append(result)
        
        # All messages should have message_id = 1 (first in their respective scopes)
        for result in results:
            assert result.message_id == 1
        
        # Create second message in first scope
        second_message = MessageCreate(
            topic_type="project",
            topic_id="123",
            message_type="status",
            sender_type="user",
            sender_id="test_user2",
            content_type="text/plain",
            content="Second message in first scope"
        )
        second_result = self.service.persist_message(db_session, second_message)
        assert second_result.message_id == 2  # Second in this scope


class TestMessageCreateValidation:
    """Test Pydantic validation for MessageCreate schema."""
    
    def test_valid_message_creation(self):
        """Test creating MessageCreate with all valid fields."""
        message = MessageCreate(
            topic_type="project",
            topic_id="123",
            message_type="status_update",
            sender_type="user",
            sender_id="user123",
            content_type="text/plain",
            content="Valid message content"
        )
        
        assert message.topic_type == "project"
        assert message.topic_id == "123"
        assert message.message_type == "status_update"
        assert message.sender_type == "user"
        assert message.sender_id == "user123"
        assert message.content_type == "text/plain"
        assert message.content == "Valid message content"
    
    def test_empty_topic_type_validation_error(self):
        """Test validation error for empty topic_type."""
        with pytest.raises(ValidationError) as exc_info:
            MessageCreate(
                topic_type="",  # Empty string
                topic_id="123",
                message_type="status_update",
                sender_type="user",
                sender_id="user123",
                content_type="text/plain",
                content="Test content"
            )
        
        # Verify error details
        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]['loc'] == ('topic_type',)
        assert 'at least 1 character' in str(errors[0]['msg'])
    
    def test_too_long_topic_type_validation_error(self):
        """Test validation error for topic_type exceeding max_length."""
        long_topic_type = "a" * 256  # Exceeds max_length=255
        
        with pytest.raises(ValidationError) as exc_info:
            MessageCreate(
                topic_type=long_topic_type,
                topic_id="123",
                message_type="status_update",
                sender_type="user",
                sender_id="user123",
                content_type="text/plain",
                content="Test content"
            )
        
        # Verify error details
        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]['loc'] == ('topic_type',)
        assert 'at most 255 characters' in str(errors[0]['msg'])
    
    def test_empty_content_validation_error(self):
        """Test validation error for empty content."""
        with pytest.raises(ValidationError) as exc_info:
            MessageCreate(
                topic_type="project",
                topic_id="123",
                message_type="status_update",
                sender_type="user",
                sender_id="user123",
                content_type="text/plain",
                content=""  # Empty content
            )
        
        # Verify error details
        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]['loc'] == ('content',)
        assert 'at least 1 character' in str(errors[0]['msg'])
    
    def test_missing_required_field_validation_error(self):
        """Test validation error for missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            MessageCreate(
                topic_type="project",
                topic_id="123",
                message_type="status_update",
                sender_type="user",
                # sender_id missing
                content_type="text/plain",
                content="Test content"
            )
        
        # Verify error details
        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]['loc'] == ('sender_id',)
        assert 'Field required' in str(errors[0]['msg'])
    
    def test_all_fields_max_length_validation(self):
        """Test that all string fields respect their max_length constraints."""
        # Test each field with exactly 255 characters (should pass)
        valid_255_char_string = "a" * 255
        
        message = MessageCreate(
            topic_type=valid_255_char_string,
            topic_id=valid_255_char_string,
            message_type=valid_255_char_string,
            sender_type=valid_255_char_string,
            sender_id=valid_255_char_string,
            content_type=valid_255_char_string,
            content="Valid content"  # Content only has min_length constraint
        )
        
        # Should not raise validation error
        assert len(message.topic_type) == 255
        assert len(message.topic_id) == 255
        assert len(message.message_type) == 255
        assert len(message.sender_type) == 255
        assert len(message.sender_id) == 255
        assert len(message.content_type) == 255
    
    def test_multiple_validation_errors(self):
        """Test that multiple validation errors are reported together."""
        with pytest.raises(ValidationError) as exc_info:
            MessageCreate(
                topic_type="",  # Empty (invalid)
                topic_id="123",
                message_type="",  # Empty (invalid)
                sender_type="user",
                sender_id="user123",
                content_type="text/plain",
                content=""  # Empty (invalid)
            )
        
        # Verify multiple errors are reported
        errors = exc_info.value.errors()
        assert len(errors) == 3
        
        # Check that all invalid fields are reported
        error_fields = [error['loc'][0] for error in errors]
        assert 'topic_type' in error_fields
        assert 'message_type' in error_fields
        assert 'content' in error_fields
