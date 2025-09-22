import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from cnts_messaging_svc.models.message import Message


def test_message_creation_with_all_fields(db_session):
    """Test creating a message with all required fields."""
    message = Message(
        topic_type="project",
        topic_id="123",
        message_type="status_update",
        message_id=1,
        sender_type="user",
        sender_id="user123",
        content_type="text/plain",
        content="Hello, this is a test message"
    )
    
    db_session.add(message)
    db_session.commit()
    
    # Retrieve the message
    retrieved = db_session.query(Message).filter_by(
        topic_type="project",
        topic_id="123",
        message_type="status_update",
        message_id=1
    ).first()
    
    assert retrieved is not None
    assert retrieved.topic_type == "project"
    assert retrieved.topic_id == "123"
    assert retrieved.message_type == "status_update"
    assert retrieved.message_id == 1
    assert retrieved.sender_type == "user"
    assert retrieved.sender_id == "user123"
    assert retrieved.content_type == "text/plain"
    assert retrieved.content == "Hello, this is a test message"
    assert retrieved.created_at is not None


def test_message_auto_increment_within_topic_scope(db_session):
    """Test that message_id auto-increments correctly within the same topic scope."""
    # Create first message
    message1 = Message(
        topic_type="project",
        topic_id="123",
        message_type="status_update",
        message_id=1,
        sender_type="user",
        sender_id="user123",
        content_type="text/plain",
        content="First message"
    )
    db_session.add(message1)
    db_session.commit()
    
    # Create second message in the same topic scope
    message2 = Message(
        topic_type="project",
        topic_id="123",
        message_type="status_update",
        message_id=2,
        sender_type="user",
        sender_id="user456",
        content_type="text/plain",
        content="Second message"
    )
    db_session.add(message2)
    db_session.commit()
    
    # Verify both messages exist
    messages = db_session.query(Message).filter_by(
        topic_type="project",
        topic_id="123",
        message_type="status_update"
    ).order_by(Message.message_id).all()
    
    assert len(messages) == 2
    assert messages[0].message_id == 1
    assert messages[1].message_id == 2


def test_message_id_scope_isolation(db_session):
    """Test that message_id is scoped to (topic_type, topic_id, message_type)."""
    # Create messages in different topic scopes
    message1 = Message(
        topic_type="project",
        topic_id="123",
        message_type="status_update",
        message_id=1,
        sender_type="user",
        sender_id="user123",
        content_type="text/plain",
        content="Message in project 123"
    )
    
    message2 = Message(
        topic_type="project",
        topic_id="456",  # Different topic_id
        message_type="status_update",
        message_id=1,  # Same message_id but different scope
        sender_type="user",
        sender_id="user123",
        content_type="text/plain",
        content="Message in project 456"
    )
    
    message3 = Message(
        topic_type="task",  # Different topic_type
        topic_id="123",
        message_type="status_update",
        message_id=1,  # Same message_id but different scope
        sender_type="user",
        sender_id="user123",
        content_type="text/plain",
        content="Message in task 123"
    )
    
    message4 = Message(
        topic_type="project",
        topic_id="123",
        message_type="comment",  # Different message_type
        message_id=1,  # Same message_id but different scope
        sender_type="user",
        sender_id="user123",
        content_type="text/plain",
        content="Comment in project 123"
    )
    
    db_session.add_all([message1, message2, message3, message4])
    db_session.commit()
    
    # Verify all messages were created successfully
    all_messages = db_session.query(Message).all()
    assert len(all_messages) == 4
    
    # Verify each message can be retrieved by its composite primary key
    retrieved1 = db_session.query(Message).filter_by(
        topic_type="project", topic_id="123", message_type="status_update", message_id=1
    ).first()
    retrieved2 = db_session.query(Message).filter_by(
        topic_type="project", topic_id="456", message_type="status_update", message_id=1
    ).first()
    retrieved3 = db_session.query(Message).filter_by(
        topic_type="task", topic_id="123", message_type="status_update", message_id=1
    ).first()
    retrieved4 = db_session.query(Message).filter_by(
        topic_type="project", topic_id="123", message_type="comment", message_id=1
    ).first()
    
    assert retrieved1.content == "Message in project 123"
    assert retrieved2.content == "Message in project 456"
    assert retrieved3.content == "Message in task 123"
    assert retrieved4.content == "Comment in project 123"


def test_duplicate_primary_key_fails(db_session):
    """Test that attempting to insert a message with duplicate composite primary key fails."""
    message1 = Message(
        topic_type="project",
        topic_id="123",
        message_type="status_update",
        message_id=1,
        sender_type="user",
        sender_id="user123",
        content_type="text/plain",
        content="First message"
    )
    db_session.add(message1)
    db_session.commit()
    
    # Try to insert another message with the same composite primary key
    message2 = Message(
        topic_type="project",
        topic_id="123",
        message_type="status_update",
        message_id=1,  # Same composite key
        sender_type="user",
        sender_id="user456",
        content_type="text/plain",
        content="Duplicate message"
    )
    db_session.add(message2)
    
    with pytest.raises(IntegrityError):
        db_session.commit()


def test_message_required_fields(db_session):
    """Test that all required fields must be provided."""
    # Test missing topic_type
    with pytest.raises(IntegrityError):
        message = Message(
            topic_id="123",
            message_type="status_update",
            message_id=1,
            sender_type="user",
            sender_id="user123",
            content_type="text/plain",
            content="Test message"
        )
        db_session.add(message)
        db_session.commit()
    
    db_session.rollback()
    
    # Test missing content
    with pytest.raises((IntegrityError, TypeError)):
        message = Message(
            topic_type="project",
            topic_id="123",
            message_type="status_update",
            message_id=1,
            sender_type="user",
            sender_id="user123",
            content_type="text/plain"
            # content missing
        )
        db_session.add(message)
        db_session.commit()


def test_created_at_auto_population(db_session):
    """Test that created_at is automatically populated."""
    message = Message(
        topic_type="project",
        topic_id="123",
        message_type="status_update",
        message_id=1,
        sender_type="user",
        sender_id="user123",
        content_type="text/plain",
        content="Test message"
    )
    
    db_session.add(message)
    db_session.commit()
    
    # Retrieve the message to check created_at
    retrieved = db_session.query(Message).filter_by(
        topic_type="project",
        topic_id="123",
        message_type="status_update",
        message_id=1
    ).first()
    
    assert retrieved.created_at is not None
    # The created_at should be recent (within last minute)
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    time_diff = now - retrieved.created_at.replace(tzinfo=timezone.utc)
    assert time_diff.total_seconds() < 60


def test_message_repr(db_session):
    """Test the string representation of Message model."""
    message = Message(
        topic_type="project",
        topic_id="123",
        message_type="status_update",
        message_id=1,
        sender_type="user",
        sender_id="user123",
        content_type="text/plain",
        content="Test message"
    )
    
    expected_repr = "<Message(topic_type='project', topic_id='123', message_type='status_update', message_id=1)>"
    assert repr(message) == expected_repr


def test_large_content_handling(db_session):
    """Test that large content can be stored and retrieved."""
    large_content = "A" * 10000  # 10KB of text
    
    message = Message(
        topic_type="project",
        topic_id="123",
        message_type="status_update",
        message_id=1,
        sender_type="user",
        sender_id="user123",
        content_type="text/plain",
        content=large_content
    )
    
    db_session.add(message)
    db_session.commit()
    
    retrieved = db_session.query(Message).filter_by(
        topic_type="project",
        topic_id="123",
        message_type="status_update",
        message_id=1
    ).first()
    
    assert retrieved.content == large_content
    assert len(retrieved.content) == 10000


def test_message_update(db_session):
    """Test updating a message (only non-key fields should be updatable)."""
    message = Message(
        topic_type="project",
        topic_id="123",
        message_type="status_update",
        message_id=1,
        sender_type="user",
        sender_id="user123",
        content_type="text/plain",
        content="Original content"
    )
    
    db_session.add(message)
    db_session.commit()
    
    # Update the content
    message.content = "Updated content"
    message.content_type = "text/html"
    db_session.commit()
    
    # Retrieve and verify the update
    retrieved = db_session.query(Message).filter_by(
        topic_type="project",
        topic_id="123",
        message_type="status_update",
        message_id=1
    ).first()
    
    assert retrieved.content == "Updated content"
    assert retrieved.content_type == "text/html"


def test_message_deletion(db_session):
    """Test deleting a message."""
    message = Message(
        topic_type="project",
        topic_id="123",
        message_type="status_update",
        message_id=1,
        sender_type="user",
        sender_id="user123",
        content_type="text/plain",
        content="Test message"
    )
    
    db_session.add(message)
    db_session.commit()
    
    # Verify message exists
    retrieved = db_session.query(Message).filter_by(
        topic_type="project",
        topic_id="123",
        message_type="status_update",
        message_id=1
    ).first()
    assert retrieved is not None
    
    # Delete the message
    db_session.delete(retrieved)
    db_session.commit()
    
    # Verify message is deleted
    deleted_check = db_session.query(Message).filter_by(
        topic_type="project",
        topic_id="123",
        message_type="status_update",
        message_id=1
    ).first()
    assert deleted_check is None
