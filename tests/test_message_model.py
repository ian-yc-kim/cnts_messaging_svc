import pytest
from sqlalchemy import StaticPool, create_engine, select, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from cnts_messaging_svc.models.message import Message
from cnts_messaging_svc.models.factories import (
    MessageDataFactory, 
    create_all_tables, 
    drop_all_tables
)
from cnts_messaging_svc.models.base import Base


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
    
    # Retrieve the message using modern syntax
    stmt = select(Message).where(
        Message.topic_type == "project",
        Message.topic_id == "123",
        Message.message_type == "status_update",
        Message.message_id == 1
    )
    result = db_session.execute(stmt)
    retrieved = result.scalar_one_or_none()
    
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
    
    # Verify both messages exist using modern syntax
    stmt = select(Message).where(
        Message.topic_type == "project",
        Message.topic_id == "123",
        Message.message_type == "status_update"
    ).order_by(Message.message_id)
    result = db_session.execute(stmt)
    messages = result.scalars().all()
    
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
    stmt = select(Message)
    result = db_session.execute(stmt)
    all_messages = result.scalars().all()
    assert len(all_messages) == 4
    
    # Verify each message can be retrieved by its composite primary key
    stmt1 = select(Message).where(
        Message.topic_type == "project",
        Message.topic_id == "123",
        Message.message_type == "status_update",
        Message.message_id == 1
    )
    retrieved1 = db_session.execute(stmt1).scalar_one_or_none()
    
    stmt2 = select(Message).where(
        Message.topic_type == "project",
        Message.topic_id == "456",
        Message.message_type == "status_update",
        Message.message_id == 1
    )
    retrieved2 = db_session.execute(stmt2).scalar_one_or_none()
    
    stmt3 = select(Message).where(
        Message.topic_type == "task",
        Message.topic_id == "123",
        Message.message_type == "status_update",
        Message.message_id == 1
    )
    retrieved3 = db_session.execute(stmt3).scalar_one_or_none()
    
    stmt4 = select(Message).where(
        Message.topic_type == "project",
        Message.topic_id == "123",
        Message.message_type == "comment",
        Message.message_id == 1
    )
    retrieved4 = db_session.execute(stmt4).scalar_one_or_none()
    
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
    
    # Retrieve the message to check created_at using modern syntax
    stmt = select(Message).where(
        Message.topic_type == "project",
        Message.topic_id == "123",
        Message.message_type == "status_update",
        Message.message_id == 1
    )
    result = db_session.execute(stmt)
    retrieved = result.scalar_one_or_none()
    
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
    
    stmt = select(Message).where(
        Message.topic_type == "project",
        Message.topic_id == "123",
        Message.message_type == "status_update",
        Message.message_id == 1
    )
    result = db_session.execute(stmt)
    retrieved = result.scalar_one_or_none()
    
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
    
    # Retrieve and verify the update using modern syntax
    stmt = select(Message).where(
        Message.topic_type == "project",
        Message.topic_id == "123",
        Message.message_type == "status_update",
        Message.message_id == 1
    )
    result = db_session.execute(stmt)
    retrieved = result.scalar_one_or_none()
    
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
    
    # Verify message exists using modern syntax
    stmt = select(Message).where(
        Message.topic_type == "project",
        Message.topic_id == "123",
        Message.message_type == "status_update",
        Message.message_id == 1
    )
    result = db_session.execute(stmt)
    retrieved = result.scalar_one_or_none()
    assert retrieved is not None
    
    # Delete the message
    db_session.delete(retrieved)
    db_session.commit()
    
    # Verify message is deleted
    result = db_session.execute(stmt)
    deleted_check = result.scalar_one_or_none()
    assert deleted_check is None


# Factory Tests
def test_message_data_factory_with_message_id(db_session):
    """Test MessageDataFactory with explicit message_id."""
    message = MessageDataFactory(
        topic_type="test_topic",
        topic_id="456",
        message_type="test_message",
        message_id=10,
        content="Factory test message"
    )
    
    # Verify factory created correct instance
    assert message.topic_type == "test_topic"
    assert message.topic_id == "456"
    assert message.message_type == "test_message"
    assert message.message_id == 10
    assert message.content == "Factory test message"
    assert message.sender_type == "user"  # Default value
    assert message.sender_id == "test_user"  # Default value
    assert message.content_type == "text/plain"  # Default value
    
    # Test insertion
    db_session.add(message)
    db_session.commit()
    
    # Verify in database
    stmt = select(Message).where(
        Message.topic_type == "test_topic",
        Message.topic_id == "456",
        Message.message_type == "test_message",
        Message.message_id == 10
    )
    result = db_session.execute(stmt)
    retrieved = result.scalar_one_or_none()
    assert retrieved is not None
    assert retrieved.content == "Factory test message"


def test_message_data_factory_without_message_id(db_session):
    """Test MessageDataFactory with auto-generated message_id."""
    message = MessageDataFactory(
        topic_type="auto_topic",
        topic_id="789",
        message_type="auto_message",
        # message_id intentionally omitted
        content="Auto-generated ID test"
    )
    
    # Verify factory created instance without message_id
    assert message.topic_type == "auto_topic"
    assert message.topic_id == "789"
    assert message.message_type == "auto_message"
    assert message.message_id is None  # Should be None initially
    assert message.content == "Auto-generated ID test"
    
    # Test insertion - event listener should set message_id
    db_session.add(message)
    db_session.commit()
    
    # Verify message_id was auto-generated
    assert message.message_id is not None
    assert message.message_id == 1  # Should be 1 for first message in scope


def test_message_data_factory_with_kwargs(db_session):
    """Test MessageDataFactory with additional kwargs."""
    message = MessageDataFactory(
        topic_type="kwargs_topic",
        sender_type="system",
        content_type="application/json",
        custom_field="ignored"  # This should be ignored as it's not a valid Message field
    )
    
    assert message.topic_type == "kwargs_topic"
    assert message.topic_id == "123"  # Default value
    assert message.sender_type == "system"  # Overridden
    assert message.content_type == "application/json"  # Overridden


# Event Listener Tests
def test_before_insert_listener_auto_generates_message_id(db_session):
    """Test that message_id is auto-generated when None."""
    # Create message without message_id
    message = Message(
        topic_type="auto_gen",
        topic_id="test",
        message_type="test_msg",
        sender_type="user",
        sender_id="user1",
        content_type="text/plain",
        content="Auto-generated ID"
    )
    
    # message_id should be None initially
    assert message.message_id is None
    
    # Insert into database - event listener should set message_id
    db_session.add(message)
    db_session.commit()
    
    # Verify message_id was auto-generated
    assert message.message_id is not None
    assert message.message_id == 1


def test_before_insert_listener_preserves_explicit_message_id(db_session):
    """Test that explicit message_id is not overwritten."""
    message = Message(
        topic_type="explicit_id",
        topic_id="test",
        message_type="test_msg",
        message_id=42,  # Explicit value
        sender_type="user",
        sender_id="user1",
        content_type="text/plain",
        content="Explicit ID"
    )
    
    # Insert into database
    db_session.add(message)
    db_session.commit()
    
    # Verify explicit message_id was preserved
    assert message.message_id == 42


def test_before_insert_listener_scope_isolation(db_session):
    """Test message_id generation across different scopes."""
    # Create messages in different scopes - insert one by one to avoid batch conflicts
    message1 = Message(
        topic_type="scope1", topic_id="1", message_type="type1",
        sender_type="user", sender_id="user1",
        content_type="text/plain", content="Scope 1 Message 1"
    )
    db_session.add(message1)
    db_session.commit()
    
    message2 = Message(
        topic_type="scope1", topic_id="1", message_type="type1",
        sender_type="user", sender_id="user1",
        content_type="text/plain", content="Scope 1 Message 2"
    )
    db_session.add(message2)
    db_session.commit()
    
    message3 = Message(
        topic_type="scope2", topic_id="1", message_type="type1",  # Different topic_type
        sender_type="user", sender_id="user1",
        content_type="text/plain", content="Scope 2 Message 1"
    )
    db_session.add(message3)
    db_session.commit()
    
    message4 = Message(
        topic_type="scope1", topic_id="2", message_type="type1",  # Different topic_id
        sender_type="user", sender_id="user1",
        content_type="text/plain", content="Scope 1 Different ID"
    )
    db_session.add(message4)
    db_session.commit()
    
    message5 = Message(
        topic_type="scope1", topic_id="1", message_type="type2",  # Different message_type
        sender_type="user", sender_id="user1",
        content_type="text/plain", content="Scope 1 Different Type"
    )
    db_session.add(message5)
    db_session.commit()
    
    # Verify message_id generation within scopes
    assert message1.message_id == 1  # First in scope1-1-type1
    assert message2.message_id == 2  # Second in scope1-1-type1
    assert message3.message_id == 1  # First in scope2-1-type1
    assert message4.message_id == 1  # First in scope1-2-type1
    assert message5.message_id == 1  # First in scope1-1-type2


def test_before_insert_listener_sequential_generation(db_session):
    """Test sequential message_id generation within same scope."""
    # Create multiple messages in same scope
    messages = []
    for i in range(5):
        message = Message(
            topic_type="sequential",
            topic_id="test",
            message_type="sequence_test",
            sender_type="user",
            sender_id=f"user{i}",
            content_type="text/plain",
            content=f"Sequential message {i+1}"
        )
        messages.append(message)
        db_session.add(message)
        db_session.commit()  # Commit each individually to test sequence
    
    # Verify sequential IDs
    for i, message in enumerate(messages):
        assert message.message_id == i + 1


# Table Management Function Tests
def test_create_all_tables_function():
    """Test create_all_tables with new in-memory engine."""
    # Create a new in-memory engine
    engine = create_engine(
        'sqlite:///:memory:',
        connect_args={'check_same_thread': False},
        poolclass=StaticPool
    )
    
    # Tables should not exist initially
    inspector = engine.dialect.get_table_names(engine.connect())
    assert 'messages' not in inspector
    
    # Create tables using factory function
    create_all_tables(engine)
    
    # Verify tables were created
    with engine.connect() as conn:
        inspector = engine.dialect.get_table_names(conn)
        assert 'messages' in inspector


def test_drop_all_tables_function():
    """Test drop_all_tables function."""
    # Create a new in-memory engine with tables
    engine = create_engine(
        'sqlite:///:memory:',
        connect_args={'check_same_thread': False},
        poolclass=StaticPool
    )
    
    # Create tables first
    create_all_tables(engine)
    
    # Verify tables exist
    with engine.connect() as conn:
        inspector = engine.dialect.get_table_names(conn)
        assert 'messages' in inspector
    
    # Drop tables using factory function
    drop_all_tables(engine)
    
    # Verify tables were dropped
    with engine.connect() as conn:
        inspector = engine.dialect.get_table_names(conn)
        assert 'messages' not in inspector


def test_table_management_functions_integration():
    """Test create and drop table functions work together."""
    # Create a new in-memory engine
    engine = create_engine(
        'sqlite:///:memory:',
        connect_args={'check_same_thread': False},
        poolclass=StaticPool
    )
    
    # Create tables
    create_all_tables(engine)
    
    # Create a session and insert test data
    TestSession = sessionmaker(bind=engine)
    session = TestSession()
    
    try:
        message = MessageDataFactory(
            topic_type="integration",
            message_id=1
        )
        session.add(message)
        session.commit()
        
        # Verify data was inserted
        stmt = select(Message).where(Message.topic_type == "integration")
        result = session.execute(stmt)
        retrieved = result.scalar_one_or_none()
        assert retrieved is not None
        
    finally:
        session.close()
    
    # Drop tables
    drop_all_tables(engine)
    
    # Verify tables are gone
    with engine.connect() as conn:
        inspector = engine.dialect.get_table_names(conn)
        assert 'messages' not in inspector
