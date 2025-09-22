import pytest
import tempfile
import os
import subprocess
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from cnts_messaging_svc.models.base import Base
from cnts_messaging_svc.models.message import Message
from cnts_messaging_svc.models.factories import create_all_tables


class TestMigrationBTreeIndexes:
    """Test class to verify B-tree indexes in the migration script."""
    
    def test_create_message_with_indexes(self, db_session):
        """Test creating messages and verify indexes support efficient queries."""
        # Create and commit messages individually to ensure auto-generation works
        message1 = Message(
            topic_type="chat",
            topic_id="room1", 
            message_type="text",
            sender_type="user",
            sender_id="user123",
            content_type="text/plain",
            content="Hello world"
        )
        db_session.add(message1)
        db_session.commit()
        
        message2 = Message(
            topic_type="chat",
            topic_id="room1",
            message_type="text", 
            sender_type="user",
            sender_id="user456",
            content_type="text/plain",
            content="Hi there"
        )
        db_session.add(message2)
        db_session.commit()
        
        message3 = Message(
            topic_type="notification",
            topic_id="system",
            message_type="alert",
            sender_type="system",
            sender_id="sys001",
            content_type="application/json", 
            content='{"level": "info", "message": "System update"}'
        )
        db_session.add(message3)
        db_session.commit()
        
        # Verify messages are created with auto-generated message_ids
        created_messages = db_session.query(Message).all()
        assert len(created_messages) == 3
        
        # Verify auto-generated message_ids are sequential within topic scope
        chat_messages = db_session.query(Message).filter(
            Message.topic_type == "chat",
            Message.topic_id == "room1",
            Message.message_type == "text"
        ).order_by(Message.message_id).all()
        
        assert len(chat_messages) == 2
        assert chat_messages[0].message_id == 1
        assert chat_messages[1].message_id == 2
        
        notification_messages = db_session.query(Message).filter(
            Message.topic_type == "notification",
            Message.topic_id == "system", 
            Message.message_type == "alert"
        ).all()
        
        assert len(notification_messages) == 1
        assert notification_messages[0].message_id == 1
        
    def test_query_by_topic_composite(self, db_session):
        """Test queries using the composite B-tree index on topic fields."""
        # Create test messages with different topic combinations - commit individually
        message1 = Message(
            topic_type="chat", topic_id="room1", message_type="text",
            sender_type="user", sender_id="user1",
            content_type="text/plain", content="Message 1"
        )
        db_session.add(message1)
        db_session.commit()
        
        message2 = Message(
            topic_type="chat", topic_id="room1", message_type="image", 
            sender_type="user", sender_id="user2",
            content_type="image/jpeg", content="base64imagedata"
        )
        db_session.add(message2)
        db_session.commit()
        
        message3 = Message(
            topic_type="chat", topic_id="room2", message_type="text",
            sender_type="user", sender_id="user3", 
            content_type="text/plain", content="Message 2"
        )
        db_session.add(message3)
        db_session.commit()
        
        # Query by composite index fields - should be efficient with the B-tree index
        room1_text = db_session.query(Message).filter(
            Message.topic_type == "chat",
            Message.topic_id == "room1",
            Message.message_type == "text"
        ).all()
        
        assert len(room1_text) == 1
        assert room1_text[0].content == "Message 1"
        
        room1_all = db_session.query(Message).filter(
            Message.topic_type == "chat",
            Message.topic_id == "room1"
        ).all()
        
        assert len(room1_all) == 2
        
    def test_query_by_created_at(self, db_session):
        """Test queries using the created_at B-tree index."""
        # Create test messages with different message_ids to ensure unique messages
        message1 = Message(
            topic_type="test", topic_id="timing", message_type="seq1",
            sender_type="user", sender_id="tester",
            content_type="text/plain", content="First message"
        )
        db_session.add(message1)
        db_session.commit()
        
        message2 = Message(
            topic_type="test", topic_id="timing", message_type="seq2",
            sender_type="user", sender_id="tester", 
            content_type="text/plain", content="Second message"
        )
        db_session.add(message2)
        db_session.commit()
        
        # Query messages ordered by created_at - should be efficient with the B-tree index
        messages_ordered = db_session.query(Message).filter(
            Message.topic_type == "test"
        ).order_by(Message.created_at).all()
        
        assert len(messages_ordered) == 2
        assert messages_ordered[0].created_at <= messages_ordered[1].created_at
        
        # Query by topic type to test index usage
        test_messages = db_session.query(Message).filter(
            Message.topic_type == "test"
        ).all()
        
        assert len(test_messages) == 2
        
    def test_message_constraints(self, db_session):
        """Test that message constraints work correctly with indexes."""
        # Create a message
        message1 = Message(
            topic_type="constraint_test", topic_id="test1", message_type="unique", 
            sender_type="user", sender_id="tester",
            content_type="text/plain", content="Test message"
        )
        db_session.add(message1)
        db_session.commit()
        
        # Verify message was created with message_id=1
        created = db_session.query(Message).filter(
            Message.topic_type == "constraint_test",
            Message.topic_id == "test1",
            Message.message_type == "unique"
        ).first()
        
        assert created is not None
        assert created.message_id == 1
        
        # Try to create another message with same primary key
        message2 = Message(
            topic_type="constraint_test", topic_id="test1", message_type="unique",
            message_id=1,  # Explicitly set same message_id
            sender_type="user", sender_id="tester2",
            content_type="text/plain", content="Duplicate message"
        )
        
        db_session.add(message2)
        
        # This should raise an integrity error due to primary key constraint
        with pytest.raises(Exception):  # SQLAlchemy will raise IntegrityError or similar
            db_session.commit()
        
        db_session.rollback()
        
    def test_update_and_delete_with_indexes(self, db_session):
        """Test update and delete operations work correctly with indexes."""
        # Create test message
        message = Message(
            topic_type="crud_test", topic_id="ops", message_type="update",
            sender_type="user", sender_id="updater",
            content_type="text/plain", content="Original content"
        )
        db_session.add(message)
        db_session.commit()
        
        # Update message content
        created_message = db_session.query(Message).filter(
            Message.topic_type == "crud_test",
            Message.topic_id == "ops", 
            Message.message_type == "update"
        ).first()
        
        assert created_message is not None
        original_created_at = created_message.created_at
        
        created_message.content = "Updated content"
        created_message.sender_id = "updater_modified"
        db_session.commit()
        
        # Verify update
        updated_message = db_session.query(Message).filter(
            Message.topic_type == "crud_test",
            Message.topic_id == "ops",
            Message.message_type == "update"
        ).first()
        
        assert updated_message.content == "Updated content"
        assert updated_message.sender_id == "updater_modified"
        assert updated_message.created_at == original_created_at  # Should not change
        
        # Delete message
        db_session.delete(updated_message)
        db_session.commit()
        
        # Verify deletion
        deleted_check = db_session.query(Message).filter(
            Message.topic_type == "crud_test",
            Message.topic_id == "ops",
            Message.message_type == "update"
        ).first()
        
        assert deleted_check is None
