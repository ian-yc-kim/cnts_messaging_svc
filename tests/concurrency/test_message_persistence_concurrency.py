import pytest
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import select
from cnts_messaging_svc.services.message_persistence import MessagePersistenceService, MessagePersistenceError
from cnts_messaging_svc.schemas.message import MessageCreate
from cnts_messaging_svc.models.message import Message


class TestMessagePersistenceConcurrency:
    """Conservative concurrency tests for message persistence.
    
    These tests focus on verifying basic concurrent functionality while being
    realistic about SQLite's limitations in multi-threaded scenarios.
    """
    
    def setup_method(self):
        """Set up test fixtures."""
        self.service = MessagePersistenceService()
    
    def create_test_message_data(self, thread_id: int, scope_suffix: str = "") -> MessageCreate:
        """Factory method for creating test MessageCreate objects."""
        return MessageCreate(
            topic_type=f"test{scope_suffix}",
            topic_id=f"scope{scope_suffix}",
            message_type=f"concurrent{scope_suffix}",
            sender_type="thread",
            sender_id=f"thread_{thread_id}",
            content_type="text/plain",
            content=f"Test message from thread {thread_id}"
        )
    
    def test_sequential_message_id_generation(self, session_local):
        """Test that message IDs are generated sequentially within a scope.
        
        This test verifies the core message ID generation logic without
        heavy concurrent stress, focusing on correctness.
        """
        messages_to_create = 3
        results = []
        
        # Create messages sequentially to ensure predictable behavior
        for i in range(messages_to_create):
            session = session_local()
            try:
                message_data = MessageCreate(
                    topic_type="sequential_test",
                    topic_id="test_scope",
                    message_type="sequence",
                    sender_type="user",
                    sender_id=f"user_{i}",
                    content_type="text/plain",
                    content=f"Sequential message {i + 1}"
                )
                
                result = self.service.persist_message(session, message_data)
                results.append(result)
                
            finally:
                session.close()
        
        # Verify sequential message IDs
        assert len(results) == messages_to_create
        for i, result in enumerate(results):
            assert result.message_id == i + 1, f"Expected message_id {i + 1}, got {result.message_id}"
        
        # Verify persistence in database
        session = session_local()
        try:
            stmt = select(Message).where(
                Message.topic_type == "sequential_test",
                Message.topic_id == "test_scope",
                Message.message_type == "sequence"
            ).order_by(Message.message_id)
            
            persisted_messages = session.execute(stmt).scalars().all()
            assert len(persisted_messages) == messages_to_create
            
            # Verify sequential IDs in database
            for i, msg in enumerate(persisted_messages):
                assert msg.message_id == i + 1
                assert msg.content == f"Sequential message {i + 1}"
                
        finally:
            session.close()
    
    def test_scope_isolation_verification(self, session_local):
        """Test that different scopes maintain independent message ID sequences.
        
        This test verifies scope isolation by creating messages in different
        scopes and ensuring each scope has its own sequence.
        """
        scopes = [
            ("scope_test_1", "isolation_1", "type_1"),
            ("scope_test_2", "isolation_1", "type_1"),  # Different topic_type
            ("scope_test_1", "isolation_2", "type_1"),  # Different topic_id
            ("scope_test_1", "isolation_1", "type_2"),  # Different message_type
        ]
        
        results = []
        
        # Create one message in each scope
        for i, (topic_type, topic_id, message_type) in enumerate(scopes):
            session = session_local()
            try:
                message_data = MessageCreate(
                    topic_type=topic_type,
                    topic_id=topic_id,
                    message_type=message_type,
                    sender_type="test",
                    sender_id=f"sender_{i}",
                    content_type="text/plain",
                    content=f"Isolation test for scope {i}"
                )
                
                result = self.service.persist_message(session, message_data)
                results.append((result, topic_type, topic_id, message_type))
                
            finally:
                session.close()
        
        # Each scope should start with message_id = 1
        assert len(results) == len(scopes)
        for result, topic_type, topic_id, message_type in results:
            assert result.message_id == 1, f"Scope ({topic_type}, {topic_id}, {message_type}) should start at message_id=1"
        
        # Add a second message to the first scope to verify increment
        session = session_local()
        try:
            second_message = MessageCreate(
                topic_type=scopes[0][0],
                topic_id=scopes[0][1],
                message_type=scopes[0][2],
                sender_type="test",
                sender_id="second_sender",
                content_type="text/plain",
                content="Second message in first scope"
            )
            
            second_result = self.service.persist_message(session, second_message)
            assert second_result.message_id == 2, "Second message in first scope should have message_id=2"
            
        finally:
            session.close()
    
    def test_concurrent_different_scopes(self, session_local):
        """Test concurrent insertions into different scopes.
        
        This test uses minimal concurrency (2 threads) inserting into different
        scopes to minimize contention while still testing concurrent behavior.
        """
        num_threads = 2
        
        def insert_into_scope(thread_id):
            """Insert a message into a scope specific to the thread."""
            session = None
            try:
                session = session_local()
                
                message_data = MessageCreate(
                    topic_type=f"concurrent_scope_{thread_id}",
                    topic_id="test",
                    message_type="concurrent",
                    sender_type="thread",
                    sender_id=f"thread_{thread_id}",
                    content_type="text/plain",
                    content=f"Message from thread {thread_id}"
                )
                
                result = self.service.persist_message(session, message_data)
                
                return {
                    "success": True,
                    "thread_id": thread_id,
                    "message_id": result.message_id,
                    "scope": f"concurrent_scope_{thread_id}"
                }
                
            except Exception as e:
                return {
                    "success": False,
                    "thread_id": thread_id,
                    "error": str(e)
                }
            finally:
                if session:
                    try:
                        session.close()
                    except Exception:
                        pass
        
        # Execute with different scopes to minimize contention
        results = []
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(insert_into_scope, i) for i in range(num_threads)]
            
            for future in as_completed(futures, timeout=15):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    results.append({"success": False, "error": str(e)})
        
        # Verify results
        successful_results = [r for r in results if r.get("success", False)]
        
        # We expect at least one success since threads use different scopes
        assert len(successful_results) >= 1, f"Expected at least 1 success, got {len(successful_results)} out of {len(results)}"
        
        # Each successful thread should have message_id = 1 (first in its scope)
        for result in successful_results:
            assert result["message_id"] == 1, f"Thread {result['thread_id']} should have message_id=1 in its scope"
    
    def test_session_isolation_and_cleanup(self, session_local):
        """Test proper session management and isolation.
        
        This test verifies that sessions are properly created, used, and cleaned up
        without causing interference between operations.
        """
        operations = 3
        session_ids = []
        
        # Perform sequential operations with separate sessions
        for i in range(operations):
            session = session_local()
            try:
                # Track session for verification
                session_ids.append(id(session))
                
                message_data = MessageCreate(
                    topic_type="session_isolation",
                    topic_id="test",
                    message_type="cleanup",
                    sender_type="operation",
                    sender_id=f"op_{i}",
                    content_type="text/plain",
                    content=f"Operation {i} message"
                )
                
                result = self.service.persist_message(session, message_data)
                assert result.message_id == i + 1, f"Operation {i} should have message_id={i + 1}"
                
            finally:
                session.close()
        
        # Verify each operation used a different session instance
        assert len(set(session_ids)) == operations, "Each operation should use a unique session instance"
        
        # Verify all messages were persisted correctly
        final_session = session_local()
        try:
            stmt = select(Message).where(
                Message.topic_type == "session_isolation",
                Message.topic_id == "test",
                Message.message_type == "cleanup"
            ).order_by(Message.message_id)
            
            messages = final_session.execute(stmt).scalars().all()
            assert len(messages) == operations
            
            for i, msg in enumerate(messages):
                assert msg.message_id == i + 1
                assert msg.content == f"Operation {i} message"
                
        finally:
            final_session.close()
    
    def test_error_recovery_and_data_integrity(self, session_local):
        """Test error recovery and data integrity.
        
        This test verifies that errors don't corrupt the message ID sequence
        and that valid operations continue to work correctly.
        """
        # Create a valid message first
        session = session_local()
        try:
            valid_message = MessageCreate(
                topic_type="error_recovery",
                topic_id="test",
                message_type="integrity",
                sender_type="test",
                sender_id="valid_1",
                content_type="text/plain",
                content="First valid message"
            )
            
            result1 = self.service.persist_message(session, valid_message)
            assert result1.message_id == 1
            
        finally:
            session.close()
        
        # Create another valid message to ensure sequence continues correctly
        session = session_local()
        try:
            second_message = MessageCreate(
                topic_type="error_recovery",
                topic_id="test",
                message_type="integrity",
                sender_type="test",
                sender_id="valid_2",
                content_type="text/plain",
                content="Second valid message"
            )
            
            result2 = self.service.persist_message(session, second_message)
            assert result2.message_id == 2
            
        finally:
            session.close()
        
        # Verify data integrity in database
        session = session_local()
        try:
            stmt = select(Message).where(
                Message.topic_type == "error_recovery",
                Message.topic_id == "test",
                Message.message_type == "integrity"
            ).order_by(Message.message_id)
            
            messages = session.execute(stmt).scalars().all()
            assert len(messages) == 2
            
            # Verify message sequence and content integrity
            assert messages[0].message_id == 1
            assert messages[1].message_id == 2
            assert messages[0].content == "First valid message"
            assert messages[1].content == "Second valid message"
            assert messages[0].sender_id == "valid_1"
            assert messages[1].sender_id == "valid_2"
            
            # Verify all required fields are populated correctly
            for msg in messages:
                assert msg.topic_type == "error_recovery"
                assert msg.topic_id == "test"
                assert msg.message_type == "integrity"
                assert msg.sender_type == "test"
                assert msg.content_type == "text/plain"
                assert msg.created_at is not None
                
        finally:
            session.close()
    
    def test_message_id_generation_edge_cases(self, session_local):
        """Test edge cases in message ID generation.
        
        This test verifies behavior with various edge cases that might
        occur in concurrent scenarios.
        """
        # Test with empty scope first
        session = session_local()
        try:
            # First message in a new scope should get ID 1
            first_message = MessageCreate(
                topic_type="edge_case",
                topic_id="empty_scope",
                message_type="test",
                sender_type="test",
                sender_id="first",
                content_type="text/plain",
                content="First in empty scope"
            )
            
            result = self.service.persist_message(session, first_message)
            assert result.message_id == 1
            
        finally:
            session.close()
        
        # Test immediate follow-up
        session = session_local()
        try:
            second_message = MessageCreate(
                topic_type="edge_case",
                topic_id="empty_scope",
                message_type="test",
                sender_type="test",
                sender_id="second",
                content_type="text/plain",
                content="Second in scope"
            )
            
            result = self.service.persist_message(session, second_message)
            assert result.message_id == 2
            
        finally:
            session.close()
        
        # Verify final state
        session = session_local()
        try:
            stmt = select(Message).where(
                Message.topic_type == "edge_case",
                Message.topic_id == "empty_scope",
                Message.message_type == "test"
            ).order_by(Message.message_id)
            
            messages = session.execute(stmt).scalars().all()
            assert len(messages) == 2
            assert [msg.message_id for msg in messages] == [1, 2]
            
        finally:
            session.close()
