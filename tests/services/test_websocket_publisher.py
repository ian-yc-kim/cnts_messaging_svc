import asyncio
import pytest
from types import SimpleNamespace
from datetime import datetime, timezone
import logging

from fastapi import WebSocketDisconnect

from cnts_messaging_svc.services.websocket_publisher import WebSocketPublisher
from cnts_messaging_svc.schemas.message import MessageResponse


class DummyWebSocket:
    def __init__(self):
        self.sent = []
        self._raise_disconnect = False

    def raise_disconnect_on_send(self):
        self._raise_disconnect = True

    async def send_json(self, data):
        if self._raise_disconnect:
            raise WebSocketDisconnect()
        # emulate async send
        await asyncio.sleep(0)
        self.sent.append(data)


class DummyManager:
    def __init__(self, subscribers):
        self._subscribers = subscribers

    def get_subscribers(self, topic_type, topic_id):
        return self._subscribers

    def get_client_id(self, websocket):
        # Return index as id if known
        for idx, ws in enumerate(self._subscribers):
            if ws is websocket:
                return f"client_{idx}"
        return None

    def disconnect(self, client_id: str):
        # no-op for tests
        return None


def make_message_response():
    return MessageResponse(
        topic_type="project",
        topic_id="123",
        message_type="status_update",
        message_id=1,
        sender_type="user",
        sender_id="user123",
        content_type="text/plain",
        content="Hello world",
        created_at=datetime.now(timezone.utc)
    )


def test_publish_message_sends_to_all_subscribers():
    # Arrange
    ws1 = DummyWebSocket()
    ws2 = DummyWebSocket()
    manager = DummyManager([ws1, ws2])
    publisher = WebSocketPublisher(manager)
    msg = make_message_response()

    # Act
    asyncio.run(publisher.publish_message(msg))

    # Assert
    assert len(ws1.sent) == 1
    assert ws1.sent[0]["type"] == "message"
    assert ws1.sent[0]["message"]["content"] == msg.content

    assert len(ws2.sent) == 1
    assert ws2.sent[0]["type"] == "message"
    assert ws2.sent[0]["message"]["sender_id"] == msg.sender_id


def test_publish_message_handles_websocket_disconnect_gracefully():
    # Arrange
    ws1 = DummyWebSocket()
    ws2 = DummyWebSocket()
    ws1.raise_disconnect_on_send()
    manager = DummyManager([ws1, ws2])
    publisher = WebSocketPublisher(manager)
    msg = make_message_response()

    # Act (should not raise)
    asyncio.run(publisher.publish_message(msg))

    # Assert ws2 still received message despite ws1 disconnect
    assert len(ws2.sent) == 1
    assert ws2.sent[0]["type"] == "message"
