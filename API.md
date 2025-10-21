# Messaging Service WebSocket API

This document describes the WebSocket API exposed by the cnts_messaging_svc for real-time messaging delivery.

## Overview

The messaging service exposes a WebSocket endpoint to allow clients to subscribe to topics and receive real-time messages. Message validation and serialization are done using Pydantic v2 schemas defined in src/cnts_messaging_svc/schemas/websocket.py and src/cnts_messaging_svc/schemas/message.py.

## WebSocket Endpoint

- URL (ws): ws://{HOST}:{PORT}/api/v1/ws/{client_id}
- URL (wss): wss://{HOST}:{PORT}/api/v1/ws/{client_id}

Path parameter:
- client_id (string): Unique identifier for the connecting client. The server uses client_id to track the connection and subscriptions. No special character constraints are enforced by the API.

All routes are versioned under /api/v1.

## Connection Establishment

Clients establish a standard WebSocket handshake to the endpoint above. The server accepts the connection and registers it with an internal connection manager (WebSocketConnectionManager).

Example JavaScript client:

```javascript
const ws = new WebSocket("ws://localhost:8000/api/v1/ws/my_client_id");
ws.onopen = () => { console.log('connected'); };
```

Example Python (websockets/httpx-like usage):

```python
import websockets, asyncio
async def run():
    async with websockets.connect('ws://localhost:8000/api/v1/ws/my_client_id') as ws:
        await ws.send('...')
```

## Message Envelope

All messages sent between client and server are JSON objects. Each message MUST include a top-level field "type" indicating the message kind. Pydantic schemas for validation are located in:

- src/cnts_messaging_svc/schemas/websocket.py (SubscribeRequest, UnsubscribeRequest, WebSocketMessage, Acknowledgement, ErrorMessage, MessageDelivery)
- src/cnts_messaging_svc/schemas/message.py (MessageResponse)

## Incoming Messages (client -> server)

Supported incoming message types:

1) subscribe
- Schema: SubscribeRequest
- Fields: type="subscribe", topic_type (string), topic_id (string)
- Purpose: Register client as a subscriber for the given topic.

Example:

{
  "type": "subscribe",
  "topic_type": "chat",
  "topic_id": "room1"
}

2) unsubscribe
- Schema: UnsubscribeRequest
- Fields: type="unsubscribe", topic_type (string), topic_id (string)
- Purpose: Remove client's subscription for the given topic.

Example:

{
  "type": "unsubscribe",
  "topic_type": "chat",
  "topic_id": "room1"
}

Validation and errors:
- Missing required fields or invalid values will cause the server to respond with an error message (see outgoing messages). Invalid JSON will also be reported as an error.

## Outgoing Messages (server -> client)

Supported outgoing message types:

1) ack
- Schema: Acknowledgement
- Fields: type="ack", request_id (string), status ("success"|"error")
- Purpose: Acknowledge processing of a client request (e.g., subscribe/unsubscribe).

Example:

{
  "type": "ack",
  "request_id": "subscribe",
  "status": "success"
}

2) error
- Schema: ErrorMessage
- Fields: type="error", error (string)
- Purpose: Notify the client of validation errors, unknown types, or internal processing errors.

Examples:

{
  "type": "error",
  "error": "Unknown message type: unknown_action"
}

{
  "type": "error",
  "error": "Failed to process message: <details>"
}

3) message
- Schema: MessageDelivery
- Fields: type="message", message: MessageResponse
- Purpose: Deliver a published message to subscribers. MessageResponse fields (refer to src/cnts_messaging_svc/schemas/message.py) include: topic_type, topic_id, message_type, message_id, sender_type, sender_id, content_type, content, created_at.

Example:

{
  "type": "message",
  "message": {
    "topic_type": "chat",
    "topic_id": "room1",
    "message_type": "text",
    "message_id": 123,
    "sender_type": "user",
    "sender_id": "user456",
    "content_type": "text/plain",
    "content": "Hello, world!",
    "created_at": "2025-01-01T12:00:00Z"
  }
}

Notes on serialization:
- MessageDelivery and MessageResponse use Pydantic models. Datetime fields are serialized to ISO 8601 strings.

## Example Workflow

1) Client connects to ws://{HOST}:{PORT}/api/v1/ws/{client_id} and server accepts the connection.
2) Client sends a subscribe request for a topic:
   { "type": "subscribe", "topic_type": "chat", "topic_id": "room1" }
3) Server validates and registers subscription, then sends ack:
   { "type": "ack", "request_id": "subscribe", "status": "success" }
4) When a message is published (for example via REST /api/v1/messages), the server forwards a MessageDelivery to all subscribers of chat:room1.
5) Client receives the message payload (type="message") and processes it.
6) Client may unsubscribe or close the connection when done.

## Connection Lifecycle and Inactivity

The application runs a background cleanup task that monitors last activity per client. Configuration values are in src/cnts_messaging_svc/config.py:

- WEBSOCKET_INACTIVITY_TIMEOUT_SECONDS: inactivity timeout in seconds
- WEBSOCKET_INACTIVITY_CHECK_INTERVAL_SECONDS: how often cleanup runs

If a client is inactive longer than the configured timeout, the server will close the connection and clean up subscriptions. Clients can keep the connection alive by sending valid messages periodically (for example, re-subscribing as a keepalive or other benign payloads).

## Error Handling

- Invalid JSON or schema validation failures -> send ErrorMessage with details.
- Unknown type values -> send ErrorMessage: "Unknown message type: <type>".
- Internal processing errors -> send ErrorMessage: "Failed to process message: <details>".

## Versioning

All WebSocket endpoints are versioned under /api/v1. Future changes to the protocol should increment the API version or introduce new endpoints.

## Schema References

Pydantic schemas used by the WebSocket layer:

- src/cnts_messaging_svc/schemas/websocket.py:
  - SubscribeRequest, UnsubscribeRequest, WebSocketMessage, Acknowledgement, ErrorMessage, MessageDelivery

- src/cnts_messaging_svc/schemas/message.py:
  - MessageResponse

## Appendix: Quick JSON Examples

Subscribe request:

{
  "type": "subscribe",
  "topic_type": "chat",
  "topic_id": "room1"
}

Unsubscribe request:

{
  "type": "unsubscribe",
  "topic_type": "chat",
  "topic_id": "room1"
}

Acknowledgement (ack):

{
  "type": "ack",
  "request_id": "subscribe",
  "status": "success"
}

Error message:

{
  "type": "error",
  "error": "Failed to process message: <details>"
}

Message delivery (message):

{
  "type": "message",
  "message": {
    "topic_type": "chat",
    "topic_id": "room1",
    "message_type": "text",
    "message_id": 123,
    "sender_type": "user",
    "sender_id": "user456",
    "content_type": "text/plain",
    "content": "Hello, world!",
    "created_at": "2025-01-01T12:00:00Z"
  }
}

---

For implementation details and tests refer to the code under src/cnts_messaging_svc (routers, services, schemas, connection_manager) and tests/ (integration and unit tests).
