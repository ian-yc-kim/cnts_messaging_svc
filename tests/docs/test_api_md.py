import pathlib


def test_api_md_exists_and_contains_sections():
    root = pathlib.Path(__file__).resolve().parents[2]
    api_md = root / 'API.md'
    assert api_md.exists(), f"API.md not found at {api_md}"

    content = api_md.read_text(encoding='utf-8')

    # Key anchors and phrases required by acceptance criteria
    assert '/api/v1/ws/{client_id}' in content or '/api/v1/ws/{client_id}' in content.replace('"', "'"), "Endpoint path missing"
    for keyword in ['subscribe', 'unsubscribe', 'ack', 'error', 'message']:
        assert keyword in content, f"Keyword '{keyword}' not found in API.md"

    # Ensure examples reference the Pydantic schemas location
    assert 'src/cnts_messaging_svc/schemas/websocket.py' in content
    assert 'src/cnts_messaging_svc/schemas/message.py' in content

    # Basic JSON example snippets check
    assert '"type": "subscribe"' in content or "'type': 'subscribe'" in content
    assert '"type": "unsubscribe"' in content or "'type': 'unsubscribe'" in content
    assert '"type": "ack"' in content or "'type': 'ack'" in content
    assert '"type": "error"' in content or "'type': 'error'" in content
    assert '"type": "message"' in content or "'type': 'message'" in content
