"""Hermetic stdio MCP fixture; no network, model calls, or external side effects."""
import base64
import json
import sys
import os
import time

waiting = {}

def send(value):
    print(json.dumps(dict(jsonrpc="2.0", **value)), flush=True)

for line in sys.stdin:
    message = json.loads(line)
    method = message.get("method")
    request_id = message.get("id")
    params = message.get("params", {})
    if method is None and request_id == "form-request":
        original = waiting.pop("form-request")
        send({"id": original, "result": {"content": [{"type": "text", "text": json.dumps(message.get("result"))}], "isError": False}})
        continue
    if request_id is None:
        continue
    if method == "initialize":
        time.sleep(float(os.environ.get("OMNI_MCP_FIXTURE_DELAY", "0")))
        result = {"protocolVersion": params.get("protocolVersion", "2025-11-25"), "capabilities": {"tools": {}, "resources": {}}, "serverInfo": {"name": "omni-mcp-test", "version": "1.0"}}
    elif method == "tools/list":
        result = {"tools": [{"name": name, "description": name, "inputSchema": {"type": "object", "properties": {"text": {"type": "string"}}}} for name in ["echo", "form", "image", "fail"]]}
    elif method == "tools/call":
        name = params["name"]
        if name == "form":
            waiting["form-request"] = request_id
            send({"id": "form-request", "method": "elicitation/create", "params": {"mode": "form", "message": "Enter a count", "requestedSchema": {"type": "object", "properties": {"count": {"type": "integer", "minimum": 1}}, "required": ["count"]}}})
            continue
        elif name == "image":
            result = {"content": [{"type": "image", "mimeType": "image/png", "data": "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jZ1kAAAAASUVORK5CYII="}], "isError": False}
        elif name == "fail":
            result = {"content": [{"type": "text", "text": "Fixture tool error"}], "isError": True}
        else:
            text = params.get("arguments", {}).get("text", "hello")
            result = {"content": [{"type": "text", "text": text}], "structuredContent": {"echo": text}, "isError": False}
    elif method == "resources/list":
        result = {"resources": [{"uri": "fixture://notes", "name": "notes", "mimeType": "text/plain"}]}
    elif method == "resources/templates/list":
        result = {"resourceTemplates": [{"uriTemplate": "fixture://notes/{name}", "name": "note"}]}
    elif method == "resources/read":
        result = {"contents": [{"uri": params["uri"], "mimeType": "text/plain", "text": "Fixture resource"}]}
    elif method == "ping":
        result = {}
    else:
        send({"id": request_id, "error": {"code": -32601, "message": "Unsupported fixture method"}})
        continue
    send({"id": request_id, "result": result})
