"""Deterministic, loopback-only model API fixture; Python standard library only.

POST /__mock/plan {"key":"test-key", "steps":[...]}; GET /__mock/requests.
Plans are isolated by API key and consumed atomically. Never use real credentials.
"""
import argparse
import copy
import hashlib
import http.server
import json
import socket
import ssl
import threading
import time


def fingerprint(key):
    return hashlib.sha256(key.encode()).hexdigest()


class MockServer(http.server.ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address=("127.0.0.1", 0)):
        super().__init__(address, Handler)
        self.lock = threading.Lock()
        self.plans = {}
        self.requests = []
        self.defaults = {}


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def send_json(self, status, value, headers=None):
        payload = json.dumps(value, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        for name, value in (headers or {}).items():
            self.send_header(name, str(value))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        if self.path == "/__mock/requests":
            with self.server.lock:
                records = copy.deepcopy(self.server.requests)
            self.send_json(200, records)
        elif self.path == "/health":
            self.send_json(200, {"ok": True})
        else:
            self.send_json(404, {"error": {"message": "Unknown path"}})

    def do_POST(self):
        try:
            self.handle_post()
        except (BrokenPipeError, ConnectionResetError, TimeoutError):
            pass  # Expected when testing cancellation and timeouts.

    def handle_post(self):
        try:
            size = int(self.headers.get("Content-Length", "0"))
            if not 0 <= size <= 2 * 1024 * 1024:
                self.close_connection = True
                return self.send_json(413, {"error": {"message": "Body too large"}})
            body = json.loads(self.rfile.read(size))
            if not isinstance(body, dict):
                raise ValueError("Expected an object")
        except (ValueError, UnicodeError):
            return self.send_json(400, {"error": {"message": "Invalid JSON object"}})
        if self.path == "/__mock/plan":
            steps = body.get("steps")
            key = body.get("key")
            if not isinstance(key, str) or not isinstance(steps, list) or not all(isinstance(s, dict) for s in steps):
                return self.send_json(400, {"error": {"message": "Expected key and steps"}})
            with self.server.lock:
                self.server.plans[fingerprint(key)] = copy.deepcopy(steps)
                self.server.defaults[fingerprint(key)] = copy.deepcopy(body.get("exhausted", {}))
            return self.send_json(200, {"ok": True})
        protocols = {"/v1/chat/completions": "chat", "/v1/responses": "responses", "/v1/messages": "anthropic"}
        protocol = protocols.get(self.path)
        if protocol is None:
            return self.send_json(404, {"error": {"message": "Unknown model API path"}})
        key = (self.headers.get("x-api-key", "") if protocol == "anthropic"
               else self.headers.get("Authorization", "").removeprefix("Bearer "))
        digest = fingerprint(key)
        # Secrets never enter request records, stdout, or HTTP diagnostics.
        headers = {k.lower(): v for k, v in self.headers.items()
                   if k.lower() not in ("authorization", "x-api-key", "cookie", "proxy-authorization")}
        with self.server.lock:
            self.server.requests.append({"path": self.path, "key_sha256": digest,
                                         "headers": headers, "body": body, "at": time.monotonic()})
            plan = self.server.plans.get(digest)
            step = plan.pop(0) if plan else copy.deepcopy(self.server.defaults.get(digest, {}))
            known = digest in self.server.plans
        if not known:
            return self.send_json(401, {"error": {"message": "Unregistered mock key", "type": "authentication_error"}})
        if not isinstance(body.get("model"), str) or not body["model"]:
            return self.send_json(400, {"error": {"message": "model is required"}})
        for field, expected in step.get("expect", {}).items():
            if body.get(field) != expected:
                return self.send_json(422, {"error": {"message": "Request expectation failed", "field": field}})
        time.sleep(step.get("delay", 0))
        status = step.get("status", 200)
        if status != 200:
            return self.send_json(status, {"error": {"message": step.get("error", "Injected failure"),
                                                     "type": "mock_error"}}, step.get("headers"))
        if step.get("disconnect"):
            self.close_connection = True
            self.connection.shutdown(socket.SHUT_RDWR)
            return
        text = step.get("text", "mock-ok")
        # Resolve a shell tool from the real Codex tool catalog. Versions use
        # either exec_command or shell_command, with different argument names.
        def shell_tool(value):
            value = copy.deepcopy(value)
            if value.get("name") == "__mock_shell__":
                catalog = json.dumps(body.get("tools", []))
                command = value.pop("command", "cat hello.txt")
                if '"exec_command"' in catalog:
                    value.update(name="exec_command", arguments={"cmd": command, "yield_time_ms": 1000})
                else:
                    value.update(name="shell_command", arguments={"command": command})
            return value
        if "tool" in step and step["tool"]:
            step["tool"] = shell_tool(step["tool"])
        if "tools" in step:
            step["tools"] = [shell_tool(t) for t in step["tools"]]
        tool = step.get("tool")
        model = body["model"]
        if protocol == "chat":
            message = {"role": "assistant", "content": text}
            if tool:
                message = {"role": "assistant", "content": None, "tool_calls": [
                    {"id": "call_mock", "type": "function", "function": {
                        "name": tool["name"], "arguments": tool.get("arguments_json", json.dumps(tool.get("arguments", {})))}}]}
            result = {"id": "chatcmpl-mock", "object": "chat.completion", "created": 1, "model": model,
                      "choices": [{"index": 0, "message": message, "finish_reason": "tool_calls" if tool else "stop"}],
                      "usage": {"prompt_tokens": 3, "completion_tokens": 2, "total_tokens": 5}}
            chunks = []
            if tool:
                delta = copy.deepcopy(message)
                delta["tool_calls"][0]["index"] = 0
                chunks.append({"index": 0, "delta": delta, "finish_reason": None})
            else:
                for part in (text[:len(text)//2], text[len(text)//2:]):
                    chunks.append({"index": 0, "delta": {"role": "assistant", "content": part}, "finish_reason": None})
            chunks.append({"index": 0, "delta": {}, "finish_reason": "tool_calls" if tool else "stop"})
            events = [(None, {"id": "chatcmpl-mock", "object": "chat.completion.chunk", "created": 1,
                              "model": model, "choices": [c]}) for c in chunks]
            events.append((None, "[DONE]"))
        elif protocol == "anthropic":
            content = ([{"type": "tool_use", "id": "call_mock", "name": tool["name"], "input": tool.get("arguments", {})}]
                       if tool else [{"type": "text", "text": text}])
            result = {"id": "msg_mock", "type": "message", "role": "assistant", "model": model,
                      "content": content, "stop_reason": "tool_use" if tool else "end_turn", "stop_sequence": None,
                      "usage": {"input_tokens": 3, "output_tokens": 2}}
            start = dict(result, content=[], stop_reason=None)
            block = dict(content[0], **({"input": {}} if tool else {"text": ""}))
            delta = ({"type": "input_json_delta", "partial_json": tool.get("arguments_json", json.dumps(tool.get("arguments", {})))}
                     if tool else {"type": "text_delta", "text": text})
            events = [("message_start", {"type": "message_start", "message": start}),
                      ("content_block_start", {"type": "content_block_start", "index": 0, "content_block": block}),
                      ("content_block_delta", {"type": "content_block_delta", "index": 0, "delta": delta}),
                      ("content_block_stop", {"type": "content_block_stop", "index": 0}),
                      ("message_delta", {"type": "message_delta", "delta": {"stop_reason": result["stop_reason"], "stop_sequence": None}, "usage": {"output_tokens": 2}}),
                      ("message_stop", {"type": "message_stop"})]
        else:
            item = ({"id": "fc_mock", "type": "function_call", "call_id": "call_mock", "name": tool["name"],
                     "arguments": tool.get("arguments_json", json.dumps(tool.get("arguments", {}))), "status": "completed"} if tool else
                    {"id": "msg_mock", "type": "message", "role": "assistant", "status": "completed",
                     "content": [{"type": "output_text", "text": text, "annotations": []}]})
            result = {"id": "resp_mock", "object": "response", "created_at": 1, "status": "completed", "model": model,
                      "output": [item], "usage": {"input_tokens": 3, "output_tokens": 2, "total_tokens": 5}}
            empty = dict(item, **({"arguments": "", "status": "in_progress"} if tool else {"content": [], "status": "in_progress"}))
            events = [("response.created", {"response": dict(result, status="in_progress", output=[])}),
                      ("response.output_item.added", {"output_index": 0, "item": empty})]
            if tool:
                events += [("response.function_call_arguments.delta", {"item_id": item["id"], "output_index": 0, "delta": item["arguments"]}),
                           ("response.function_call_arguments.done", {"item_id": item["id"], "output_index": 0, "arguments": item["arguments"]})]
            else:
                events += [("response.content_part.added", {"item_id": item["id"], "output_index": 0, "content_index": 0, "part": {"type": "output_text", "text": "", "annotations": []}}),
                           ("response.output_text.delta", {"item_id": item["id"], "output_index": 0, "content_index": 0, "delta": text}),
                           ("response.output_text.done", {"item_id": item["id"], "output_index": 0, "content_index": 0, "text": text}),
                           ("response.content_part.done", {"item_id": item["id"], "output_index": 0, "content_index": 0, "part": item["content"][0]})]
            events += [("response.output_item.done", {"output_index": 0, "item": item}),
                       ("response.completed", {"response": result})]
            events = [(name, dict(value, type=name, sequence_number=i)) for i, (name, value) in enumerate(events)]
        if "tools" in step:
            tools = step["tools"]
            if protocol == "chat":
                calls = [{"id": f"call_mock_{i}", "type": "function", "function": {
                    "name": t["name"], "arguments": t.get("arguments_json", json.dumps(t.get("arguments", {})))}}
                         for i, t in enumerate(tools)]
                result["choices"][0].update(message={"role": "assistant", "content": None, "tool_calls": calls}, finish_reason="tool_calls")
                events = [(None, {"id": "chatcmpl-mock", "object": "chat.completion.chunk", "created": 1, "model": model,
                                  "choices": [{"index": 0, "delta": {"role": "assistant", "tool_calls": [dict(call, index=i) for i, call in enumerate(calls)]}, "finish_reason": None}]}),
                          (None, {"id": "chatcmpl-mock", "object": "chat.completion.chunk", "created": 1, "model": model,
                                  "choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls"}]}), (None, "[DONE]")]
            elif protocol == "anthropic":
                blocks = [{"type": "tool_use", "id": f"call_mock_{i}", "name": t["name"], "input": t.get("arguments", {})} for i, t in enumerate(tools)]
                result.update(content=blocks, stop_reason="tool_use")
                events = [("message_start", {"type": "message_start", "message": dict(result, content=[], stop_reason=None)})]
                for i, (t, block) in enumerate(zip(tools, blocks)):
                    events += [("content_block_start", {"type": "content_block_start", "index": i, "content_block": dict(block, input={})}),
                               ("content_block_delta", {"type": "content_block_delta", "index": i, "delta": {"type": "input_json_delta", "partial_json": t.get("arguments_json", json.dumps(t.get("arguments", {})))}}),
                               ("content_block_stop", {"type": "content_block_stop", "index": i})]
                events += [("message_delta", {"type": "message_delta", "delta": {"stop_reason": "tool_use", "stop_sequence": None}, "usage": {"output_tokens": 2}}),
                           ("message_stop", {"type": "message_stop"})]
            else:
                items = [{"id": f"fc_mock_{i}", "type": "function_call", "call_id": f"call_mock_{i}", "name": t["name"],
                          "arguments": t.get("arguments_json", json.dumps(t.get("arguments", {}))), "status": "completed"} for i, t in enumerate(tools)]
                result["output"] = items
                events = [("response.created", {"response": dict(result, status="in_progress", output=[])})]
                for i, item in enumerate(items):
                    fields = {"item_id": item["id"], "output_index": i}
                    events += [("response.output_item.added", {"output_index": i, "item": dict(item, arguments="", status="in_progress")}),
                               ("response.function_call_arguments.delta", dict(fields, delta=item["arguments"])),
                               ("response.function_call_arguments.done", dict(fields, arguments=item["arguments"])),
                               ("response.output_item.done", {"output_index": i, "item": item})]
                events.append(("response.completed", {"response": result}))
                events = [(name, dict(value, type=name, sequence_number=i)) for i, (name, value) in enumerate(events)]
        if "events" in step:
            events = [(event.get("event"), event["data"]) for event in step["events"]]
        if not body.get("stream"):
            if step.get("malformed"):
                payload = b'{broken'
                self.send_response(200)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                return self.wfile.write(payload)
            return self.send_json(200, step.get("response", result))
        self.send_response(200)
        self.send_header("Content-Type", step.get("content_type", "text/event-stream"))
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True
        for i, (name, value) in enumerate(events):
            if i == step.get("truncate_after"):
                return
            if i == step.get("stall_at"):
                time.sleep(step.get("stall_seconds", 3))
            if i == 0 and step.get("malformed"):
                self.wfile.write(b"data: {broken\n\n")
                return
            payload = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
            wire = (("event: " + name + "\n") if name else "") + "data: " + payload + "\n\n"
            encoded = wire.encode()
            width = step.get("fragment_bytes", len(encoded))
            for offset in range(0, len(encoded), max(1, width)):
                self.wfile.write(encoded[offset:offset + max(1, width)])
                self.wfile.flush()
            time.sleep(step.get("chunk_delay", 0))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--tls", action="store_true")
    args = parser.parse_args()
    server = MockServer(("127.0.0.1", args.port))
    if args.tls:
        from pathlib import Path
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(Path(__file__).with_name("llm_mock_cert.pem"), Path(__file__).with_name("llm_mock_key.pem"))
        server.socket = context.wrap_socket(server.socket, server_side=True)
    print(json.dumps({"base_url": f"{'https' if args.tls else 'http'}://127.0.0.1:{server.server_port}/v1"}), flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
