"""Local OAuth + Streamable HTTP MCP fixture. Binds loopback only."""
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlencode, urlparse

class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def log_message(self, *_):
        pass
    def reply(self, code, value=None, headers=None):
        data = json.dumps(value).encode() if value is not None else b""
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(data)
    @property
    def base(self):
        return f"http://127.0.0.1:{self.server.server_port}"
    def do_GET(self):
        path = urlparse(self.path)
        if path.path.startswith("/.well-known/oauth-protected-resource"):
            self.reply(200, {"resource": self.base + "/mcp", "authorization_servers": [self.base], "scopes_supported": ["tools"]})
        elif path.path.startswith("/.well-known/oauth-authorization-server"):
            self.reply(200, {"issuer": self.base, "authorization_endpoint": self.base + "/authorize", "token_endpoint": self.base + "/token", "registration_endpoint": self.base + "/register", "response_types_supported": ["code"], "grant_types_supported": ["authorization_code", "refresh_token"], "code_challenge_methods_supported": ["S256"], "token_endpoint_auth_methods_supported": ["none"]})
        elif path.path == "/authorize":
            params = parse_qs(path.query)
            self.reply(302, headers={"Location": params["redirect_uri"][0] + "?" + urlencode({"state": params["state"][0], "code": "fixture-code"})})
        else:
            self.reply(405)
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        if self.path == "/register":
            params = json.loads(body)
            self.reply(201, {"client_id": "fixture-client", "redirect_uris": params["redirect_uris"], "token_endpoint_auth_method": "none"})
            return
        if self.path == "/token":
            self.reply(200, {"access_token": "fixture-access-token", "token_type": "Bearer", "expires_in": 3600, "scope": "tools"})
            return
        if self.path != "/mcp":
            self.reply(404)
            return
        if self.headers.get("Authorization") != "Bearer fixture-access-token":
            self.reply(401, {"error": "unauthorized"}, {"WWW-Authenticate": f'Bearer resource_metadata="{self.base}/.well-known/oauth-protected-resource"'})
            return
        message = json.loads(body)
        if "id" not in message:
            self.reply(202)
            return
        method = message.get("method")
        if method == "initialize":
            result = {"protocolVersion": message["params"]["protocolVersion"], "capabilities": {"tools": {}}, "serverInfo": {"name": "oauth-fixture", "version": "1.0"}}
        elif method == "tools/list":
            result = {"tools": [{"name": "authorized_echo", "description": "Authenticated fixture tool", "inputSchema": {"type": "object"}}]}
        elif method == "tools/call":
            result = {"content": [{"type": "text", "text": "Authorized MCP response"}]}
        else:
            result = {}
        self.reply(200, {"jsonrpc": "2.0", "id": message["id"], "result": result})

server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
print(server.server_port, flush=True)
server.serve_forever()
