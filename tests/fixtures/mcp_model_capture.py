"""Local Responses endpoint: captures the tool catalog; never contacts a model."""
import http.server
import json
import pathlib
import sys

class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get('Content-Length', 0)))
        target = pathlib.Path(sys.argv[1])
        pending = target.with_suffix('.tmp')
        pending.write_bytes(body)
        pending.replace(target)
        payload = json.dumps({'error': {'message': 'Tool catalog captured; no model called', 'type': 'invalid_request_error'}}).encode()
        self.send_response(400)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
print(server.server_port, flush=True)
server.serve_forever()
