from http.server import BaseHTTPRequestHandler, HTTPServer
import logging
from airflow_pipeline import delivery_service


class PipelineHandler(BaseHTTPRequestHandler):

    def do_POST(self):
        if self.path == "/upload":
            delivery_service.upload_from_s3_to_postgres()
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")
        else:
            self.send_error(404)

    def do_GET(self):
        if self.path == "/validate-last-upload":
            self.send_response(200)
            self.end_headers()
            last_upload_valid: bool = delivery_service.check_validity_of_file_upload()
            result = b"true" if last_upload_valid else b"false"
            self.wfile.write(result)
        elif self.path == "/test":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"test")
        elif self.path == "/test2":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"test2")
        else:
            self.send_error(404)


class PipelineServer():
    def start_server(self):
        self.server = HTTPServer(("0.0.0.0", 8080), PipelineHandler)
        logging.info("Server running on http://localhost:8080")
        self.server.serve_forever()


server = PipelineServer()
