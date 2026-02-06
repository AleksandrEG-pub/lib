from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import logging
from airflow_pipeline import delivery_service
from airflow_pipeline.telegram_sevice import notification 


class PipelineHandler(BaseHTTPRequestHandler):    

    def do_POST(self):
        if self.path == "/notification":
            length = int(self.headers["Content-Length"])
            payload = json.loads(self.rfile.read(length))
            try:
                notification.send(
                    dag_id=payload["dag_id"],
                    dag_execution_time=datetime.fromisoformat(payload["dag_execution_time"]),
                    result=payload["result"],
                    details=payload["details"],
                )
                logging.info("sending notification to telegram")
                self.send_response(200)
                self.end_headers()
            except Exception as e:
                logging.error(f"Send notification failed {e}")
                self.send_response(500)
                self.end_headers()
                self.wfile.write(b"NOTIFICATION_FAILED")
        if self.path == '/upload':
            try:
                uploaded: bool = delivery_service.upload_from_s3_to_postgres()
                if uploaded:
                    self.send_response(201)
                    self.end_headers()
                    self.wfile.write(b"UPLOAD_OK")
                else:
                    self.send_response(409)
                    self.end_headers()
                    self.wfile.write(b"NO_FILES")
            except Exception as e:
                logging.error(f"Upload failed {e}")
                self.send_response(500)
                self.end_headers()
                self.wfile.write(b"UPLOAD_FAILED")

    def do_GET(self):
        if self.path == "/validate-last-upload":
            try:
                is_valid = delivery_service.check_validity_of_file_upload()
        # После успешного запуска DAG №2 в технической таблицу БД PostgreSQL 
        # сохранены данные DQ-проверки для обработанного файла
                if is_valid:
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(b"VALID")
                else:
                    self.send_response(422)  # semantic validation failure
                    self.end_headers()
                    self.wfile.write(b"INVALID")
            except Exception as e:
                logging.error(f"check failed {e}")
                self.send_response(500)
                self.end_headers()
                self.wfile.write(b"VALIDATION_ERROR")

class PipelineServer():
    def start_server(self):
        self.server = HTTPServer(("0.0.0.0", 8080), PipelineHandler)
        logging.info("Server running on http://localhost:8080")
        self.server.serve_forever()


server = PipelineServer()
