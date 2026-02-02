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
        elif self.path == "/notification":
            logging.info("sending notification to telegram")
        else:
            self.send_error(404)

    def do_GET(self):
        if self.path == "/validate-last-upload":
            self.send_response(200)
            self.end_headers()
            last_upload_valid: bool = delivery_service.check_validity_of_file_upload()
            result = b"true" if last_upload_valid else b"false"
            self.wfile.write(result)
            # После успешного запуска DAG №2 в технической таблицу БД PostgreSQL 
            # сохранены данные DQ-проверки для обработанного файла
        else:
            self.send_error(404)


class PipelineServer():
    def start_server(self):
        self.server = HTTPServer(("0.0.0.0", 8080), PipelineHandler)
        logging.info("Server running on http://localhost:8080")
        self.server.serve_forever()


server = PipelineServer()
