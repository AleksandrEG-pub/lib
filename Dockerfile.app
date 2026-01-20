FROM spark-image
USER root
WORKDIR /app
RUN mkdir src
COPY pyproject.toml README.md ./

RUN apt update && apt install -y python3.10-venv
RUN python3 -m venv /opt/venv \
    && /opt/venv/bin/pip install --upgrade pip \
    && /opt/venv/bin/pip install debugpy \
    && /opt/venv/bin/pip install .
ENV PATH="/opt/venv/bin:$PATH"

RUN pip install debugpy \
&& pip install .

COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

COPY src/ ./src/
RUN /opt/venv/bin/pip install .
COPY env/ ./env/

ENTRYPOINT ["./entrypoint.sh"]
