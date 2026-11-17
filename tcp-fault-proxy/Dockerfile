FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

# SERVICE (set per compose service) selects which component the container runs.
ENV SERVICE=proxy
EXPOSE 8080 9000 9100 9101

HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=20 \
  CMD python -c "import os,socket; p={'proxy':8080,'sender':8080}.get(os.environ.get('SERVICE','proxy'),9100); socket.create_connection(('127.0.0.1',p),2).close()"

CMD ["sh", "-c", "if [ \"$SERVICE\" = proxy ]; then uvicorn proxy.app:app --host 0.0.0.0 --port 8080; elif [ \"$SERVICE\" = sender ]; then uvicorn sender.app:app --host 0.0.0.0 --port 8080; else python -m receiver.app; fi"]
