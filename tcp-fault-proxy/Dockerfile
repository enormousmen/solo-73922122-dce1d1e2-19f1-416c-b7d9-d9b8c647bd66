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

CMD ["sh", "-c", "if [ \"$SERVICE\" = proxy ]; then uvicorn proxy.app:app --host 0.0.0.0 --port 8080; elif [ \"$SERVICE\" = sender ]; then uvicorn sender.app:app --host 0.0.0.0 --port 8080; else python -m receiver.app; fi"]
