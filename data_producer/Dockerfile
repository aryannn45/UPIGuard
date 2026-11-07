FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY normal_transaction_producer.py hacker_transaction_producer.py ./

ARG PRODUCER_SCRIPT=normal_transaction_producer.py
ENV PRODUCER_SCRIPT=${PRODUCER_SCRIPT}
ENV PYTHONUNBUFFERED=1
CMD ["sh", "-c", "exec python -u \"$PRODUCER_SCRIPT\""]
