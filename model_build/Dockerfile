FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY training_data_generation.py ml_model.py ./
ENV PYTHONUNBUFFERED=1
CMD ["python", "ml_model.py"]
